/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"github.com/linkedin/Burrow/protocol"
	"sync"
	"time"
)

type KafkaClient struct {
	app                *ApplicationContext
	cluster            string
	client             sarama.Client
	masterConsumer     sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	requestChannel     chan *BrokerTopicRequest
	messageChannel     chan *sarama.ConsumerMessage
	errorChannel       chan *sarama.ConsumerError
	wgFanIn            sync.WaitGroup
	wgProcessor        sync.WaitGroup
	topicMap           map[string]int
	topicMapLock       sync.RWMutex
	brokerOffsetTicker *time.Ticker
}

type BrokerTopicRequest struct {
	Result chan int
	Topic  string
}

func NewKafkaClient(app *ApplicationContext, cluster string) (*KafkaClient, error) {
	// Set up sarama config from profile
	clientConfig := sarama.NewConfig()
	profile := app.Config.Clientprofile[app.Config.Kafka[cluster].Clientprofile]
	clientConfig.ClientID = profile.ClientID
	clientConfig.Net.TLS.Enable = profile.TLS
	clientConfig.Net.TLS.Config = &tls.Config{}
	clientConfig.Net.TLS.Config.InsecureSkipVerify = profile.TLSNoVerify

	sclient, err := sarama.NewClient(app.Config.Kafka[cluster].Brokers, clientConfig)
	if err != nil {
		return nil, err
	}

	// Create sarama master consumer
	master, err := sarama.NewConsumerFromClient(sclient)
	if err != nil {
		sclient.Close()
		return nil, err
	}

	client := &KafkaClient{
		app:            app,
		cluster:        cluster,
		client:         sclient,
		masterConsumer: master,
		requestChannel: make(chan *BrokerTopicRequest),
		messageChannel: make(chan *sarama.ConsumerMessage),
		errorChannel:   make(chan *sarama.ConsumerError),
		wgFanIn:        sync.WaitGroup{},
		wgProcessor:    sync.WaitGroup{},
		topicMap:       make(map[string]int),
		topicMapLock:   sync.RWMutex{},
	}

	// Start the main processor goroutines for __consumer_offset messages
	client.wgProcessor.Add(2)
	go func() {
		defer client.wgProcessor.Done()
		for msg := range client.messageChannel {
			go client.processConsumerOffsetsMessage(msg)
		}
	}()
	go func() {
		defer client.wgProcessor.Done()
		for err := range client.errorChannel {
			log.Errorf("Consume error on %s:%v: %v", err.Topic, err.Partition, err.Err)
		}
	}()

	// Start goroutine to handle topic metadata requests. Do this first because the getOffsets call needs this working
	client.RefreshTopicMap()
	go func() {
		for r := range client.requestChannel {
			client.getPartitionCount(r)
		}
	}()

	// Now get the first set of offsets and start a goroutine to continually check them
	client.getOffsets()
	client.brokerOffsetTicker = time.NewTicker(time.Duration(client.app.Config.Tickers.BrokerOffsets) * time.Second)
	go func() {
		for _ = range client.brokerOffsetTicker.C {
			client.getOffsets()
		}
	}()

	// Get a partition count for the consumption topic
	partitions, err := client.client.Partitions(client.app.Config.Kafka[client.cluster].OffsetsTopic)
	if err != nil {
		return nil, err
	}

	// Start consumers for each partition with fan in
	client.partitionConsumers = make([]sarama.PartitionConsumer, len(partitions))
	log.Infof("Starting consumers for %v partitions of %s in cluster %s", len(partitions), client.app.Config.Kafka[client.cluster].OffsetsTopic, client.cluster)
	for i, partition := range partitions {
		pconsumer, err := client.masterConsumer.ConsumePartition(client.app.Config.Kafka[client.cluster].OffsetsTopic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}
		client.partitionConsumers[i] = pconsumer
		client.wgFanIn.Add(2)
		go func() {
			defer client.wgFanIn.Done()
			for msg := range pconsumer.Messages() {
				client.messageChannel <- msg
			}
		}()
		go func() {
			defer client.wgFanIn.Done()
			for err := range pconsumer.Errors() {
				client.errorChannel <- err
			}
		}()
	}

	return client, nil
}

func (client *KafkaClient) Stop() {
	// We don't really need to do a safe stop, because we're not maintaining offsets. But we'll do it anyways
	for _, pconsumer := range client.partitionConsumers {
		pconsumer.AsyncClose()
	}

	// Wait for the Messages and Errors channel to be fully drained.
	client.wgFanIn.Wait()
	close(client.errorChannel)
	close(client.messageChannel)
	client.wgProcessor.Wait()

	// Stop the offset checker and the topic metdata refresh and request channel
	client.brokerOffsetTicker.Stop()
	close(client.requestChannel)
}

// Send the offset on the specified channel, but wait no more than maxTime seconds to do so
func timeoutSendOffset(offsetChannel chan *protocol.PartitionOffset, offset *protocol.PartitionOffset, maxTime int) {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case offsetChannel <- offset:
	case <-timeout:
	}
}

// This function performs massively parallel OffsetRequests, which is better than Sarama's internal implementation,
// which does one at a time. Several orders of magnitude faster.
func (client *KafkaClient) getOffsets() error {
	// Start with refreshing the topic list
	client.RefreshTopicMap()

	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	client.topicMapLock.RLock()

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range client.topicMap {
		for i := 0; i < partitions; i++ {
			broker, err := client.client.Leader(topic, int32(i))
			if err != nil {
				client.topicMapLock.RUnlock()
				log.Errorf("Topic leader error on %s:%v: %v", topic, int32(i), err)
				return err
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	var wg sync.WaitGroup

	getBrokerOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			log.Errorf("Cannot fetch offsets from broker %v: %v", brokerID, err)
			_ = brokers[brokerID].Close()
			return
		}
		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerID, offsetResponse.Err.Error())
					continue
				}
				offset := &protocol.PartitionOffset{
					Cluster:             client.cluster,
					Topic:               topic,
					Partition:           partition,
					Offset:              offsetResponse.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: client.topicMap[topic],
				}
				timeoutSendOffset(client.app.Storage.offsetChannel, offset, 1)
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerID, request)
	}

	wg.Wait()
	client.topicMapLock.RUnlock()
	return nil
}

func (client *KafkaClient) RefreshTopicMap() {
	client.topicMapLock.Lock()
	topics, _ := client.client.Topics()
	for _, topic := range topics {
		partitions, _ := client.client.Partitions(topic)
		client.topicMap[topic] = len(partitions)
	}
	client.topicMapLock.Unlock()
}

func (client *KafkaClient) getPartitionCount(r *BrokerTopicRequest) {
	client.topicMapLock.RLock()
	if partitions, ok := client.topicMap[r.Topic]; ok {
		r.Result <- partitions
	} else {
		r.Result <- -1
	}
	client.topicMapLock.RUnlock()
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

func (client *KafkaClient) processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) {
	var keyver, valver uint16
	var group, topic string
	var partition uint32
	var offset, timestamp uint64

	buf := bytes.NewBuffer(msg.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	switch keyver {
	case 0, 1:
		group, err = readString(buf)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: group", msg.Topic, msg.Partition, msg.Offset)
			return
		}
		topic, err = readString(buf)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: topic", msg.Topic, msg.Partition, msg.Offset)
			return
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			log.Warnf("Failed to decode %s:%v offset %v: partition", msg.Topic, msg.Partition, msg.Offset)
			return
		}
	case 2:
		log.Debugf("Discarding group metadata message with key version 2")
		return
	default:
		log.Warnf("Failed to decode %s:%v offset %v: keyver %v", msg.Topic, msg.Partition, msg.Offset, keyver)
		return
	}

	buf = bytes.NewBuffer(msg.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		log.Warnf("Failed to decode %s:%v offset %v: valver %v", msg.Topic, msg.Partition, msg.Offset, valver)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: offset", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	_, err = readString(buf)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: metadata", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		log.Warnf("Failed to decode %s:%v offset %v: timestamp", msg.Topic, msg.Partition, msg.Offset)
		return
	}

	// fmt.Printf("[%s,%s,%v]::OffsetAndMetadata[%v,%s,%v]\n", group, topic, partition, offset, metadata, timestamp)
	partitionOffset := &protocol.PartitionOffset{
		Cluster:   client.cluster,
		Topic:     topic,
		Partition: int32(partition),
		Group:     group,
		Timestamp: int64(timestamp),
		Offset:    int64(offset),
	}
	timeoutSendOffset(client.app.Storage.offsetChannel, partitionOffset, 1)
	return
}
