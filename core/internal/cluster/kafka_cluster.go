/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

// KafkaCluster is a cluster module which connects to a single Apache Kafka cluster and manages the broker topic and
// partition information. It periodically updates a list of all topics and partitions, and also fetches the broker
// end offset (latest) for each partition. This information is forwarded to the storage module for use in consumer
// evaluations.
type KafkaCluster struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name          string
	saramaConfig  *sarama.Config
	servers       []string
	offsetRefresh int
	topicRefresh  int

	offsetTicker   *time.Ticker
	metadataTicker *time.Ticker
	quitChannel    chan struct{}
	running        sync.WaitGroup

	fetchMetadata   bool
	topicPartitions map[string][]int32
}

// Configure validates the configuration for the cluster. At minimum, there must be a list of servers provided for the
// Kafka cluster, of the form host:port. Default values will be set for the intervals to use for refreshing offsets
// (10 seconds) and topics (60 seconds). A missing, or bad, list of servers will cause this func to panic.
func (module *KafkaCluster) Configure(name string, configRoot string) {
	module.Log.Info("configuring")

	module.name = name
	module.quitChannel = make(chan struct{})
	module.running = sync.WaitGroup{}

	profile := viper.GetString(configRoot + ".client-profile")
	module.saramaConfig = helpers.GetSaramaConfigFromClientProfile(profile)

	module.servers = viper.GetStringSlice(configRoot + ".servers")
	if len(module.servers) == 0 {
		panic("No Kafka brokers specified for cluster " + module.name)
	} else if !helpers.ValidateHostList(module.servers) {
		panic("Cluster '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed
	viper.SetDefault(configRoot+".offset-refresh", 10)
	viper.SetDefault(configRoot+".topic-refresh", 60)
	module.offsetRefresh = viper.GetInt(configRoot + ".offset-refresh")
	module.topicRefresh = viper.GetInt(configRoot + ".topic-refresh")
}

// Start connects to the Kafka cluster using the Shopify/sarama client. Any error connecting to the cluster is returned
// to the caller. Once the client is set up, tickers are started to periodically refresh topics and offsets.
func (module *KafkaCluster) Start() error {
	module.Log.Info("starting")

	// Connect Kafka client
	client, err := sarama.NewClient(module.servers, module.saramaConfig)
	if err != nil {
		module.Log.Error("failed to start client", zap.Error(err))
		return err
	}

	// Fire off the offset requests once, before we start the ticker, to make sure we start with good data for consumers
	helperClient := &helpers.BurrowSaramaClient{
		Client: client,
	}
	module.fetchMetadata = true
	module.getOffsets(helperClient)

	// Start main loop that has a timer for offset and topic fetches
	module.offsetTicker = time.NewTicker(time.Duration(module.offsetRefresh) * time.Second)
	module.metadataTicker = time.NewTicker(time.Duration(module.topicRefresh) * time.Second)
	go module.mainLoop(helperClient)

	return nil
}

// Stop causes both the topic and offset refresh tickers to be stopped, and then it closes the Kafka client.
func (module *KafkaCluster) Stop() error {
	module.Log.Info("stopping")

	module.metadataTicker.Stop()
	module.offsetTicker.Stop()
	close(module.quitChannel)
	module.running.Wait()

	return nil
}

func (module *KafkaCluster) mainLoop(client helpers.SaramaClient) {
	module.running.Add(1)
	defer module.running.Done()

	for {
		select {
		case <-module.offsetTicker.C:
			module.getOffsets(client)
		case <-module.metadataTicker.C:
			// Update metadata on next offset fetch
			module.fetchMetadata = true
		case <-module.quitChannel:
			return
		}
	}
}

func (module *KafkaCluster) maybeUpdateMetadataAndDeleteTopics(client helpers.SaramaClient) {
	if module.fetchMetadata {
		module.fetchMetadata = false
		client.RefreshMetadata()

		// Get the current list of topics and make a map
		topicList, err := client.Topics()
		if err != nil {
			module.Log.Error("failed to fetch topic list", zap.String("sarama_error", err.Error()))
			return
		}

		// We'll use topicPartitions later
		topicPartitions := make(map[string][]int32)
		for _, topic := range topicList {
			partitions, err := client.Partitions(topic)
			if err != nil {
				module.Log.Error("failed to fetch partition list", zap.String("sarama_error", err.Error()))
				return
			}

			topicPartitions[topic] = make([]int32, 0, len(partitions))
			for _, partitionID := range partitions {
				if _, err := client.Leader(topic, partitionID); err != nil {
					module.Log.Warn("failed to fetch leader for partition",
						zap.String("topic", topic),
						zap.Int32("partition", partitionID),
						zap.String("sarama_error", err.Error()))
				} else { // partitionID has a leader
					// NOTE: append only happens here
					// so cap(topicPartitions[topic]) is the partition count
					topicPartitions[topic] = append(topicPartitions[topic], partitionID)
				}
			}
		}

		// Check for deleted topics if we have a previous map to check against
		if module.topicPartitions != nil {
			for topic := range module.topicPartitions {
				if _, ok := topicPartitions[topic]; !ok {
					// Topic no longer exists - tell storage to delete it
					module.App.StorageChannel <- &protocol.StorageRequest{
						RequestType: protocol.StorageSetDeleteTopic,
						Cluster:     module.name,
						Topic:       topic,
					}
				}
			}
		}

		// Save the new topicPartitions for next time
		module.topicPartitions = topicPartitions
	}
}

func (module *KafkaCluster) generateOffsetRequests(client helpers.SaramaClient) (map[int32]*sarama.OffsetRequest, map[int32]helpers.SaramaBroker) {
	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]helpers.SaramaBroker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range module.topicPartitions {
		for _, partitionID := range partitions {
			broker, err := client.Leader(topic, partitionID)
			if err != nil {
				module.Log.Warn("failed to fetch leader for partition",
					zap.String("topic", topic),
					zap.Int32("partition", partitionID),
					zap.String("sarama_error", err.Error()))
				module.fetchMetadata = true
				continue
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, partitionID, sarama.OffsetNewest, 1)
		}
	}

	return requests, brokers
}

// This function performs massively parallel OffsetRequests, which is better than Sarama's internal implementation,
// which does one at a time. Several orders of magnitude faster.
func (module *KafkaCluster) getOffsets(client helpers.SaramaClient) {
	module.maybeUpdateMetadataAndDeleteTopics(client)
	requests, brokers := module.generateOffsetRequests(client)

	// Send out the OffsetRequest to each broker for all the partitions it is leader for
	// The results go to the offset storage module
	var wg = sync.WaitGroup{}
	var errorTopics = sync.Map{}

	getBrokerOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			module.Log.Error("failed to fetch offsets from broker",
				zap.String("sarama_error", err.Error()),
				zap.Int32("broker", brokerID),
			)
			brokers[brokerID].Close()
			return
		}
		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					module.Log.Warn("error in OffsetResponse",
						zap.String("sarama_error", offsetResponse.Err.Error()),
						zap.Int32("broker", brokerID),
						zap.String("topic", topic),
						zap.Int32("partition", partition),
					)

					// Gather a list of topics that had errors
					errorTopics.Store(topic, true)
					continue
				}
				offset := &protocol.StorageRequest{
					RequestType:         protocol.StorageSetBrokerOffset,
					Cluster:             module.name,
					Topic:               topic,
					Partition:           partition,
					Offset:              offsetResponse.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: int32(cap(module.topicPartitions[topic])),
				}
				helpers.TimeoutSendStorageRequest(module.App.StorageChannel, offset, 1)
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerID, request)
	}

	wg.Wait()

	// If there are any topics that had errors, force a metadata refresh on the next run
	errorTopics.Range(func(key, value interface{}) bool {
		module.fetchMetadata = true
		return false
	})
}
