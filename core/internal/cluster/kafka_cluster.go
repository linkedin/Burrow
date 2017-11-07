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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"time"
)

type KafkaCluster struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name            string
	myConfiguration *configuration.ClusterConfig
	saramaConfig    *sarama.Config

	offsetTicker   *time.Ticker
	metadataTicker *time.Ticker
	quitChannel    chan struct{}
	running        sync.WaitGroup

	fetchMetadata bool
	topicMap      map[string]int
}

func (module *KafkaCluster) Configure(name string) {
	module.Log.Info("configuring")

	module.name = name
	module.quitChannel = make(chan struct{})
	module.running = sync.WaitGroup{}
	module.myConfiguration = module.App.Configuration.Cluster[name]

	if profile, ok := module.App.Configuration.ClientProfile[module.myConfiguration.ClientProfile]; ok {
		module.saramaConfig = helpers.GetSaramaConfigFromClientProfile(profile)
	} else {
		panic("Cluster '" + name + "' references an unknown client-profile '" + module.myConfiguration.ClientProfile + "'")
	}
	if len(module.myConfiguration.Servers) == 0 {
		panic("No Kafka brokers specified for cluster " + module.name)
	} else if !configuration.ValidateHostList(module.myConfiguration.Servers) {
		panic("Cluster '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed
	if module.App.Configuration.Cluster[module.name].OffsetRefresh == 0 {
		module.App.Configuration.Cluster[module.name].OffsetRefresh = 10
	}
	if module.App.Configuration.Cluster[module.name].TopicRefresh == 0 {
		module.App.Configuration.Cluster[module.name].TopicRefresh = 60
	}
}

func (module *KafkaCluster) Start() error {
	module.Log.Info("starting")

	// Connect Kafka client
	client, err := sarama.NewClient(module.myConfiguration.Servers, module.saramaConfig)
	if err != nil {
		return err
	}

	// Start main loop that has a timer for offset and topic fetches
	module.offsetTicker = time.NewTicker(time.Duration(module.myConfiguration.OffsetRefresh) * time.Second)
	module.metadataTicker = time.NewTicker(time.Duration(module.myConfiguration.TopicRefresh) * time.Second)
	go module.mainLoop(&helpers.BurrowSaramaClient{client})

	return nil
}

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

		// We'll use the partition counts later
		topicMap := make(map[string]int)
		for _, topic := range topicList {
			partitions, err := client.Partitions(topic)
			if err != nil {
				module.Log.Error("failed to fetch partition list", zap.String("sarama_error", err.Error()))
				return
			}
			topicMap[topic] = len(partitions)
		}

		// Check for deleted topics if we have a previous map to check against
		if module.topicMap != nil {
			for topic := range module.topicMap {
				if _, ok := topicMap[topic]; !ok {
					// Topic no longer exists - tell storage to delete it
					module.App.StorageChannel <- &protocol.StorageRequest{
						RequestType: protocol.StorageSetDeleteTopic,
						Cluster:     module.name,
						Topic:       topic,
					}
				}
			}
		}

		// Save the new topicMap for next time
		module.topicMap = topicMap
	}
}

func (module *KafkaCluster) generateOffsetRequests(client helpers.SaramaClient) (map[int32]*sarama.OffsetRequest, map[int32]helpers.SaramaBroker) {
	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]helpers.SaramaBroker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitions := range module.topicMap {
		for i := 0; i < partitions; i++ {
			broker, err := client.Leader(topic, int32(i))
			if err != nil {
				module.Log.Error("failed to fetch leader for partition", zap.String("sarama_error", err.Error()))
				continue
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
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
	var wg sync.WaitGroup

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
					continue
				}
				offset := &protocol.StorageRequest{
					RequestType:         protocol.StorageSetBrokerOffset,
					Cluster:             module.name,
					Topic:               topic,
					Partition:           partition,
					Offset:              offsetResponse.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: int32(module.topicMap[topic]),
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
}
