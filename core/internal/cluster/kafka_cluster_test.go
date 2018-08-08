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
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"sync"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureModule() *KafkaCluster {
	module := KafkaCluster{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	viper.Reset()
	viper.Set("client-profile..client-id", "testid")
	viper.Set("cluster.test.class-name", "kafka")
	viper.Set("cluster.test.servers", []string{"broker1.example.com:1234"})

	return &module
}

func TestKafkaCluster_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(KafkaCluster))
}

func TestKafkaCluster_Configure(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")
	assert.NotNil(t, module.saramaConfig, "Expected saramaConfig to be populated")
}

func TestKafkaCluster_Configure_DefaultIntervals(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")

	assert.Equal(t, int(10), module.offsetRefresh, "Default OffsetRefresh value of 10 did not get set")
	assert.Equal(t, int(60), module.topicRefresh, "Default TopicRefresh value of 60 did not get set")
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_NoUpdate(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")
	client := &helpers.MockSaramaClient{}

	module.maybeUpdateMetadataAndDeleteTopics(client)
	client.AssertNotCalled(t, "RefreshMetadata")
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_NoDelete(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")

	// Set up the mock to return a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("RefreshMetadata").Return(nil)
	client.On("Topics").Return([]string{"testtopic"}, nil)
	client.On("Partitions", "testtopic").Return([]int32{0}, nil)
	client.On("Leader", "testtopic", int32(0)).Return(&helpers.MockSaramaBroker{}, nil)

	module.fetchMetadata = true
	module.maybeUpdateMetadataAndDeleteTopics(client)

	client.AssertExpectations(t)
	assert.False(t, module.fetchMetadata, "Expected fetchMetadata to be reset to false")
	assert.Lenf(t, module.topicPartitions, 1, "Expected 1 topic entry, not %v", len(module.topicPartitions))
	topic, ok := module.topicPartitions["testtopic"]
	assert.True(t, ok, "Expected to find testtopic in topicPartitions")
	assert.Equalf(t, 1, len(topic), "Expected testtopic to be recorded with 1 partition, not %v", len(topic))
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_PartialUpdate(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")

	// Set up the mock to return a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("RefreshMetadata").Return(nil)
	client.On("Topics").Return([]string{"testtopic"}, nil)
	client.On("Partitions", "testtopic").Return([]int32{0, 1}, nil)

	var nilBroker *helpers.BurrowSaramaBroker
	client.On("Leader", "testtopic", int32(0)).Return(nilBroker, errors.New("no leader error"))
	client.On("Leader", "testtopic", int32(1)).Return(&helpers.MockSaramaBroker{}, nil)

	module.fetchMetadata = true
	module.maybeUpdateMetadataAndDeleteTopics(client)

	client.AssertExpectations(t)
	assert.False(t, module.fetchMetadata, "Expected fetchMetadata to be reset to false")
	assert.Lenf(t, module.topicPartitions, 1, "Expected 1 topic entry, not %v", len(module.topicPartitions))
	topic, ok := module.topicPartitions["testtopic"]
	assert.True(t, ok, "Expected to find testtopic in topicPartitions")
	assert.Equalf(t, len(topic), 1, "Expected testtopic's length to be 1, not %v", len(topic))
	assert.Equalf(t, cap(topic), 2, "Expected testtopic's capacity to be 2, not %v", cap(topic))
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_Delete(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")

	// Set up the mock to return a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("RefreshMetadata").Return(nil)
	client.On("Topics").Return([]string{"testtopic"}, nil)
	client.On("Partitions", "testtopic").Return([]int32{0}, nil)
	client.On("Leader", "testtopic", int32(0)).Return(&helpers.MockSaramaBroker{}, nil)

	module.fetchMetadata = true
	module.topicPartitions = make(map[string][]int32)
	module.topicPartitions["topictodelete"] = make([]int32, 0, 10)
	for i := 0; i < cap(module.topicPartitions["topictodelete"]); i++ {
		module.topicPartitions["topictodelete"] = append(module.topicPartitions["topictodelete"], int32(i))
	}

	// Need to wait for this request to come in and finish, which happens when we call maybeUpdate...
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		request := <-module.App.StorageChannel
		assert.Equalf(t, protocol.StorageSetDeleteTopic, request.RequestType, "Expected request sent with type StorageSetDeleteTopic, not %v", request.RequestType)
		assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
		assert.Equalf(t, "topictodelete", request.Topic, "Expected request sent with topic topictodelete, not %v", request.Topic)
	}()
	module.maybeUpdateMetadataAndDeleteTopics(client)
	wg.Wait()

	client.AssertExpectations(t)
	assert.False(t, module.fetchMetadata, "Expected fetchMetadata to be reset to false")
	assert.Lenf(t, module.topicPartitions, 1, "Expected 1 topic entry, not %v", len(module.topicPartitions))
	topic, ok := module.topicPartitions["testtopic"]
	assert.True(t, ok, "Expected to find testtopic in topicPartitions")
	assert.Equalf(t, 1, len(topic), "Expected testtopic to be recorded with 1 partition, not %v", len(topic))
}

func TestKafkaCluster_generateOffsetRequests(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")
	module.topicPartitions = make(map[string][]int32)
	module.topicPartitions["testtopic"] = []int32{0}

	// Set up a broker mock
	broker := &helpers.MockSaramaBroker{}
	broker.On("ID").Return(int32(13))

	// Set up the mock to return the leader broker for a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("Leader", "testtopic", int32(0)).Return(broker, nil)

	requests, brokers := module.generateOffsetRequests(client)

	broker.AssertExpectations(t)
	client.AssertExpectations(t)
	assert.Lenf(t, brokers, 1, "Expected 1 broker entry, not %v", len(brokers))
	_, ok := brokers[13]
	assert.True(t, ok, "Expected key for the broker to be its ID")
	assert.Equal(t, broker, brokers[13], "Expected broker returned to be the mock")
	assert.Lenf(t, requests, 1, "Expected 1 request, not %v", len(requests))
}

func TestKafkaCluster_generateOffsetRequests_NoLeader(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")
	module.topicPartitions = make(map[string][]int32)
	module.topicPartitions["testtopic"] = []int32{0, 1}

	// Set up a broker mock
	broker := &helpers.MockSaramaBroker{}
	broker.On("ID").Return(int32(13))

	// Set up the mock to return the leader broker for a test topic and partition
	client := &helpers.MockSaramaClient{}
	var nilBroker *helpers.BurrowSaramaBroker
	client.On("Leader", "testtopic", int32(0)).Return(nilBroker, errors.New("no leader error"))
	client.On("Leader", "testtopic", int32(1)).Return(broker, nil)

	requests, brokers := module.generateOffsetRequests(client)

	broker.AssertExpectations(t)
	client.AssertExpectations(t)
	assert.Lenf(t, brokers, 1, "Expected 1 broker entry, not %v", len(brokers))
	_, ok := brokers[13]
	assert.True(t, ok, "Expected key for the broker to be its ID")
	assert.Equal(t, broker, brokers[13], "Expected broker returned to be the mock")
	assert.Lenf(t, requests, 1, "Expected 1 request, not %v", len(requests))
	assert.True(t, module.fetchMetadata, "Expected fetchMetadata to be true")
}

func TestKafkaCluster_getOffsets(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")
	module.topicPartitions = make(map[string][]int32)
	module.topicPartitions["testtopic"] = []int32{0, 1}
	module.fetchMetadata = false

	// Set up an OffsetResponse
	offsetResponse := &sarama.OffsetResponse{Version: 1}
	offsetResponse.AddTopicPartition("testtopic", 0, 8374)

	// Set up a broker mock
	broker := &helpers.MockSaramaBroker{}
	broker.On("ID").Return(int32(13))
	broker.On("GetAvailableOffsets", mock.MatchedBy(func(request *sarama.OffsetRequest) bool { return request != nil })).Return(offsetResponse, nil)

	// Set up the mock to return the leader broker for a test topic and partition
	client := &helpers.MockSaramaClient{}
	var nilBroker *helpers.BurrowSaramaBroker
	client.On("Leader", "testtopic", int32(0)).Return(broker, nil)
	client.On("Leader", "testtopic", int32(1)).Return(nilBroker, errors.New("no leader error"))

	go module.getOffsets(client)
	request := <-module.App.StorageChannel

	broker.AssertExpectations(t)
	client.AssertExpectations(t)
	assert.Equalf(t, protocol.StorageSetBrokerOffset, request.RequestType, "Expected request sent with type StorageSetBrokerOffset, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "testtopic", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(0), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, int32(2), request.TopicPartitionCount, "Expected request sent with TopicPartitionCount 2, not %v", request.TopicPartitionCount)
	assert.Equalf(t, int64(8374), request.Offset, "Expected request sent with offset 8374, not %v", request.Offset)
	assert.True(t, module.fetchMetadata, "Expected fetchMetadata to be true")

	// Make sure there is nothing else on the channel
	time.Sleep(100 * time.Millisecond)
	select {
	case <-module.App.StorageChannel:
		t.Fatal("Expected no additional value waiting on storage channel")
	default:
		break
	}
}

func TestKafkaCluster_getOffsets_BrokerFailed(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "cluster.test")
	module.topicPartitions = make(map[string][]int32)
	module.topicPartitions["testtopic"] = []int32{0}
	module.fetchMetadata = false

	// Set up a broker mock
	broker := &helpers.MockSaramaBroker{}
	broker.On("ID").Return(int32(13))
	var offsetResponse *sarama.OffsetResponse
	broker.On("GetAvailableOffsets", mock.MatchedBy(func(request *sarama.OffsetRequest) bool { return request != nil })).Return(offsetResponse, errors.New("broker failed"))
	broker.On("Close").Return(nil)

	// Set up the mock to return the leader broker for a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("Leader", "testtopic", int32(0)).Return(broker, nil)

	module.getOffsets(client)

	broker.AssertExpectations(t)
	client.AssertExpectations(t)
}
