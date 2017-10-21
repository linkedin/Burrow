package cluster

import (
	"errors"
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"

	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	"time"
)

func fixtureModule() *KafkaCluster {
	module := KafkaCluster{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		Configuration:  &configuration.Configuration{},
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	module.App.Configuration.ClientProfile = make(map[string]*configuration.ClientProfile)
	module.App.Configuration.ClientProfile[""] = &configuration.ClientProfile{
		ClientID: "testid",
	}

	module.App.Configuration.Cluster = make(map[string]*configuration.ClusterConfig)
	module.App.Configuration.Cluster["test"] = &configuration.ClusterConfig{
		ClassName: "kafka",
		Servers:   []string{"broker1.example.com:1234"},
	}

	return &module
}

func TestKafkaCluster_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(KafkaCluster))
}

func TestKafkaCluster_Configure(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")
	assert.NotNil(t, module.saramaConfig, "Expected saramaConfig to be populated")
}

func TestKafkaCluster_Configure_DefaultIntervals(t *testing.T) {
	module := fixtureModule()
	module.App.Configuration.Cluster["test"].OffsetRefresh = 0
	module.App.Configuration.Cluster["test"].TopicRefresh = 0
	module.Configure("test")
	assert.Equal(t, int64(10), module.myConfiguration.OffsetRefresh, "Default OffsetRefresh value of 10 did not get set")
	assert.Equal(t, int64(60), module.myConfiguration.TopicRefresh, "Default TopicRefresh value of 60 did not get set")
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_NoUpdate(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")
	client := &helpers.MockSaramaClient{}

    module.maybeUpdateMetadataAndDeleteTopics(client)
    client.AssertNotCalled(t, "RefreshMetadata")
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_NoDelete(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")

	// Set up the mock to return a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("RefreshMetadata").Return(nil)
	client.On("Topics").Return([]string{"testtopic"}, nil)
	client.On("Partitions", "testtopic").Return([]int32{0}, nil)

	module.fetchMetadata = true
	module.maybeUpdateMetadataAndDeleteTopics(client)

	client.AssertExpectations(t)
	assert.False(t, module.fetchMetadata, "Expected fetchMetadata to be reset to false")
	assert.Lenf(t, module.topicMap, 1, "Expected 1 topic entry, not %v", len(module.topicMap))
	topic, ok := module.topicMap["testtopic"]
	assert.True(t, ok, "Expected to find testtopic in topicMap")
	assert.Equalf(t, 1, topic, "Expected testtopic to be recorded with 1 partition, not %v", topic)
}

func TestKafkaCluster_maybeUpdateMetadataAndDeleteTopics_Delete(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")

	// Set up the mock to return a test topic and partition
	client := &helpers.MockSaramaClient{}
	client.On("RefreshMetadata").Return(nil)
	client.On("Topics").Return([]string{"testtopic"}, nil)
	client.On("Partitions", "testtopic").Return([]int32{0}, nil)

	module.fetchMetadata = true
	module.topicMap = make(map[string]int)
	module.topicMap["topictodelete"] = 10
	go module.maybeUpdateMetadataAndDeleteTopics(client)
	request := <- module.App.StorageChannel

	client.AssertExpectations(t)
	assert.False(t, module.fetchMetadata, "Expected fetchMetadata to be reset to false")
	assert.Lenf(t, module.topicMap, 1, "Expected 1 topic entry, not %v", len(module.topicMap))
	topic, ok := module.topicMap["testtopic"]
	assert.True(t, ok, "Expected to find testtopic in topicMap")
	assert.Equalf(t, 1, topic, "Expected testtopic to be recorded with 1 partition, not %v", topic)

	assert.Equalf(t, protocol.StorageSetDeleteTopic, request.RequestType, "Expected request sent with type StorageSetDeleteTopic, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "topictodelete", request.Topic, "Expected request sent with topic topictodelete, not %v", request.Topic)
}

func TestKafkaCluster_generateOffsetRequests(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")
	module.topicMap = make(map[string]int)
	module.topicMap["testtopic"] = 1

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
	module.Configure("test")
	module.topicMap = make(map[string]int)
	module.topicMap["testtopic"] = 2

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
}

func TestKafkaCluster_getOffsets(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")
	module.topicMap = make(map[string]int)
	module.topicMap["testtopic"] = 2
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
	request := <- module.App.StorageChannel

	broker.AssertExpectations(t)
	client.AssertExpectations(t)
	assert.Equalf(t, protocol.StorageSetBrokerOffset, request.RequestType, "Expected request sent with type StorageSetBrokerOffset, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "testtopic", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(0), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, int32(2), request.TopicPartitionCount, "Expected request sent with TopicPartitionCount 2, not %v", request.TopicPartitionCount)
	assert.Equalf(t, int64(8374), request.Offset, "Expected request sent with offset 8374, not %v", request.Offset)

	// Make sure there is nothing else on the channel
	time.Sleep(100 * time.Millisecond)
	select {
	case <- module.App.StorageChannel:
		t.Fatal("Expected no additional value waiting on storage channel")
	default:
		break
	}
}

func TestKafkaCluster_getOffsets_BrokerFailed(t *testing.T) {
	module := fixtureModule()
	module.Configure("test")
	module.topicMap = make(map[string]int)
	module.topicMap["testtopic"] = 1
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
