package cluster

import (
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"

	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
	"github.com/linkedin/Burrow/core/internal/helpers"
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