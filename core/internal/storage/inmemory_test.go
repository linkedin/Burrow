package storage

import (
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"

	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
)

func fixtureModule(whitelist string) *InMemoryStorage {
	module := InMemoryStorage{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		Configuration:  &configuration.Configuration{},
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	module.App.Configuration.Storage = make(map[string]*configuration.StorageConfig)
	module.App.Configuration.Storage["test"] = &configuration.StorageConfig{
		ClassName: "inmemory",
		Intervals: 10,
		MinDistance: 1,
		GroupWhitelist: whitelist,
	}

	return &module
}

func startWithTestCluster(whitelist string) *InMemoryStorage {
	module := fixtureModule(whitelist)

	// Start needs at least one cluster defined, but it only needs to have a name here
	module.App.Configuration.Cluster = make(map[string]*configuration.ClusterConfig)
	module.App.Configuration.Cluster["testcluster"] = &configuration.ClusterConfig{}
	module.Configure("test")
	module.Start()
	return module
}

func startWithTestBrokerOffsets(whitelist string) *InMemoryStorage {
	module := startWithTestCluster(whitelist)

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetBrokerOffset,
		Cluster:             "testcluster",
		Topic:               "testtopic",
		Partition:           0,
		TopicPartitionCount: 1,
		Offset:              4321,
		Timestamp:           9876,
	}
	module.addBrokerOffset(&request, module.Log)
	return module
}

func startWithTestConsumerOffsets(whitelist string) *InMemoryStorage {
	module := startWithTestBrokerOffsets(whitelist)

	request := protocol.StorageRequest{
		RequestType: protocol.StorageSetConsumerOffset,
		Cluster:     "testcluster",
		Topic:       "testtopic",
		Group:       "testgroup",
		Partition:   0,
	}
	for i := 0; i < 10; i++ {
		request.Offset = int64(1000 + (i * 100))
		request.Timestamp = int64(100000 + (i * 10000))
		module.addConsumerOffset(&request, module.Log)
	}
	return module
}

func TestInMemoryStorage_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(InMemoryStorage))
}

func TestInMemoryStorage_ImplementsStorageModule(t *testing.T) {
	assert.Implements(t, (*StorageModule)(nil), new(InMemoryStorage))
}

func TestInMemoryStorage_Configure(t *testing.T) {
	module := fixtureModule("")
	module.Configure("test")
}

func TestInMemoryStorage_Configure_DefaultIntervals(t *testing.T) {
	module := fixtureModule("")
	module.App.Configuration.Storage["test"].Intervals = 0
	module.Configure("test")
	assert.Equal(t, 10, module.myConfiguration.Intervals, "Default Intervals value of 10 did not get set")
}

func TestInMemoryStorage_Configure_BadRegexp(t *testing.T) {
	module := fixtureModule("")
	module.App.Configuration.Storage["test"].GroupWhitelist = "["
	assert.Panics(t, func() { module.Configure("test") }, "The code did not panic")
}

func TestInMemoryStorage_Start(t *testing.T) {
	module := startWithTestCluster("")
	assert.Len(t, module.offsets, 1, "Module start did not define 1 cluster")
}

func TestInMemoryStorage_Stop(t *testing.T) {
	module := startWithTestCluster("")
	module.Stop()
}

func TestInMemoryStorage_addBrokerOffset(t *testing.T) {
	module := startWithTestBrokerOffsets("")

	topicList, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.True(t, ok, "Topic not created")
	assert.Len(t, topicList, 1, "One partition not created")
	assert.NotNil(t, topicList[0], "BrokerOffset object not created")
	assert.Equalf(t, int64(4321), topicList[0].Offset, "Expected offset to be 4321, got %v", topicList[0].Offset)
	assert.Equalf(t, int64(9876), topicList[0].Timestamp, "Expected timestamp to be 9876, got %v", topicList[0].Timestamp)
}

func TestInMemoryStorage_addBrokerOffset_ExistingTopic(t *testing.T) {
	module := startWithTestCluster("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetBrokerOffset,
		Cluster:             "testcluster",
		Topic:               "testtopic",
		Partition:           0,
		TopicPartitionCount: 2,
		Offset:              4321,
		Timestamp:           9876,
	}
	module.addBrokerOffset(&request, module.Log)

	request.Partition = 1
	request.Offset = 5432
	request.Timestamp = 8765
	module.addBrokerOffset(&request, module.Log)

	topicList, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.True(t, ok, "Topic not created")
	assert.Len(t, topicList, 2, "Two partitions not created")

	assert.NotNil(t, topicList[0], "BrokerOffset object for p0 not created")
	assert.Equalf(t, int64(4321), topicList[0].Offset, "Expected offset for p0 to be 4321, got %v", topicList[0].Offset)
	assert.Equalf(t, int64(9876), topicList[0].Timestamp, "Expected timestamp for p0 to be 9876, got %v", topicList[0].Timestamp)

	assert.NotNil(t, topicList[1], "BrokerOffset object for p1 not created")
	assert.Equalf(t, int64(5432), topicList[1].Offset, "Expected offset for p1 to be 5432, got %v", topicList[1].Offset)
	assert.Equalf(t, int64(8765), topicList[1].Timestamp, "Expected timestamp for p1 to be 8765, got %v", topicList[1].Timestamp)
}

func TestInMemoryStorage_addBrokerOffset_ExistingPartition(t *testing.T) {
	module := startWithTestBrokerOffsets("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetBrokerOffset,
		Cluster:             "testcluster",
		Topic:               "testtopic",
		Partition:           0,
		TopicPartitionCount: 1,
		Offset:              5432,
		Timestamp:           8765,
	}
	module.addBrokerOffset(&request, module.Log)

	topicList, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.True(t, ok, "Topic not created")
	assert.Len(t, topicList, 1, "One partition not created")

	assert.NotNil(t, topicList[0], "BrokerOffset object for p0 not created")
	assert.Equalf(t, int64(5432), topicList[0].Offset, "Expected offset for p0 to be 5432, got %v", topicList[0].Offset)
	assert.Equalf(t, int64(8765), topicList[0].Timestamp, "Expected timestamp for p0 to be 8765, got %v", topicList[0].Timestamp)
}

func TestInMemoryStorage_addBrokerOffset_BadCluster(t *testing.T) {
	module := startWithTestCluster("")
	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetBrokerOffset,
		Cluster:             "nocluster",
		Topic:               "testtopic",
		Partition:           0,
		TopicPartitionCount: 1,
		Offset:              4321,
		Timestamp:           9876,
	}
	module.addBrokerOffset(&request, module.Log)

	assert.Len(t, module.offsets, 1, "Extra cluster exists")
	_, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.False(t, ok, "Topic created in wrong cluster")
}

func TestInMemoryStorage_getBrokerOffset(t *testing.T) {
	module := startWithTestCluster("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetBrokerOffset,
		Cluster:             "testcluster",
		Topic:               "testtopic",
		Partition:           0,
		TopicPartitionCount: 2,
		Offset:              4321,
		Timestamp:           9876,
	}
	module.addBrokerOffset(&request, module.Log)

	clusterMap := module.offsets["testcluster"]
	offset, partitions := module.getBrokerOffset(&clusterMap, "testtopic", 0, module.Log)
	assert.Equalf(t, int64(4321), offset, "Expected offset to be 4321, got %v", offset)
	assert.Equalf(t, int32(2), partitions, "Expected partitions to be 2, got %v", partitions)

	offset, partitions = module.getBrokerOffset(&clusterMap, "notopic", 0, module.Log)
	assert.Equalf(t, int64(0), offset, "Expected offset to be 0, got %v", offset)
	assert.Equalf(t, int32(0), partitions, "Expected partitions to be 0, got %v", partitions)

	offset, partitions = module.getBrokerOffset(&clusterMap, "testtopic", 2, module.Log)
	assert.Equalf(t, int64(0), offset, "Expected offset to be 0, got %v", offset)
	assert.Equalf(t, int32(0), partitions, "Expected partitions to be 0, got %v", partitions)

	offset, partitions = module.getBrokerOffset(&clusterMap, "testtopic", 1, module.Log)
	assert.Equalf(t, int64(0), offset, "Expected offset to be 0, got %v", offset)
	assert.Equalf(t, int32(0), partitions, "Expected partitions to be 0, got %v", partitions)

	offset, partitions = module.getBrokerOffset(&clusterMap, "testtopic", -1, module.Log)
	assert.Equalf(t, int64(0), offset, "Expected offset to be 0, got %v", offset)
	assert.Equalf(t, int32(0), partitions, "Expected partitions to be 0, got %v", partitions)
}

func TestInMemoryStorage_addConsumerOffset(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageSetConsumerOffset,
		Cluster:     "testcluster",
		Topic:       "testtopic",
		Group:       "testgroup",
		Partition:   0,
		Offset:      2000,
		Timestamp:   200000,
	}
	module.addConsumerOffset(&request, module.Log)

	consumerMap, ok := module.offsets["testcluster"].consumer["testgroup"]
	assert.True(t, ok, "Group not created")
	partitions, ok := consumerMap.topics["testtopic"]
	assert.True(t, ok, "Topic not created")
	assert.Len(t, partitions, 1, "One partition not created")
	assert.Equal(t, 10, partitions[0].Len(), "10 offset ring entries not created")

	// All the ring values should be not nil
	r := partitions[0]
	for i := 0; i < 10; i++ {
		assert.NotNilf(t, r.Value, "Expected ring value to be NOT nil at position %v", i)
		assert.IsType(t, new(protocol.ConsumerOffset), r.Value, "Expected ring value to be of type ConsumerOffset")

		offset := r.Value.(*protocol.ConsumerOffset)
		offsetValue := int64(1100 + (i * 100))
		timestampValue := int64(110000 + (i * 10000))
		lagValue := int64(4321) - offsetValue

		assert.Equalf(t, offsetValue, offset.Offset, "Expected offset at position %v to be %v, got %v", i, offsetValue, offset.Offset)
		assert.Equalf(t, timestampValue, offset.Timestamp, "Expected timestamp at position %v to be %v, got %v", i, timestampValue, offset.Timestamp)
		assert.Equalf(t, lagValue, offset.Lag, "Expected lag at position %v to be %v, got %v", i, lagValue, offset.Lag)

		r = r.Next()
	}
}

func TestInMemoryStorage_addConsumerOffset_Whitelist(t *testing.T) {
	module := startWithTestConsumerOffsets("whitelistedgroup")

	// All offsets for the test group should have been dropped
	_, ok := module.offsets["testcluster"].consumer["testgroup"]
	assert.False(t, ok, "Group testgroup created when not whitelisted")
}

type testset struct {
	whitelist  string
	passGroups []string
	failGroups []string
}

var whitelist_tests = []testset{
	{"", []string{"testgroup", "ok_group", "dash-group", "num02group"}, []string{}},
	{"test.*", []string{"testgroup"}, []string{"ok_group", "dash-group", "num02group"}},
	{".*[0-9]+.*", []string{"num02group"}, []string{"ok_group", "dash-group", "testgroup"}},
	{"onlygroup", []string{"onlygroup"}, []string{"testgroup", "ok_group", "dash-group", "num02group"}},
}

func TestInMemoryStorage_acceptConsumerGroup_NoWhitelist(t *testing.T) {
	for i, testSet := range whitelist_tests {
		module := fixtureModule(testSet.whitelist)
		module.Configure("test")

		for _, group := range testSet.passGroups {
			result := module.acceptConsumerGroup(group)
			assert.Truef(t, result, "TEST %v: Expected group %v to pass", i, group)
		}
		for _, group := range testSet.failGroups {
			result := module.acceptConsumerGroup(group)
			assert.Falsef(t, result, "TEST %v: Expected group %v to fail", i, group)
		}
	}
}

func TestInMemoryStorage_addConsumerOffset_MinDistance(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	// This commit violates min-distance and should not cause the ring to advance
	request := protocol.StorageRequest{
		RequestType: protocol.StorageSetConsumerOffset,
		Cluster:     "testcluster",
		Topic:       "testtopic",
		Group:       "testgroup",
		Partition:   0,
		Offset:      2000,
		Timestamp:   190001,
	}
	module.addConsumerOffset(&request, module.Log)

	consumerMap, ok := module.offsets["testcluster"].consumer["testgroup"]
	assert.True(t, ok, "Group not created")
	partitions, ok := consumerMap.topics["testtopic"]
	assert.True(t, ok, "Topic not created")
	assert.Len(t, partitions, 1, "One partition not created")
	assert.Equal(t, 10, partitions[0].Len(), "10 offset ring entries not created")

	// All the ring values should be not nil
	r := partitions[0]
	for i := 0; i < 10; i++ {
		assert.NotNilf(t, r.Value, "Expected ring value to be NOT nil at position %v", i)
		assert.IsType(t, new(protocol.ConsumerOffset), r.Value, "Expected ring value to be of type ConsumerOffset")

		offset := r.Value.(*protocol.ConsumerOffset)
		offsetValue := int64(1000 + (i * 100))
		timestampValue := int64(100000 + (i * 10000))
		if i == 9 {
			// The last offset in the ring is the one that got the min-distance update
			offsetValue = 2000
			timestampValue = 190001
		}
		lagValue := int64(4321) - offsetValue

		assert.Equalf(t, offsetValue, offset.Offset, "Expected offset at position %v to be %v, got %v", i, offsetValue, offset.Offset)
		assert.Equalf(t, timestampValue, offset.Timestamp, "Expected timestamp at position %v to be %v, got %v", i, timestampValue, offset.Timestamp)
		assert.Equalf(t, lagValue, offset.Lag, "Expected lag at position %v to be %v, got %v", i, lagValue, offset.Lag)

		r = r.Next()
	}
}

func TestInMemoryStorage_addConsumerOffset_BadBrokerOffset(t *testing.T) {
	module := startWithTestBrokerOffsets("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetConsumerOffset,
		Cluster:             "testcluster",
		Topic:               "notopic",
		Group:               "testgroup",
		Partition:           0,
		Offset:              3434,
		Timestamp:           5677,
	}
	module.addConsumerOffset(&request, module.Log)

	// We're only testing one case, as we've previously tested getBrokerOffset to make sure it works completely
	_, ok := module.offsets["testcluster"].consumer["testgroup"]
	assert.False(t, ok, "Group created, but offset should have been dropped")
}

func TestInMemoryStorage_addConsumerOffset_BadCluster(t *testing.T) {
	module := startWithTestBrokerOffsets("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetConsumerOffset,
		Cluster:             "nocluster",
		Topic:               "testtopic",
		Group:               "testgroup",
		Partition:           0,
		Offset:              3434,
		Timestamp:           5677,
	}
	module.addConsumerOffset(&request, module.Log)

	assert.Len(t, module.offsets, 1, "Extra cluster exists")
	_, ok := module.offsets["testcluster"].consumer["testgroup"]
	assert.False(t, ok, "Group created in wrong cluster")
}

func TestInMemoryStorage_deleteTopic(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetDeleteTopic,
		Cluster:             "testcluster",
		Topic:               "testtopic",
	}
	module.deleteTopic(&request, module.Log)

	_, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.False(t, ok, "Topic not deleted from broker offsets")
	consumerMap, ok := module.offsets["testcluster"].consumer["testgroup"]
	_, ok = consumerMap.topics["testtopic"]
	assert.False(t, ok, "Topic not deleted from group offsets")
}

func TestInMemoryStorage_deleteTopic_BadCluster(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetDeleteTopic,
		Cluster:             "nocluster",
		Topic:               "testtopic",
	}
	module.deleteTopic(&request, module.Log)

	assert.Len(t, module.offsets, 1, "Extra cluster exists")
	_, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.True(t, ok, "Topic deleted in wrong cluster")
}

func TestInMemoryStorage_deleteTopic_NoTopic(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType:         protocol.StorageSetDeleteTopic,
		Cluster:             "testcluster",
		Topic:               "notopic",
	}
	module.deleteTopic(&request, module.Log)

	_, ok := module.offsets["testcluster"].broker["testtopic"]
	assert.True(t, ok, "Wrong topic deleted from broker offsets")
	consumerMap, ok := module.offsets["testcluster"].consumer["testgroup"]
	_, ok = consumerMap.topics["testtopic"]
	assert.True(t, ok, "Wrong topic deleted from group offsets")
}

func TestInMemoryStorage_fetchClusterList(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchClusterList(&request, module.Log)
	response := <- request.Reply

	assert.IsType(t, []string{}, response, "Expected response to be of type []string")
	val := response.([]string)
	assert.Len(t, val, 1, "One entry not returned")
	assert.Equalf(t, val[0], "testcluster", "Expected return value to be 'testcluster', not %v", val[0])

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchTopicList(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopics,
		Cluster:     "testcluster",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchTopicList(&request, module.Log)
	response := <- request.Reply

	assert.IsType(t, []string{}, response, "Expected response to be of type []string")
	val := response.([]string)
	assert.Len(t, val, 1, "One entry not returned")
	assert.Equalf(t, val[0], "testtopic", "Expected return value to be 'testtopic', not %v", val[0])

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchTopicList_BadCluster(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopics,
		Cluster:     "nocluster",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchTopicList(&request, module.Log)
	response, ok := <- request.Reply

	assert.Nil(t, response, "Expected response to be nil")
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchConsumerList(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumers,
		Cluster:     "testcluster",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchConsumerList(&request, module.Log)
	response := <- request.Reply

	assert.IsType(t, []string{}, response, "Expected response to be of type []string")
	val := response.([]string)
	assert.Len(t, val, 1, "One entry not returned")
	assert.Equalf(t, val[0], "testgroup", "Expected return value to be 'testgroup', not %v", val[0])

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchConsumerList_BadCluster(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumers,
		Cluster:     "nocluster",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchConsumerList(&request, module.Log)
	response, ok := <- request.Reply

	assert.Nil(t, response, "Expected response to be nil")
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchTopic(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopic,
		Cluster:     "testcluster",
		Topic:       "testtopic",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchTopic(&request, module.Log)
	response := <- request.Reply

	assert.IsType(t, []int64{}, response, "Expected response to be of type []int64")
	val := response.([]int64)
	assert.Len(t, val, 1, "One partition not returned")
	assert.Equalf(t, val[0], int64(4321), "Expected return value to be 4321, not %v", val[0])

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchTopic_BadCluster(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopic,
		Cluster:     "nocluster",
		Topic:       "testtopic",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchTopic(&request, module.Log)
	response, ok := <- request.Reply

	assert.Nil(t, response, "Expected response to be nil")
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchTopic_BadTopic(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopic,
		Cluster:     "testcluster",
		Topic:       "notopic",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchTopic(&request, module.Log)
	response, ok := <- request.Reply

	assert.Nil(t, response, "Expected response to be nil")
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchConsumer(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumer,
		Cluster:     "testcluster",
		Group:       "testgroup",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchConsumer(&request, module.Log)
	response := <- request.Reply

	assert.IsType(t, protocol.ConsumerTopics{}, response, "Expected response to be of type map[string][]*protocol.ConsumerPartition")
	val := response.(protocol.ConsumerTopics)
	assert.Len(t, val, 1, "One topic for consumer not returned")
	_, ok := val["testtopic"]
	assert.True(t, ok, "Expected response to contain topic testtopic")
	assert.Len(t, val["testtopic"], 1, "One partition for topic not returned")
	assert.Equalf(t, val["testtopic"][0].CurrentLag, int64(2421), "Expected current lag to be 2421, not %v", val["testtopic"][0].CurrentLag)

	offsets := val["testtopic"][0].Offsets
	assert.Lenf(t, offsets, 10, "Expected to get 10 offsets for the partition, not %v", len(offsets))
	for i := 0; i < 10; i++ {
		assert.NotNilf(t, offsets[0], "Expected offset to be NOT nil at position %v", i)

		offsetValue := int64(1000 + (i * 100))
		timestampValue := int64(100000 + (i * 10000))
		lagValue := int64(4321) - offsetValue

		assert.Equalf(t, offsetValue, offsets[i].Offset, "Expected offset at position %v to be %v, got %v", i, offsetValue, offsets[i].Offset)
		assert.Equalf(t, timestampValue, offsets[i].Timestamp, "Expected timestamp at position %v to be %v, got %v", i, timestampValue, offsets[i].Timestamp)
		assert.Equalf(t, lagValue, offsets[i].Lag, "Expected lag at position %v to be %v, got %v", i, lagValue, offsets[i].Lag)
	}

	response, ok = <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchConsumer_BadCluster(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumer,
		Cluster:     "nocluster",
		Group:       "testgroup",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchConsumer(&request, module.Log)
	response, ok := <- request.Reply

	assert.Nil(t, response, "Expected response to be nil")
	assert.False(t, ok, "Expected channel to be closed")
}

func TestInMemoryStorage_fetchConsumer_BadTopic(t *testing.T) {
	module := startWithTestConsumerOffsets("")

	request := protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumer,
		Cluster:     "testcluster",
		Group:       "nogroup",
		Reply:       make(chan interface{}),
	}

	// Can't read a reply without concurrency
	go module.fetchConsumer(&request, module.Log)
	response, ok := <- request.Reply

	assert.Nil(t, response, "Expected response to be nil")
	assert.False(t, ok, "Expected channel to be closed")
}