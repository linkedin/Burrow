package consumer

import (
	"errors"
	"testing"

	"go.uber.org/zap"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/internal/helpers"
	"time"
)

func fixtureKafkaZkModule() *KafkaZkClient {
	module := KafkaZkClient{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		Configuration:  &configuration.Configuration{},
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	module.App.Configuration.Cluster = make(map[string]*configuration.ClusterConfig)
	module.App.Configuration.Cluster["test"] = &configuration.ClusterConfig{}

	module.App.Configuration.Consumer = make(map[string]*configuration.ConsumerConfig)
	module.App.Configuration.Consumer["test"] = &configuration.ConsumerConfig{
		ClassName: "kafkazk",
		Servers:   []string{"broker1.example.com:1234"},
		Cluster:   "test",
	}

	return &module
}

func TestKafkaZkClient_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(KafkaZkClient))
}

func TestKafkaZkClient_Configure(t *testing.T) {
	module := fixtureKafkaZkModule()
	module.Configure("test")
}

func TestKafkaZkClient_Configure_Defaults(t *testing.T) {
	module := fixtureKafkaZkModule()
	module.App.Configuration.Consumer["test"].ZookeeperTimeout = 0
	module.App.Configuration.Consumer["test"].ZookeeperPath = ""
	module.Configure("test")
	assert.Equal(t, "/consumers", module.myConfiguration.ZookeeperPath, "Expected ZookeeperPath to get set to '/consumers', not %v", module.myConfiguration.ZookeeperPath)
	assert.Equal(t, int32(30), module.myConfiguration.ZookeeperTimeout, "Default ZookeeperTimeout value of 30 did not get set")
}

func TestKafkaZkClient_Configure_BadRegexp(t *testing.T) {
	module := fixtureModule()
	module.App.Configuration.Consumer["test"].GroupWhitelist = "["
	assert.Panics(t, func() { module.Configure("test") }, "The code did not panic")
}

func TestKafkaZkClient_Start(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{
		EventChannel: make(chan zk.Event),
	}

	module := fixtureKafkaZkModule()
	module.Configure("test")
	module.connectFunc = mockZookeeper.MockZookeeperConnect

	watchEventChan := make(chan zk.Event)
	mockZookeeper.On("ChildrenW", module.myConfiguration.ZookeeperPath).Return([]string{}, &zk.Stat{}, func() <-chan zk.Event { return watchEventChan }(), nil)
	mockZookeeper.On("Close").Return()

	err := module.Start()

	// Check that there is something reading connection state events - this should not block
	mockZookeeper.EventChannel <- zk.Event{}

	module.Stop()

	assert.Nil(t, err, "Expected Start to return no error")
	assert.Equal(t, module.myConfiguration.Servers, mockZookeeper.Servers, "Expected ZookeeperConnect to be called with server list")
	assert.Equal(t, time.Duration(module.myConfiguration.ZookeeperTimeout) * time.Second, mockZookeeper.SessionTimeout, "Expected ZookeeperConnect to be called with session timeout")

	watchEventChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/shouldntgetcalled",
	}

	mockZookeeper.AssertExpectations(t)
}

// This tests all the watchers - each one will be called in turn and set, and we assure that they're all closing properly
func TestKafkaZkClient_watchGroupList(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}

	module := fixtureKafkaZkModule()
	module.App.Configuration.Consumer["test"].GroupWhitelist = "test.*"
	module.Configure("test")
	module.zk = &mockZookeeper

	offsetStat := &zk.Stat{ Mtime: 894859 }
	newGroupChan := make(chan zk.Event)
	newTopicChan := make(chan zk.Event)
	newPartitionChan := make(chan zk.Event)
	newOffsetChan := make(chan zk.Event)
	mockZookeeper.On("ChildrenW", "/consumers").Return([]string{"testgroup"}, offsetStat, func() <-chan zk.Event { return newGroupChan }(), nil)
	mockZookeeper.On("ChildrenW", "/consumers/testgroup/offsets").Return([]string{"testtopic"}, offsetStat, func() <-chan zk.Event { return newTopicChan }(), nil)
	mockZookeeper.On("ChildrenW", "/consumers/testgroup/offsets/testtopic").Return([]string{"0"}, offsetStat, func() <-chan zk.Event { return newPartitionChan }(), nil)
	mockZookeeper.On("GetW", "/consumers/testgroup/offsets/testtopic/0").Return([]byte("81234"), offsetStat, func() <-chan zk.Event { return newOffsetChan }(), nil)

	watchEventChan := make(chan zk.Event)
	go module.watchGroupList(watchEventChan)
	watchEventChan <- zk.Event{
		Type:  zk.EventNodeChildrenChanged,
		State: zk.StateConnected,
		Path:  "/consumers",
	}

	request := <- module.App.StorageChannel
	assert.Equalf(t, protocol.StorageSetConsumerOffset, request.RequestType, "Expected request sent with type StorageSetConsumerOffset, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "testtopic", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(0), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, "testgroup", request.Group, "Expected request sent with Group testgroup, not %v", request.Group)
	assert.Equalf(t, int64(81234), request.Offset, "Expected Offset to be 8372, not %v", request.Offset)
	assert.Equalf(t, int64(894859), request.Timestamp, "Expected Timestamp to be 1637, not %v", request.Timestamp)

	newGroupChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/shouldntgetcalled",
	}
	newTopicChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/testgroup/offsets/shouldntgetcalled",
	}
	newPartitionChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/testgroup/offsets/testtopic/shouldntgetcalled",
	}
	newOffsetChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/testgroup/offsets/testtopic/1/shouldntgetcalled",
	}

	mockZookeeper.AssertExpectations(t)
	assert.Equalf(t, int32(1), module.groupList["testgroup"].topics["testtopic"].count, "Expected partition count to be 1, not %v", module.groupList["testgroup"].topics["testtopic"].count)
}


func TestKafkaZkClient_resetOffsetWatchAndSend_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	mockZookeeper.On("GetW", "/consumers/testgroup/offsets/testtopic/0").Return([]byte("81234"), (*zk.Stat)(nil), (<-chan zk.Event)(nil), errors.New("badpath"))

	module := fixtureKafkaZkModule()
	module.Configure("test")
	module.zk = &mockZookeeper

	module.resetOffsetWatchAndSend("testgroup", "testtopic", 0, false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetOffsetWatchAndSend_BadOffset(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}

	module := fixtureKafkaZkModule()
	module.Configure("test")
	module.zk = &mockZookeeper

	offsetStat := &zk.Stat{ Mtime: 894859 }
	newWatchEventChan := make(chan zk.Event)
	mockZookeeper.On("GetW", "/consumers/testgroup/offsets/testtopic/0").Return([]byte("notanumber"), offsetStat, func() <-chan zk.Event { return newWatchEventChan }(), nil)

	// This will block if a storage request is sent, as nothing is watching that channel
	module.resetOffsetWatchAndSend("testgroup", "testtopic", 0, false)

	// This should not block because the watcher would have been started
	newWatchEventChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/testgroup/offsets/testtopic/1",
	}

	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetPartitionListWatchAndAdd_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	mockZookeeper.On("ChildrenW", "/consumers/testgroup/offsets/testtopic").Return([]string{}, (*zk.Stat)(nil), (<-chan zk.Event)(nil), errors.New("badpath"))

	module := fixtureKafkaZkModule()
	module.Configure("test")
	module.zk = &mockZookeeper

	module.resetPartitionListWatchAndAdd("testgroup", "testtopic", false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetTopicListWatchAndAdd_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	mockZookeeper.On("ChildrenW", "/consumers/testgroup/offsets").Return([]string{}, (*zk.Stat)(nil), (<-chan zk.Event)(nil), errors.New("badpath"))

	module := fixtureKafkaZkModule()
	module.Configure("test")
	module.zk = &mockZookeeper

	module.resetTopicListWatchAndAdd("testgroup", false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetGroupListWatchAndAdd_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	mockZookeeper.On("ChildrenW", "/consumers").Return([]string{}, (*zk.Stat)(nil), (<-chan zk.Event)(nil), errors.New("badpath"))

	module := fixtureKafkaZkModule()
	module.Configure("test")
	module.zk = &mockZookeeper

	module.resetGroupListWatchAndAdd(false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetGroupListWatchAndAdd_WhiteList(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}

	module := fixtureKafkaZkModule()
	module.App.Configuration.Consumer["test"].GroupWhitelist = "test.*"
	module.Configure("test")
	module.zk = &mockZookeeper

	offsetStat := &zk.Stat{ Mtime: 894859 }
	newGroupChan := make(chan zk.Event)
	mockZookeeper.On("ChildrenW", "/consumers").Return([]string{"dropthisgroup"}, offsetStat, func() <-chan zk.Event { return newGroupChan }(), nil)

	module.resetGroupListWatchAndAdd(false)

	newGroupChan <- zk.Event{
		Type:  zk.EventNotWatching,
		State: zk.StateConnected,
		Path:  "/consumers/shouldntgetcalled",
	}

	mockZookeeper.AssertExpectations(t)
	_, ok := module.groupList["dropthisgroup"]
	assert.False(t, ok, "Expected group to be dropped due to whitelist")
}
