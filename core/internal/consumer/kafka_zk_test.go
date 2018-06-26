/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package consumer

import (
	"errors"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/stretchr/testify/mock"
	"sync"
)

func fixtureKafkaZkModule() *KafkaZkClient {
	module := KafkaZkClient{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	viper.Reset()
	viper.Set("cluster.test.class-name", "kafka")
	viper.Set("cluster.test.servers", []string{"broker1.example.com:1234"})
	viper.Set("consumer.test.class-name", "kafka_zk")
	viper.Set("consumer.test.servers", []string{"broker1.example.com:1234"})
	viper.Set("consumer.test.cluster", "test")

	return &module
}

func TestKafkaZkClient_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(KafkaZkClient))
}

func TestKafkaZkClient_Configure(t *testing.T) {
	module := fixtureKafkaZkModule()
	module.Configure("test", "consumer.test")
	assert.Equal(t, "/consumers", module.zookeeperPath, "Expected ZookeeperPath to get set to '/consumers', not %v", module.zookeeperPath)
	assert.Equal(t, int(30), module.zookeeperTimeout, "Default ZookeeperTimeout value of 30 did not get set")
}

func TestKafkaZkClient_Configure_BadRegexp(t *testing.T) {
	module := fixtureModule()
	viper.Set("consumer.test.group-whitelist", "[")
	assert.Panics(t, func() { module.Configure("test", "consumer.test") }, "The code did not panic")
}

func TestKafkaZkClient_Start(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{
		EventChannel: make(chan zk.Event),
	}

	module := fixtureKafkaZkModule()
	module.Configure("test", "consumer.test")
	module.connectFunc = mockZookeeper.MockZookeeperConnect

	watchEventChan := make(chan zk.Event)
	mockZookeeper.On("ChildrenW", module.zookeeperPath).Return([]string{}, &zk.Stat{}, func() <-chan zk.Event { return watchEventChan }(), nil)
	mockZookeeper.On("Close").Return().Run(func(args mock.Arguments) {
		watchEventChan <- zk.Event{Type: zk.EventNotWatching}
		close(watchEventChan)
	})

	err := module.Start()

	// Check that there is something reading connection state events - this should not block
	mockZookeeper.EventChannel <- zk.Event{}

	module.Stop()

	assert.Nil(t, err, "Expected Start to return no error")
	assert.Equal(t, module.servers, mockZookeeper.Servers, "Expected ZookeeperConnect to be called with server list")
	assert.Equal(t, time.Duration(module.zookeeperTimeout)*time.Second, mockZookeeper.SessionTimeout, "Expected ZookeeperConnect to be called with session timeout")

	mockZookeeper.AssertExpectations(t)
}

// This tests all the watchers - each one will be called in turn and set, and we assure that they're all closing properly
func TestKafkaZkClient_watchGroupList(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}

	module := fixtureKafkaZkModule()
	viper.Set("consumer.test.group-whitelist", "test.*")
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	offsetStat := &zk.Stat{Mtime: 894859}
	newGroupChan := make(chan zk.Event)
	topicExistsChan := make(chan zk.Event)
	newTopicChan := make(chan zk.Event)
	newPartitionChan := make(chan zk.Event)
	newOffsetChan := make(chan zk.Event)
	mockZookeeper.On("ChildrenW", "/consumers").Return([]string{"testgroup"}, offsetStat, func() <-chan zk.Event { return newGroupChan }(), nil)
	mockZookeeper.On("ChildrenW", "/consumers/testgroup/offsets").Return([]string{"testtopic"}, offsetStat, func() <-chan zk.Event { return newTopicChan }(), nil)
	mockZookeeper.On("ExistsW", "/consumers/testgroup/offsets").Return(true, offsetStat, func() <-chan zk.Event { return topicExistsChan }(), nil)
	mockZookeeper.On("ChildrenW", "/consumers/testgroup/offsets/testtopic").Return([]string{"0"}, offsetStat, func() <-chan zk.Event { return newPartitionChan }(), nil)
	mockZookeeper.On("GetW", "/consumers/testgroup/offsets/testtopic/0").Return([]byte("81234"), offsetStat, func() <-chan zk.Event { return newOffsetChan }(), nil)
	mockZookeeper.On("GetW", "/consumers/testgroup/owners/testtopic/0").Return([]byte("testowner"), offsetStat, func() <-chan zk.Event { return newOffsetChan }(), nil)

	watchEventChan := make(chan zk.Event)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		watchEventChan <- zk.Event{
			Type:  zk.EventNodeChildrenChanged,
			State: zk.StateConnected,
			Path:  "/consumers",
		}

		request := <-module.App.StorageChannel
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
		topicExistsChan <- zk.Event{
			Type:  zk.EventNotWatching,
			State: zk.StateConnected,
			Path:  "/consumers/testgroup/offsets",
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

		secondRequest := <-module.App.StorageChannel
		assert.Equalf(t, protocol.StorageSetConsumerOwner, secondRequest.RequestType, "Expected request sent with type StorageSetConsumerOwner, not %v", secondRequest.RequestType)
		assert.Equalf(t, "testowner", secondRequest.Owner, "Expected request sent with Owner testowner, not %v", secondRequest.Owner)
	}()

	module.running.Add(1)
	module.watchGroupList(watchEventChan)
	module.running.Wait()

	mockZookeeper.AssertExpectations(t)
	assert.Equalf(t, int32(1), module.groupList["testgroup"].topics["testtopic"].count, "Expected partition count to be 1, not %v", module.groupList["testgroup"].topics["testtopic"].count)
}

func TestKafkaZkClient_resetOffsetWatchAndSend_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	mockZookeeper.On("GetW", "/consumers/testgroup/offsets/testtopic/0").Return([]byte("81234"), (*zk.Stat)(nil), (<-chan zk.Event)(nil), errors.New("badpath"))
	mockZookeeper.On("GetW", "/consumers/testgroup/owners/testtopic/0").Return([]byte("testowner"), (*zk.Stat)(nil), (<-chan zk.Event)(nil), nil)

	module := fixtureKafkaZkModule()
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	module.running.Add(1)
	module.resetOffsetWatchAndSend("testgroup", "testtopic", 0, false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetOffsetWatchAndSend_BadOffset(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}

	module := fixtureKafkaZkModule()
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	offsetStat := &zk.Stat{Mtime: 894859}
	newWatchEventChan := make(chan zk.Event)
	mockZookeeper.On("GetW", "/consumers/testgroup/offsets/testtopic/0").Return([]byte("notanumber"), offsetStat, func() <-chan zk.Event { return newWatchEventChan }(), nil)
	mockZookeeper.On("GetW", "/consumers/testgroup/owners/testtopic/0").Return([]byte("testowner"), (*zk.Stat)(nil), (<-chan zk.Event)(nil), nil)

	// This will block if a storage request is sent, as nothing is watching that channel
	module.running.Add(1)
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
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	module.running.Add(1)
	module.resetPartitionListWatchAndAdd("testgroup", "testtopic", false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetTopicListWatchAndAdd_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	topicExistsChan := make(chan zk.Event)
	mockZookeeper.On("ExistsW", "/consumers/testgroup/offsets").Return(false, (*zk.Stat)(nil), func() <-chan zk.Event { return topicExistsChan }(), nil)

	module := fixtureKafkaZkModule()
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	go func() {
		topicExistsChan <- zk.Event{
			Type:  zk.EventNotWatching,
			State: zk.StateConnected,
			Path:  "/consumers/testgroup/offsets",
		}
	}()
	module.running.Add(1)
	module.resetTopicListWatchAndAdd("testgroup", false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetGroupListWatchAndAdd_BadPath(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}
	mockZookeeper.On("ChildrenW", "/consumers").Return([]string{}, (*zk.Stat)(nil), (<-chan zk.Event)(nil), errors.New("badpath"))

	module := fixtureKafkaZkModule()
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	module.running.Add(1)
	module.resetGroupListWatchAndAdd(false)
	mockZookeeper.AssertExpectations(t)
}

func TestKafkaZkClient_resetGroupListWatchAndAdd_WhiteList(t *testing.T) {
	mockZookeeper := helpers.MockZookeeperClient{}

	module := fixtureKafkaZkModule()
	viper.Set("consumer.test.group-whitelist", "test.*")
	module.Configure("test", "consumer.test")
	module.zk = &mockZookeeper

	offsetStat := &zk.Stat{Mtime: 894859}
	newGroupChan := make(chan zk.Event)
	mockZookeeper.On("ChildrenW", "/consumers").Return([]string{"dropthisgroup"}, offsetStat, func() <-chan zk.Event { return newGroupChan }(), nil)

	module.running.Add(1)
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
