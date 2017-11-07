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
	"sync"
	"time"

	"go.uber.org/zap"
	"github.com/samuel/go-zookeeper/zk"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"strconv"
	"github.com/linkedin/Burrow/core/internal/helpers"
	"regexp"
)

type TopicList struct {
	topics map[string]*PartitionCount
	lock   *sync.Mutex
}
type PartitionCount struct {
	count int32
	lock  *sync.Mutex
}

type KafkaZkClient struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.ConsumerConfig
	connectFunc     func([]string, time.Duration, *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error)

	zk              protocol.ZookeeperClient
	areWatchesSet   bool
	groupLock       *sync.Mutex
	groupList       map[string]*TopicList
	groupWhitelist  *regexp.Regexp
}

func (module *KafkaZkClient) Configure(name string) {
	module.Log.Info("configuring")

	module.name = name
	module.myConfiguration = module.App.Configuration.Consumer[name]
	module.groupLock = &sync.Mutex{}
	module.groupList = make(map[string]*TopicList)
	module.connectFunc = helpers.ZookeeperConnect

	if _, ok := module.App.Configuration.Cluster[module.myConfiguration.Cluster]; ! ok {
		panic("Consumer '" + name + "' references an unknown cluster '" + module.myConfiguration.Cluster + "'")
	}
	if len(module.myConfiguration.Servers) == 0 {
		panic("No Zookeeper servers specified for consumer " + module.name)
	} else if ! configuration.ValidateHostList(module.myConfiguration.Servers) {
		panic("Consumer '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed
	if module.App.Configuration.Consumer[module.name].ZookeeperTimeout == 0 {
		module.App.Configuration.Consumer[module.name].ZookeeperTimeout = 30
	}
	module.App.Configuration.Consumer[module.name].ZookeeperPath = module.App.Configuration.Consumer[module.name].ZookeeperPath + "/consumers"
	if ! configuration.ValidateZookeeperPath(module.App.Configuration.Consumer[module.name].ZookeeperPath) {
		panic("Consumer '" + name + "' has a bad zookeeper path configuration")
	}

	if module.App.Configuration.Consumer[module.name].GroupWhitelist != "" {
		re, err := regexp.Compile(module.App.Configuration.Consumer[module.name].GroupWhitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}
}

func (module *KafkaZkClient) Start() error {
	module.Log.Info("starting")

	zkconn, connEventChan, err := module.connectFunc(module.myConfiguration.Servers, time.Duration(module.myConfiguration.ZookeeperTimeout) * time.Second, module.Log)
	if err != nil {
		return err
	}
	module.zk = zkconn

	// Set up all groups initially (we can't count on catching the first CONNECTED event
	module.resetGroupListWatchAndAdd(false)
	module.areWatchesSet = true

	// Start up a func to watch for connection state changes and reset all the watches when needed
	go module.connectionStateWatcher(connEventChan)

	return nil
}

func (module *KafkaZkClient) Stop() error {
	module.Log.Info("stopping")

	// Closing the ZK client will invalidate all the watches, which will close all the running goroutines
	module.zk.Close()

	return nil
}

func (module *KafkaZkClient) connectionStateWatcher(eventChan <-chan zk.Event) {
	for {
		select {
		case event, isOpen := <- eventChan:
			if ! isOpen {
				// All done here
				return
			}
			if event.Type == zk.EventSession {
				switch event.State {
				case zk.StateExpired:
					module.Log.Error("session expired")
					module.areWatchesSet = false
				case zk.StateConnected:
					if ! module.areWatchesSet {
						module.Log.Info("reinitializing watches")
						module.groupLock.Lock()
						module.groupList = make(map[string]*TopicList)
						module.groupLock.Unlock()
						go module.resetGroupListWatchAndAdd(false)
					}
				}
			}
		}
	}
}

func (module *KafkaZkClient) acceptConsumerGroup(group string) bool {
	// No whitelist means everything passes
	if module.groupWhitelist == nil {
		return true
	}
	return module.groupWhitelist.MatchString(group)
}

func (module *KafkaZkClient) watchGroupList(eventChan <-chan zk.Event) {
	select {
	case event, isOpen := <-eventChan:
		if (! isOpen) || (event.Type == zk.EventNotWatching) {
			// We're done here
			return
		}
		go module.resetGroupListWatchAndAdd(event.Type != zk.EventNodeChildrenChanged)
	}
}

func (module *KafkaZkClient) resetGroupListWatchAndAdd(resetOnly bool) {
	// Get the current group list and reset our watch
	consumerGroups, _, groupListEventChan, err := module.zk.ChildrenW(module.myConfiguration.ZookeeperPath)
	if err != nil {
		// Can't read the consumers path. Bail for now
		module.Log.Error("failed to list groups", zap.String("error", err.Error()))
		return
	}
	go module.watchGroupList(groupListEventChan)

	if ! resetOnly {
		// Check for any new groups and create the watches for them
		module.groupLock.Lock()
		defer module.groupLock.Unlock()
		for _, group := range consumerGroups {
			if ! module.acceptConsumerGroup(group) {
				module.Log.Debug("skip group",
					zap.String("group", group),
					zap.String("reason", "whitelist"),
				)
				continue
			}

			if module.groupList[group] == nil {
				module.groupList[group] = &TopicList{
					topics: make(map[string]*PartitionCount),
					lock:   &sync.Mutex{},
				}
				module.Log.Info("add group",
					zap.String("group", group),
				)
				module.resetTopicListWatchAndAdd(group, false)
			}
		}
	}
}

func (module *KafkaZkClient) watchTopicList(group string, eventChan <-chan zk.Event) {
	select {
	case event, isOpen := <-eventChan:
		if (! isOpen) || (event.Type == zk.EventNotWatching) {
			// We're done here
			return
		}
		go module.resetTopicListWatchAndAdd(group, event.Type != zk.EventNodeChildrenChanged)
	}
}

func (module *KafkaZkClient) resetTopicListWatchAndAdd(group string, resetOnly bool) {
	// Get the current group topic list and reset our watch
	groupTopics, _, topicListEventChan, err := module.zk.ChildrenW(module.myConfiguration.ZookeeperPath + "/" + group + "/offsets")
	if err != nil {
		// Can't read the offsets path. Bail for now
		module.Log.Error("failed to read offsets",
			zap.String("group", group),
			zap.String("error", err.Error()),
		)
		return
	}
	go module.watchTopicList(group, topicListEventChan)

	if ! resetOnly {
		// Check for any new topics and create the watches for them
		module.groupList[group].lock.Lock()
		defer module.groupList[group].lock.Unlock()
		for _, topic := range groupTopics {
			if module.groupList[group].topics[topic] == nil {
				module.groupList[group].topics[topic] = &PartitionCount{
					count: 0,
					lock:  &sync.Mutex{},
				}
				module.Log.Debug("add topic",
					zap.String("group", group),
					zap.String("topic", topic),
				)
				module.resetPartitionListWatchAndAdd(group, topic, false)
			}
		}
	}
}

func (module *KafkaZkClient) watchPartitionList(group string, topic string, eventChan <-chan zk.Event) {
	select {
	case event, isOpen := <-eventChan:
		if (! isOpen) || (event.Type == zk.EventNotWatching) {
			// We're done here
			return
		}
		go module.resetPartitionListWatchAndAdd(group, topic, event.Type != zk.EventNodeChildrenChanged)
	}
}

func (module *KafkaZkClient) resetPartitionListWatchAndAdd(group string, topic string, resetOnly bool) {
	// Get the current topic partition list and reset our watch
	topicPartitions, _, partitionListEventChan, err := module.zk.ChildrenW(module.myConfiguration.ZookeeperPath + "/" + group + "/offsets/" + topic)
	if err != nil {
		// Can't read the consumers path. Bail for now
		module.Log.Error("failed to read partitions",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.String("error", err.Error()),
		)
		return
	}
	go module.watchPartitionList(group, topic, partitionListEventChan)

	if ! resetOnly {
		// Check for any new partitions and create the watches for them
		module.groupList[group].topics[topic].lock.Lock()
		defer module.groupList[group].topics[topic].lock.Unlock()
		if int32(len(topicPartitions)) >= module.groupList[group].topics[topic].count {
			for i := module.groupList[group].topics[topic].count; i < int32(len(topicPartitions)); i++ {
				module.Log.Debug("add partition",
					zap.String("group", group),
					zap.String("topic", topic),
					zap.Int32("partition", i),
				)
				module.resetOffsetWatchAndSend(group, topic, i, false)
			}
			module.groupList[group].topics[topic].count = int32(len(topicPartitions))
		}
	}
}

func (module *KafkaZkClient) watchOffset(group string, topic string, partition int32, eventChan <-chan zk.Event) {
	select {
	case event, isOpen := <-eventChan:
		if (! isOpen) || (event.Type == zk.EventNotWatching) {
			// We're done here
			return
		}
		go module.resetOffsetWatchAndSend(group, topic, partition, event.Type != zk.EventNodeDataChanged)
	}
}

func (module *KafkaZkClient) resetOffsetWatchAndSend(group string, topic string, partition int32, resetOnly bool) {
	// Get the current offset and reset our watch
	offsetString, offsetStat, offsetEventChan, err := module.zk.GetW(module.myConfiguration.ZookeeperPath + "/" + group + "/offsets/" + topic + "/" + strconv.FormatInt(int64(partition), 10))
	if err != nil {
		// Can't read the partition ofset path. Bail for now
		module.Log.Error("failed to read offset",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.String("error", err.Error()),
		)
		return
	}
	go module.watchOffset(group, topic, partition, offsetEventChan)

	if ! resetOnly {
		offset, err := strconv.ParseInt(string(offsetString), 10, 64)
		if err != nil {
			// Badly formatted offset
			module.Log.Error("badly formatted offset",
				zap.String("group", group),
				zap.String("topic", topic),
				zap.Int32("partition", partition),
				zap.ByteString("offset_string", offsetString),
				zap.String("error", err.Error()),
			)
			return
		}

		// Send the offset to the storage module
		partitionOffset := &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOffset,
			Cluster:     module.myConfiguration.Cluster,
			Topic:       topic,
			Partition:   int32(partition),
			Group:       group,
			Timestamp:   offsetStat.Mtime,
			Offset:      offset,
		}
		module.Log.Debug("consumer offset",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int64("offset", offset),
			zap.Int64("timestamp", offsetStat.Mtime),
		)
		helpers.TimeoutSendStorageRequest(module.App.StorageChannel, partitionOffset, 1)
	}
}
