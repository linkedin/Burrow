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
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type topicList struct {
	topics map[string]*partitionCount
	lock   *sync.RWMutex
}
type partitionCount struct {
	count int32
	lock  *sync.Mutex
}

// KafkaZkClient is a consumer module which connects to the Zookeeper ensemble where an Apache Kafka cluster maintains
// metadata, and reads consumer group information from the /consumers tree (older ZK-based consumers). It uses watches
// to monitor every group and offset, and the information is forwarded to the storage subsystem for use in evaluations.
type KafkaZkClient struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name             string
	cluster          string
	servers          []string
	zookeeperTimeout int
	zookeeperPath    string

	zk             protocol.ZookeeperClient
	areWatchesSet  bool
	running        *sync.WaitGroup
	groupLock      *sync.RWMutex
	groupList      map[string]*topicList
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	connectFunc    func([]string, time.Duration, *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error)
}

// Configure validates the configuration for the consumer. At minimum, there must be a cluster name to which these
// consumers belong, as well as a list of servers provided for the Zookeeper ensemble, of the form host:port. If not
// explicitly configured, it is assumed that the Kafka cluster metadata is present in the ensemble root path. If the
// cluster name is unknown, or if the server list is missing or invalid, this func will panic.
func (module *KafkaZkClient) Configure(name string, configRoot string) {
	module.Log.Info("configuring")

	module.name = name
	module.running = &sync.WaitGroup{}
	module.groupLock = &sync.RWMutex{}
	module.groupList = make(map[string]*topicList)
	module.connectFunc = helpers.ZookeeperConnect

	module.servers = viper.GetStringSlice(configRoot + ".servers")
	if len(module.servers) == 0 {
		panic("No Zookeeper servers specified for consumer " + module.name)
	} else if !helpers.ValidateHostList(module.servers) {
		panic("Consumer '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed, and get them
	viper.SetDefault(configRoot+".zookeeper-timeout", 30)
	module.zookeeperTimeout = viper.GetInt(configRoot + ".zookeeper-timeout")
	module.zookeeperPath = viper.GetString(configRoot+".zookeeper-path") + "/consumers"
	module.cluster = viper.GetString(configRoot + ".cluster")

	if !helpers.ValidateZookeeperPath(module.zookeeperPath) {
		panic("Consumer '" + name + "' has a bad zookeeper path configuration")
	}

	whitelist := viper.GetString(configRoot + ".group-whitelist")
	if whitelist != "" {
		re, err := regexp.Compile(whitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}

	blacklist := viper.GetString(configRoot + ".group-blacklist")
	if blacklist != "" {
		re, err := regexp.Compile(blacklist)
		if err != nil {
			module.Log.Panic("Failed to compile group blacklist")
			panic(err)
		}
		module.groupBlacklist = re
	}
}

// Start connects to the Zookeeper ensemble configured. Any error connecting to the cluster is returned to the caller.
// Once the client is set up, the consumer group list is enumerated and watches are set up for each group, topic,
// partition, and offset. A goroutine is also started to monitor the Zookeeper connection state, and reset the watches
// in the case the the session expires.
func (module *KafkaZkClient) Start() error {
	module.Log.Info("starting")

	zkconn, connEventChan, err := module.connectFunc(module.servers, time.Duration(module.zookeeperTimeout)*time.Second, module.Log)
	if err != nil {
		return err
	}
	module.zk = zkconn

	// Set up all groups initially (we can't count on catching the first CONNECTED event
	module.running.Add(1)
	module.resetGroupListWatchAndAdd(false)
	module.areWatchesSet = true

	// Start up a func to watch for connection state changes and reset all the watches when needed
	module.running.Add(1)
	go module.connectionStateWatcher(connEventChan)

	return nil
}

// Stop closes the Zookeeper client.
func (module *KafkaZkClient) Stop() error {
	module.Log.Info("stopping")

	// Closing the ZK client will invalidate all the watches, which will close all the running goroutines
	module.zk.Close()
	module.running.Wait()

	return nil
}

func (module *KafkaZkClient) connectionStateWatcher(eventChan <-chan zk.Event) {
	defer module.running.Done()
	for event := range eventChan {
		if event.Type == zk.EventSession {
			switch event.State {
			case zk.StateExpired:
				module.Log.Error("session expired")
				module.areWatchesSet = false
			case zk.StateConnected:
				if !module.areWatchesSet {
					module.Log.Info("reinitializing watches")
					module.groupLock.Lock()
					module.groupList = make(map[string]*topicList)
					module.groupLock.Unlock()

					module.running.Add(1)
					go module.resetGroupListWatchAndAdd(false)
				}
			}
		}
	}
}

func (module *KafkaZkClient) acceptConsumerGroup(group string) bool {
	if (module.groupWhitelist != nil) && (!module.groupWhitelist.MatchString(group)) {
		return false
	}
	if (module.groupBlacklist != nil) && module.groupBlacklist.MatchString(group) {
		return false
	}
	return true
}

// This is a simple goroutine that will wait for an event on a watch channnel and then exit. It's here so that when
// we set a watch that we don't care about (from an ExistsW on a node that already exists), we can drain it properly.
func drainEventChannel(eventChan <-chan zk.Event) {
	<-eventChan
}

func (module *KafkaZkClient) waitForNodeToExist(zkPath string, Logger *zap.Logger) bool {
	nodeExists, _, existsWatchChan, err := module.zk.ExistsW(zkPath)
	if err != nil {
		// This is a real error (since NoNode will not return an error)
		Logger.Debug("failed to check existence of znode",
			zap.String("path", zkPath),
			zap.String("error", err.Error()),
		)
		return false
	}
	if nodeExists {
		// The node already exists, just drain the data watch that got created whenever it fires
		go drainEventChannel(existsWatchChan)
		return true
	}

	// Wait for the node to exist
	Logger.Debug("waiting for node to exist", zap.String("path", zkPath))
	event := <-existsWatchChan
	if event.Type == zk.EventNotWatching {
		// Watch is gone, so we're gone too
		Logger.Debug("exists watch invalidated",
			zap.String("path", zkPath),
		)
		return false
	}
	return true
}

func (module *KafkaZkClient) watchGroupList(eventChan <-chan zk.Event) {
	defer module.running.Done()

	event := <-eventChan
	if event.Type == zk.EventNotWatching {
		// We're done here
		module.Log.Debug("group list watch invalidated")
		return
	}
	module.Log.Debug("group list watch fired", zap.Int("event_type", int(event.Type)))
	module.running.Add(1)
	go module.resetGroupListWatchAndAdd(event.Type != zk.EventNodeChildrenChanged)
}

func (module *KafkaZkClient) resetGroupListWatchAndAdd(resetOnly bool) {
	defer module.running.Done()

	// Get the current group list and reset our watch
	consumerGroups, _, groupListEventChan, err := module.zk.ChildrenW(module.zookeeperPath)
	if err != nil {
		// Can't read the consumers path. Bail for now
		module.Log.Error("failed to list groups", zap.String("error", err.Error()))
		return
	}
	module.running.Add(1)
	go module.watchGroupList(groupListEventChan)

	if !resetOnly {
		// Check for any new groups and create the watches for them
		module.groupLock.Lock()
		defer module.groupLock.Unlock()
		for _, group := range consumerGroups {
			if !module.acceptConsumerGroup(group) {
				module.Log.Debug("skip group",
					zap.String("group", group),
					zap.String("reason", "whitelist"),
				)
				continue
			}

			if module.groupList[group] == nil {
				module.groupList[group] = &topicList{
					topics: make(map[string]*partitionCount),
					lock:   &sync.RWMutex{},
				}
				module.Log.Debug("add group",
					zap.String("group", group),
				)
				module.running.Add(1)
				go module.resetTopicListWatchAndAdd(group, false)
			}
		}
	}
}

func (module *KafkaZkClient) watchTopicList(group string, eventChan <-chan zk.Event) {
	defer module.running.Done()

	event := <-eventChan
	if event.Type == zk.EventNotWatching {
		// We're done here
		module.Log.Debug("topic list watch invalidated", zap.String("group", group))
		return
	}
	module.Log.Debug("topic list watch fired",
		zap.String("group", group),
		zap.Int("event_type", int(event.Type)),
	)
	module.running.Add(1)
	go module.resetTopicListWatchAndAdd(group, event.Type != zk.EventNodeChildrenChanged)
}

func (module *KafkaZkClient) resetTopicListWatchAndAdd(group string, resetOnly bool) {
	defer module.running.Done()

	// Wait for the offsets znode for this group to exist. We need to do this because the previous child watch
	// fires on /consumers/(group) existing, but here we try to read /consumers/(group)/offsets (which might not exist
	// yet)
	zkPath := module.zookeeperPath + "/" + group + "/offsets"
	Logger := module.Log.With(zap.String("group", group))
	if !module.waitForNodeToExist(zkPath, Logger) {
		// There was an error checking node existence, so we can't continue
		return
	}

	// Get the current group topic list and reset our watch
	groupTopics, _, topicListEventChan, err := module.zk.ChildrenW(zkPath)
	if err != nil {
		Logger.Debug("failed to read topic list", zap.String("error", err.Error()))
		return
	}
	module.running.Add(1)
	go module.watchTopicList(group, topicListEventChan)

	if !resetOnly {
		// Check for any new topics and create the watches for them
		module.groupLock.RLock()
		defer module.groupLock.RUnlock()

		module.groupList[group].lock.Lock()
		defer module.groupList[group].lock.Unlock()
		for _, topic := range groupTopics {
			if module.groupList[group].topics[topic] == nil {
				module.groupList[group].topics[topic] = &partitionCount{
					count: 0,
					lock:  &sync.Mutex{},
				}
				Logger.Debug("add topic", zap.String("topic", topic))
				module.running.Add(1)
				go module.resetPartitionListWatchAndAdd(group, topic, false)
			}
		}
	}
}

func (module *KafkaZkClient) watchPartitionList(group string, topic string, eventChan <-chan zk.Event) {
	defer module.running.Done()

	event := <-eventChan
	if event.Type == zk.EventNotWatching {
		// We're done here
		module.Log.Debug("partition list watch invalidated",
			zap.String("group", group),
			zap.String("topic", topic),
		)
		return
	}
	module.Log.Debug("partition list watch fired",
		zap.String("group", group),
		zap.String("topic", topic),
		zap.Int("event_type", int(event.Type)),
	)
	module.running.Add(1)
	go module.resetPartitionListWatchAndAdd(group, topic, event.Type != zk.EventNodeChildrenChanged)
}

func (module *KafkaZkClient) resetPartitionListWatchAndAdd(group string, topic string, resetOnly bool) {
	defer module.running.Done()

	// Get the current topic partition list and reset our watch
	topicPartitions, _, partitionListEventChan, err := module.zk.ChildrenW(module.zookeeperPath + "/" + group + "/offsets/" + topic)
	if err != nil {
		// Can't read the partition list path. Bail for now
		module.Log.Warn("failed to read partitions",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.String("error", err.Error()),
		)
		return
	}
	module.running.Add(1)
	go module.watchPartitionList(group, topic, partitionListEventChan)

	if !resetOnly {
		// Check for any new partitions and create the watches for them
		module.groupLock.RLock()
		defer module.groupLock.RUnlock()

		module.groupList[group].lock.RLock()
		defer module.groupList[group].lock.RUnlock()

		module.groupList[group].topics[topic].lock.Lock()
		defer module.groupList[group].topics[topic].lock.Unlock()
		if int32(len(topicPartitions)) >= module.groupList[group].topics[topic].count {
			for i := module.groupList[group].topics[topic].count; i < int32(len(topicPartitions)); i++ {
				module.Log.Debug("add partition",
					zap.String("group", group),
					zap.String("topic", topic),
					zap.Int32("partition", i),
				)
				module.running.Add(1)
				module.resetOffsetWatchAndSend(group, topic, i, false)
			}
			module.groupList[group].topics[topic].count = int32(len(topicPartitions))
		}
	}
}

func (module *KafkaZkClient) watchOffset(group string, topic string, partition int32, eventChan <-chan zk.Event) {
	defer module.running.Done()

	event := <-eventChan
	if event.Type == zk.EventNotWatching {
		// We're done here
		module.Log.Debug("offset watch invalidated",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
		)
		return
	}
	module.Log.Debug("offset watch fired",
		zap.String("group", group),
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int("event_type", int(event.Type)),
	)
	module.running.Add(1)
	go module.resetOffsetWatchAndSend(group, topic, partition, event.Type != zk.EventNodeDataChanged)
}

func (module *KafkaZkClient) resetOffsetWatchAndSend(group string, topic string, partition int32, resetOnly bool) {
	defer module.running.Done()

	// Get the current offset and reset our watch
	offsetString, offsetStat, offsetEventChan, err := module.zk.GetW(module.zookeeperPath + "/" + group + "/offsets/" + topic + "/" + strconv.FormatInt(int64(partition), 10))

	// Get the current owner of the partition
	consumerID, _, _, _ := module.zk.GetW(module.zookeeperPath + "/" + group + "/owners/" + topic + "/" + strconv.FormatInt(int64(partition), 10))

	if err != nil {
		// Can't read the partition offset path. Bail for now
		module.Log.Warn("failed to read offset",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.String("error", err.Error()),
		)
		return
	}
	module.running.Add(1)
	go module.watchOffset(group, topic, partition, offsetEventChan)

	if !resetOnly {
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
			Cluster:     module.cluster,
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

		// Send the owner of the current partition to the storage module
		helpers.TimeoutSendStorageRequest(module.App.StorageChannel, &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOwner,
			Cluster:     module.cluster,
			Topic:       topic,
			Partition:   int32(partition),
			Group:       group,
			Owner:       string(consumerID),
		}, 1)
	}
}
