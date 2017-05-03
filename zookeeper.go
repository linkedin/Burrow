/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	log "github.com/cihub/seelog"
	"github.com/linkedin/Burrow/protocol"
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type ZookeeperClient struct {
	app             *ApplicationContext
	cluster         string
	conn            *zk.Conn
	zkRefreshTicker *time.Ticker
	zkGroupList     map[string]bool
	zkGroupLock     sync.RWMutex
}

func NewZookeeperClient(app *ApplicationContext, cluster string) (*ZookeeperClient, error) {
	zkconn, _, err := zk.Connect(app.Config.Kafka[cluster].Zookeepers, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &ZookeeperClient{
		app:         app,
		cluster:     cluster,
		conn:        zkconn,
		zkGroupLock: sync.RWMutex{},
		zkGroupList: make(map[string]bool),
	}

	// Check if this cluster is configured to check Zookeeper consumer offsets
	if client.app.Config.Kafka[cluster].ZKOffsets {
		// Get a group list to start with (this will start the offset checkers)
		client.refreshConsumerGroups()

		// Set a ticker to refresh the group list periodically
		client.zkRefreshTicker = time.NewTicker(time.Duration(client.app.Config.Lagcheck.ZKGroupRefresh) * time.Second)
		go func() {
			for _ = range client.zkRefreshTicker.C {
				client.refreshConsumerGroups()
			}
		}()
	}

	return client, nil
}

func (zkClient *ZookeeperClient) Stop() {
	if zkClient.zkRefreshTicker != nil {
		zkClient.zkRefreshTicker.Stop()
		zkClient.zkGroupLock.Lock()
		zkClient.zkGroupList = make(map[string]bool)
		zkClient.zkGroupLock.Unlock()
	}

	zkClient.conn.Close()
}

func (zkClient *ZookeeperClient) refreshConsumerGroups() {
	zkClient.zkGroupLock.Lock()
	defer zkClient.zkGroupLock.Unlock()

	consumerGroups, _, err := zkClient.conn.Children(zkClient.app.Config.Kafka[zkClient.cluster].ZookeeperPath + "/consumers")
	if err != nil {
		// Can't read the consumers path. Bail for now
		log.Errorf("Cannot get consumer group list for cluster %s: %s", zkClient.cluster, err)
		return
	}

	// Mark all existing groups false
	for consumerGroup := range zkClient.zkGroupList {
		zkClient.zkGroupList[consumerGroup] = false
	}

	// Check for new groups, mark existing groups true
	for _, consumerGroup := range consumerGroups {
		// Don't bother adding groups in the blacklist
		if !zkClient.app.Storage.AcceptConsumerGroup(consumerGroup) {
			continue
		}

		if _, ok := zkClient.zkGroupList[consumerGroup]; !ok {
			// Add new consumer group and start it
			log.Debugf("Add ZK consumer group %s to cluster %s", consumerGroup, zkClient.cluster)
			go zkClient.startConsumerGroupChecker(consumerGroup)
		}
		zkClient.zkGroupList[consumerGroup] = true
	}

	// Delete groups that are still false
	for consumerGroup := range zkClient.zkGroupList {
		if !zkClient.zkGroupList[consumerGroup] {
			log.Debugf("Remove ZK consumer group %s from cluster %s", consumerGroup, zkClient.cluster)
			delete(zkClient.zkGroupList, consumerGroup)
		}
	}
}

func (zkClient *ZookeeperClient) startConsumerGroupChecker(consumerGroup string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(zkClient.app.Config.Lagcheck.ZKCheck*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		zkClient.zkGroupLock.RLock()
		if _, ok := zkClient.zkGroupList[consumerGroup]; !ok {
			zkClient.zkGroupLock.RUnlock()
			log.Debugf("Stopping checker for ZK consumer group %s in cluster %s", consumerGroup, zkClient.cluster)
			break
		}
		zkClient.zkGroupLock.RUnlock()

		// Check this group's offsets
		log.Debugf("Get ZK offsets for group %s in cluster %s", consumerGroup, zkClient.cluster)
		go zkClient.getOffsetsForConsumerGroup(consumerGroup)

		// Sleep for the check interval
		time.Sleep(time.Duration(zkClient.app.Config.Lagcheck.ZKCheck) * time.Second)
	}
}

func (zkClient *ZookeeperClient) getOffsetsForConsumerGroup(consumerGroup string) {
	topics, _, err := zkClient.conn.Children(zkClient.app.Config.Kafka[zkClient.cluster].ZookeeperPath + "/consumers/" + consumerGroup + "/offsets")
	switch {
	case err == nil:
		// Spawn a goroutine for each topic. This provides parallelism for multi-topic consumers
		for _, topic := range topics {
			go zkClient.getOffsetsForTopic(consumerGroup, topic)
		}
	case err == zk.ErrNoNode:
		// If the node doesn't exist, it may be because the group is using Kafka-committed offsets. Skip it
		log.Debugf("Skip checking ZK offsets for group %s in cluster %s as the offsets path doesn't exist", consumerGroup, zkClient.cluster)
	default:
		log.Warnf("Cannot read topics for group %s in cluster %s: %s", consumerGroup, zkClient.cluster, err)
	}
}

func (zkClient *ZookeeperClient) getOffsetsForTopic(consumerGroup string, topic string) {
	partitions, _, err := zkClient.conn.Children(zkClient.app.Config.Kafka[zkClient.cluster].ZookeeperPath + "/consumers/" + consumerGroup + "/offsets/" + topic)
	if err != nil {
		log.Warnf("Cannot read partitions for topic %s for group %s in cluster %s: %s", topic, consumerGroup, zkClient.cluster, err)
		return
	}

	// Spawn a goroutine for each partition
	for _, partition := range partitions {
		go zkClient.getOffsetForPartition(consumerGroup, topic, partition)
	}
}

func (zkClient *ZookeeperClient) getOffsetForPartition(consumerGroup string, topic string, partition string) {
	offsetStr, zkNodeStat, err := zkClient.conn.Get(zkClient.app.Config.Kafka[zkClient.cluster].ZookeeperPath + "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition)
	if err != nil {
		log.Warnf("Failed to read partition %s:%v for group %s in cluster %s: %s", topic, partition, consumerGroup, zkClient.cluster, err)
		return
	}

	partitionNum, err := strconv.ParseInt(partition, 10, 32)
	if err != nil {
		log.Errorf("Partition (%s) for topic %s for group %s in cluster %s is not an integer", partition, topic, consumerGroup, zkClient.cluster)
		return
	}

	offset, err := strconv.ParseInt(string(offsetStr), 10, 64)
	if err != nil {
		log.Errorf("Offset value (%s) for partition %s:%v for group %s in cluster %s is not an integer", string(offsetStr), topic, partition, consumerGroup, zkClient.cluster)
		return
	}

	partitionOffset := &protocol.PartitionOffset{
		Cluster:   zkClient.cluster,
		Topic:     topic,
		Partition: int32(partitionNum),
		Group:     consumerGroup,
		Timestamp: zkNodeStat.Mtime,
		Offset:    offset,
	}
	timeoutSendOffset(zkClient.app.Storage.offsetChannel, partitionOffset, 1)
}

func (zkClient *ZookeeperClient) NewLock(path string) *zk.Lock {
	// Pass through to the connection NewLock, without ACLs
	return zk.NewLock(zkClient.conn, path, make([]zk.ACL, 0))
}

func (zkClient *ZookeeperClient) RecursiveDelete(path string) {
	if path == "/" {
		panic("Do not try and delete the root znode")
	}

	children, _, err := zkClient.conn.Children(path)
	switch {
	case err == nil:
		for _, child := range children {
			zkClient.RecursiveDelete(path + "/" + child)
		}
	case err == zk.ErrNoNode:
		return
	default:
		panic(err)
	}

	_, stat, err := zkClient.conn.Get(path)
	if (err != nil) && (err != zk.ErrNoNode) {
		panic(err)
	}
	err = zkClient.conn.Delete(path, stat.Version)
	if (err != nil) && (err != zk.ErrNoNode) {
		panic(err)
	}
}
