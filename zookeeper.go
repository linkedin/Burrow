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
	"github.com/samuel/go-zookeeper/zk"
	"strconv"
	"time"
)

type ZookeeperClient struct {
	app            *ApplicationContext
	cluster        string
	conn           *zk.Conn
	zkOffsetTicker *time.Ticker
}

func NewZookeeperClient(app *ApplicationContext, cluster string) (*ZookeeperClient, error) {
	zkconn, _, err := zk.Connect(app.Config.Kafka[cluster].Zookeepers, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &ZookeeperClient{
		app:     app,
		cluster: cluster,
		conn:    zkconn,
	}

	// Check if this cluster is configured to check Zookeeper consumer offsets
	if client.app.Config.Kafka[cluster].ZKOffsets {
		// Get the first set of offsets and start a goroutine to continually check them
		client.getOffsets()
		client.zkOffsetTicker = time.NewTicker(time.Duration(client.app.Config.Lagcheck.ZKCheck) * time.Second)
		go func() {
			for _ = range client.zkOffsetTicker.C {
				client.getOffsets()
			}
		}()
	}

	return client, nil
}

func (zkClient *ZookeeperClient) Stop() {
	if zkClient.zkOffsetTicker != nil {
		zkClient.zkOffsetTicker.Stop()
	}

	zkClient.conn.Close()
}

func (zkClient *ZookeeperClient) getOffsets() {
	log.Debugf("Get ZK consumer offsets for cluster %s", zkClient.cluster)

	consumerGroups, _, err := zkClient.conn.Children(zkClient.app.Config.Kafka[zkClient.cluster].ZookeeperPath + "/consumers")
	if err != nil {
		// Can't read the consumers path. Bail for now
		log.Errorf("Cannot get consumer group list for cluster %s: %s", zkClient.cluster, err)
		return
	}

	// Spawn a goroutine to handle each consumer group
	for _, consumerGroup := range consumerGroups {
		go zkClient.getOffsetsForConsumerGroup(consumerGroup)
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

	partitionOffset := &PartitionOffset{
		Cluster:   zkClient.cluster,
		Topic:     topic,
		Partition: int32(partitionNum),
		Group:     consumerGroup,
		Timestamp: int64(zkNodeStat.Mtime), // note: this is millis
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
