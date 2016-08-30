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
	"encoding/json"
	"errors"
	log "github.com/cihub/seelog"
	"github.com/linkedin/Burrow/protocol"
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type StormClient struct {
	app                *ApplicationContext
	cluster            string
	conn               *zk.Conn
	stormRefreshTicker *time.Ticker
	stormGroupList     map[string]bool
	stormGroupLock     sync.RWMutex
}

type Topology struct {
	Id   string
	Name string
}

type Broker struct {
	Host string
	Port int
}

type SpoutState struct {
	Topology  Topology
	Offset    int
	Partition int
	Broker    Broker
	Topic     string
}

func NewStormClient(app *ApplicationContext, cluster string) (*StormClient, error) {
	// here we share the timeout w/ global zk
	zkconn, _, err := zk.Connect(app.Config.Storm[cluster].Zookeepers, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &StormClient{
		app:            app,
		cluster:        cluster,
		conn:           zkconn,
		stormGroupLock: sync.RWMutex{},
		stormGroupList: make(map[string]bool),
	}

	// Now get the first set of offsets and start a goroutine to continually check them
	client.refreshConsumerGroups()
	client.stormRefreshTicker = time.NewTicker(time.Duration(client.app.Config.Lagcheck.StormGroupRefresh) * time.Second)
	go func() {
		for _ = range client.stormRefreshTicker.C {
			client.refreshConsumerGroups()
		}
	}()

	return client, nil
}

func (stormClient *StormClient) Stop() {

	if stormClient.stormRefreshTicker != nil {
		stormClient.stormRefreshTicker.Stop()
		stormClient.stormGroupLock.Lock()
		stormClient.stormGroupList = make(map[string]bool)
		stormClient.stormGroupLock.Unlock()
	}

	stormClient.conn.Close()
}

func parsePartitionId(partitionStr string) (int, error) {
	re := regexp.MustCompile(`^partition_([0-9]+)$`)
	if parsed := re.FindStringSubmatch(partitionStr); len(parsed) == 2 {
		return strconv.Atoi(parsed[1])
	} else {
		return -1, errors.New("Invalid partition id: " + partitionStr)
	}
}

func parseStormSpoutStateJson(stateStr string) (int, string, error) {
	stateJson := new(SpoutState)
	if err := json.Unmarshal([]byte(stateStr), &stateJson); err == nil {
		log.Debugf("Parsed storm state [%s] to structure [%v]", stateStr, stateJson)
		return stateJson.Offset, stateJson.Topic, nil
	} else {
		return -1, "", err
	}
}

func (stormClient *StormClient) getConsumerGroupPath(consumerGroup string) string {
	if "/" == stormClient.app.Config.Storm[stormClient.cluster].ZookeeperPath {
		return "/" + consumerGroup
	} else {
		return stormClient.app.Config.Storm[stormClient.cluster].ZookeeperPath + "/" + consumerGroup
	}
}

func (stormClient *StormClient) getOffsetsForConsumerGroup(consumerGroup string) {
	consumerGroupPath := stormClient.getConsumerGroupPath(consumerGroup)
	partition_ids, _, err := stormClient.conn.Children(consumerGroupPath)
	switch {
	case err == nil:
		for _, partition_id := range partition_ids {
			partition, errConversion := parsePartitionId(partition_id)
			switch {
			case errConversion == nil:
				stormClient.getOffsetsForPartition(consumerGroup, partition, consumerGroupPath+"/"+partition_id)
			default:
				log.Errorf("Something is very wrong! The partition id %s of consumer group %s in ZK path %s should be a number",
					partition_id, consumerGroup, consumerGroupPath)
			}
		}
	case err == zk.ErrNoNode:
		// it is OK as the offsets may not be managed by ZK
		log.Debugf("This consumer group's offset is not managed by ZK: " + consumerGroup)
	default:
		log.Warnf("Failed to read data for consumer group %s in ZK path %s. Error: %v", consumerGroup, consumerGroupPath, err)
	}
}

func (stormClient *StormClient) getOffsetsForPartition(consumerGroup string, partition int, partitionPath string) {
	zkNodeStat := &zk.Stat{}
	stateStr, zkNodeStat, err := stormClient.conn.Get(partitionPath)
	switch {
	case err == nil:
		offset, topic, errConversion := parseStormSpoutStateJson(string(stateStr))
		switch {
		case errConversion == nil:
			log.Debugf("About to sync Storm offset: [%s,%s,%v]::[%v,%v]\n", consumerGroup, topic, partition, offset, zkNodeStat.Mtime)
			partitionOffset := &protocol.PartitionOffset{
				Cluster:   stormClient.cluster,
				Topic:     topic,
				Partition: int32(partition),
				Group:     consumerGroup,
				Timestamp: int64(zkNodeStat.Mtime), // note: this is millis
				Offset:    int64(offset),
			}
			timeoutSendOffset(stormClient.app.Storage.offsetChannel, partitionOffset, 1)
		default:
			log.Errorf("Something is very wrong! Cannot parse state json for partition %v of consumer group %s in ZK path %s: %s. Error: %v",
				partition, consumerGroup, partitionPath, stateStr, errConversion)
		}
	default:
		log.Warnf("Failed to read data for partition %v of consumer group %s in ZK path %s. Error: %v", partition, consumerGroup, partitionPath, err)
	}
}

func (stormClient *StormClient) refreshConsumerGroups() {
	stormClient.stormGroupLock.Lock()
	defer stormClient.stormGroupLock.Unlock()

	consumerGroups, _, err := stormClient.conn.Children(stormClient.app.Config.Storm[stormClient.cluster].ZookeeperPath)
	if err != nil {
		// Can't read the consumers path. Bail for now
		log.Errorf("Cannot get Storm Kafka consumer group list for cluster %s: %s", stormClient.cluster, err)
		return
	}

	// Mark all existing groups false
	for consumerGroup := range stormClient.stormGroupList {
		stormClient.stormGroupList[consumerGroup] = false
	}

	// Check for new groups, mark existing groups true
	for _, consumerGroup := range consumerGroups {
		// Ignore groups that are out of filter bounds
		if !stormClient.app.Storage.AcceptConsumerGroup(consumerGroup) {
			continue
		}

		if _, ok := stormClient.stormGroupList[consumerGroup]; !ok {
			// Add new consumer group and start it
			log.Debugf("Add Storm Kafka consumer group %s to cluster %s", consumerGroup, stormClient.cluster)
			go stormClient.startConsumerGroupChecker(consumerGroup)
		}
		stormClient.stormGroupList[consumerGroup] = true
	}

	// Delete groups that are still false
	for consumerGroup := range stormClient.stormGroupList {
		if !stormClient.stormGroupList[consumerGroup] {
			log.Debugf("Remove Storm Kafka consumer group %s from cluster %s", consumerGroup, stormClient.cluster)
			delete(stormClient.stormGroupList, consumerGroup)
		}
	}
}

func (stormClient *StormClient) startConsumerGroupChecker(consumerGroup string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(stormClient.app.Config.Lagcheck.StormCheck*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		stormClient.stormGroupLock.RLock()
		if _, ok := stormClient.stormGroupList[consumerGroup]; !ok {
			stormClient.stormGroupLock.RUnlock()
			log.Debugf("Stopping checker for Storm Kafka consumer group %s in cluster %s", consumerGroup, stormClient.cluster)
			break
		}
		stormClient.stormGroupLock.RUnlock()

		// Check this group's offsets
		log.Debugf("Get Storm Kafka offsets for group %s in cluster %s", consumerGroup, stormClient.cluster)
		go stormClient.getOffsetsForConsumerGroup(consumerGroup)

		// Sleep for the check interval
		time.Sleep(time.Duration(stormClient.app.Config.Lagcheck.StormCheck) * time.Second)
	}
}
