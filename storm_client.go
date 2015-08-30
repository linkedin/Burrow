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
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/cihub/seelog"
	"regexp"
	"strconv"
	"time"
	"errors"
	"encoding/json"
)

type StormOffsetClient struct {
	app             	*ApplicationContext
	cluster            	string
	conn    			*zk.Conn
	stormOffsetTicker 	*time.Ticker
}

type Topology struct {
	Id    			string
	Name 			string
}

type Broker struct {
	Host    		string
	Port 			int
}

type SpoutState struct {
	Topology    	Topology
	Offset 			int
	Partition 		int
	Broker 			Broker
	Topic 			string
}

// Monitor offsets stored inside ZK by Storm Kafka Spout.
// Storm Kafka Spout stores offsets and other meta data in following ZK node
//   /<SpoutConfig.zkRoot>/<SpoutConfig.id>/partition_<partitionId>
// the zk node contains meta data, including the offset, encoded in json. Below is an example
// {
//    "broker": {
//        "host": "kafka.sample.net",
//        "port": 9092
//    },
//    "offset": 4285,
//    "partition": 1,
//    "topic": "testTopic",
//    "topology": {
//        "id": "fce905ff-25e0 -409e-bc3a-d855f 787d13b",
//        "name": "Test Topology"
//    }
//  }
func NewStormOffsetClient(app *ApplicationContext, cluster string) (*StormOffsetClient, error) {
	zkhosts := make([]string, len(app.Config.Kafka[cluster].Zookeepers))
	for i, host := range app.Config.Kafka[cluster].Zookeepers {
		zkhosts[i] = fmt.Sprintf("%s:%v", host, app.Config.Kafka[cluster].ZookeeperPort)
	}
	zkconn, _, err := zk.Connect(zkhosts, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &StormOffsetClient{
		app:     app,
		cluster: cluster,
		conn:    zkconn,
	}

	// Now get the first set of offsets and start a goroutine to continually check them
	client.getOffsets(client.app.Config.Kafka[cluster].StormOffsetPaths)
	client.stormOffsetTicker = time.NewTicker(time.Duration(client.app.Config.Tickers.StormOffsets) * time.Second)
	go func() {
		for _ = range client.stormOffsetTicker.C {
			client.getOffsets(client.app.Config.Kafka[cluster].StormOffsetPaths)
		}
	}()

	return client, nil
}

func parsePartitionId(partitionStr string)(int, error) {
	re := regexp.MustCompile(`^partition_([0-9]+)$`)
	if parsed := re.FindStringSubmatch(partitionStr); len(parsed) == 2 {
		return strconv.Atoi(parsed[1])
	} else {
		return -1, errors.New("Invalid partition id: " + partitionStr)
	}
}

func parseStormSpoutStateJson(stateStr string)(int, string, error) {
	stateJson := new(SpoutState)
	if err := json.Unmarshal([]byte(stateStr), &stateJson); err == nil {
		log.Debugf("Parsed storm state [%s] to structure [%v]", stateStr, stateJson)
		return stateJson.Offset, stateJson.Topic, nil
	} else {
		return -1, "", err
	}
}

func (stormOffsetClient *StormOffsetClient) getOffsets(paths []string) {
	log.Debugf("Start to refresh Storm offsets stored in paths: %s", paths)

	for _, path := range paths {
		kafkaSpouts, _, err := stormOffsetClient.conn.Children(path)
		switch {
		case err == nil:
			for _, kafkaSpout := range kafkaSpouts {
				go stormOffsetClient.getOffsetsForKafkaSpout(kafkaSpout, path + "/" + kafkaSpout)
			}

		case err == zk.ErrNoNode:
			// don't tolerate mis-configuration, let's bail out
			panic("Failed to fetch spouts in ZK path: " + path)

		default:
			// if we cannot even read the top level directory to get the list of all spouts, let's bail out
			panic(err)
		}
	}
}

func (stormOffsetClient *StormOffsetClient) getOffsetsForKafkaSpout(kafkaSpout string, kafkaSpoutPath string) {

	partition_ids, _, err := stormOffsetClient.conn.Children(kafkaSpoutPath)
	switch {
	case err == nil:
		for _, partition_id := range partition_ids {
			partition, errConversion := parsePartitionId(partition_id)
			switch {
			case errConversion == nil:
				stormOffsetClient.getOffsetsForPartition(kafkaSpout, partition, kafkaSpoutPath + "/" + partition_id)

			default:
				log.Errorf("Something is very wrong! The partition id %s in storm spout %s in ZK path %s should be a number",
					partition_id, kafkaSpout, kafkaSpoutPath)
			}
		}

	default:
		log.Warnf("Failed to read partitions for kafka spout %s in ZK path %s. Error: %v", kafkaSpout, kafkaSpoutPath, err)
	}
}

func (stormOffsetClient *StormOffsetClient) getOffsetsForPartition(kafkaSpout string, partition int, partitionPath string) {
	zkNodeStat := &zk.Stat {}

	stateStr, zkNodeStat, err := stormOffsetClient.conn.Get(partitionPath)
	switch {
	case err == nil:
		offset, topic, errConversion := parseStormSpoutStateJson(string(stateStr))
		switch {
		case errConversion == nil:
			log.Debugf("About to sync Storm offset: [%s,%s,%v]::[%v,%v]\n", kafkaSpout, topic, partition, offset, zkNodeStat.Mtime)
			partitionOffset := &PartitionOffset{
				Cluster:   stormOffsetClient.cluster,
				Topic:     topic,
				Partition: int32(partition),
				Group:     kafkaSpout,
				Timestamp: int64(zkNodeStat.Mtime), // note: this is millis
				Offset:    int64(offset),
			}
			timeoutSendOffset(stormOffsetClient.app.Storage.offsetChannel, partitionOffset, 1)

		default:
			log.Errorf("Something is very wrong! Cannot parse state json for partition %v in storm spout %s in ZK path %s: %s. Error: %v",
				partition, kafkaSpout, partitionPath, stateStr, errConversion)
		}

	default:
		log.Warnf("Failed to read data for partition %v in kafka spout %s in ZK path %s. Error: %v", partition, kafkaSpout, partitionPath, err)
	}
}

func (stormOffsetClient *StormOffsetClient) Stop() {
	stormOffsetClient.conn.Close()
}
