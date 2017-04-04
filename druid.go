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
	"strconv"
	"sync"
	"time"
	"net/http"
	"io/ioutil"
	"fmt"
)

type DruidClient struct {
	app                *ApplicationContext
	cluster            string
	conn               *zk.Conn
	druidRefreshTicker *time.Ticker
	druidGroupList     map[string]bool
	druidGroupLock     sync.RWMutex
}

type DruidState struct {
	Offset    int
	Partition int
	Topic     string
}

func NewDruidClient(app *ApplicationContext, cluster string) (*DruidClient, error) {
	// here we share the timeout w/ global zk
	zkconn, _, err := zk.Connect(app.Config.Druid[cluster].Zookeepers, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &DruidClient{
		app:            app,
		cluster:        cluster,
		conn:           zkconn,
		druidGroupLock: sync.RWMutex{},
		druidGroupList: make(map[string]bool),
	}

	// Now get the first set of offsets and start a goroutine to continually check them
	client.refreshConsumerGroups()
	client.druidRefreshTicker = time.NewTicker(time.Duration(client.app.Config.Lagcheck.DruidGroupRefresh) * time.Second)
	go func() {
		for _ = range client.druidRefreshTicker.C {
			client.refreshConsumerGroups()
		}
	}()

	return client, nil
}

func (druidClient *DruidClient) Stop() {

	if druidClient.druidRefreshTicker != nil {
		druidClient.druidRefreshTicker.Stop()
		druidClient.druidGroupLock.Lock()
		druidClient.druidGroupList = make(map[string]bool)
		druidClient.druidGroupLock.Unlock()
	}

	druidClient.conn.Close()
}

func (druidClient *DruidClient) getSupervisors() ([]string, error) {
	//get supervisors from overlord
	log.Infof("supervisor url: http://"+druidClient.app.Config.Druid[druidClient.cluster].DruidOverlord+"/druid/indexer/v1/supervisor")
	response, err := http.Get("http://"+druidClient.app.Config.Druid[druidClient.cluster].DruidOverlord+"/druid/indexer/v1/supervisor")

	if err != nil {
		log.Errorf("Error in getting supervisors from Druid API: %s", err.Error())
		return nil, err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)	
	if err != nil {
		log.Errorf("Error in reading response body: %v", err)
		return nil, err
	}
	
	supervisors := make([]string, 0)
	parse_err := json.Unmarshal(body, &supervisors)
	if parse_err != nil {
		log.Errorf("Error in parsing json string: %s", parse_err.Error())
		return nil, parse_err
	}
	log.Infof("supervisors: %v", supervisors)

	return supervisors, err
}

func (druidClient *DruidClient) getSupervisorOffset(supervisorId string) (string, map[string]interface{}, string, error) {
	//get status from overlord
	response, err := http.Get("http://"+druidClient.app.Config.Druid[druidClient.cluster].DruidOverlord+"/druid/indexer/v1/supervisor/"+supervisorId+"/status")
	if err != nil {
		log.Errorf("Error in getting offset status from Druid API: %s"+err.Error())
		return "", nil, "", err
	}
	defer response.Body.Close()

	status, err := ioutil.ReadAll(response.Body)	
	if err != nil {
		log.Errorf("Error in reading response body: %v", err)
		return "", nil, "", err
	}
	
	data := make(map[string]interface{})
	data_err := json.Unmarshal(status, &data)
	if data_err != nil {
		log.Errorf("Error in parsing json: %v", err)
		return "", nil, "", data_err
	}

	//extract current offset
	payload := data["payload"].(map[string]interface{})
	activeTasks := payload["activeTasks"].([]interface{})
	if len(activeTasks) == 0 {
		err = errors.New(fmt.Sprintf("No active tasks for topic \"%s\", status: %v \n", payload["topic"].(string), string(status[:])))
		return "", nil, "", err 
	}
	
	//TODO: should we send all active tasks for a single topic?
	activeTask := activeTasks[0]
	currentOffsets := activeTask.(map[string]interface{})["currentOffsets"].(map[string]interface{})
	if len(currentOffsets) == 0 && len(activeTasks) > 1 {
		activeTask = activeTasks[1]
		currentOffsets = activeTask.(map[string]interface{})["currentOffsets"].(map[string]interface{})
	}
	startTime := activeTask.(map[string]interface{})["startTime"]
	
	if len(currentOffsets)==0 || startTime == nil {
		err = errors.New(fmt.Sprintf("Current offset is empty or start time is null for topic \"%s\", status: %v \n", payload["topic"].(string), string(status[:])))
		return "", nil, "", err
	}
		
	return payload["topic"].(string), currentOffsets, startTime.(string), err
}

func (druidClient *DruidClient) getOffsetsForConsumerGroupFromDruidAPI(consumerGroup string) {
	topic, curOffset, timestamp, err := druidClient.getSupervisorOffset(consumerGroup)
	if err != nil {
		log.Warnf("Failed to get offset for consumerGroup %s. Skip this time. Message: %v",consumerGroup, err)
		return;
	}

	for partition, offset := range curOffset {
		//type conversion
		p, p_err := strconv.ParseInt(partition, 10, 32)
		o := int(offset.(float64))
		layout := "2006-01-02T15:04:05.000Z"
		t, t_err := time.Parse(layout, timestamp)
		if p_err !=nil || t_err != nil {
			log.Errorf("Error in type conversion. Skip topic=%s, partition=%s, timestamp=%s. Error: %v", topic, partition, timestamp, err)
		} else {
			partitionOffset := &protocol.PartitionOffset{
				Cluster:   druidClient.cluster,
				Topic:     topic,
				Partition: int32(p),
				Group:     consumerGroup,
				Timestamp: int64(t.Unix()*1000), // note: this is millis
				Offset:    int64(o),
			}
			timeoutSendOffset(druidClient.app.Storage.offsetChannel, partitionOffset, 1)
			log.Debugf("Sync Druid offset: [%s,%s,%v]::[%v,%v]\n", partitionOffset.Group, partitionOffset.Topic, partitionOffset.Partition, partitionOffset.Offset, partitionOffset.Timestamp)
		}			
	}
}

func (druidClient *DruidClient) refreshConsumerGroups() {
	druidClient.druidGroupLock.Lock()
	defer druidClient.druidGroupLock.Unlock()

	// Mark all existing groups false
	for consumerGroup := range druidClient.druidGroupList {
		druidClient.druidGroupList[consumerGroup] = false
	}

	consumerGroups, err := druidClient.getSupervisors()
	if err != nil {
		// Can't read the consumers path. Bail for now
		log.Errorf("Cannot get Druid Kafka consumer group list for cluster %s: %s", druidClient.cluster, err)
		return
	}

	// Check for new groups, mark existing groups true
	for _, consumerGroup := range consumerGroups {
		// Ignore groups that are out of filter bounds
		if !druidClient.app.Storage.AcceptConsumerGroup(consumerGroup) {
			continue
		}

		if _, ok := druidClient.druidGroupList[consumerGroup]; !ok {
			// Add new consumer group and start it
			log.Debugf("Add Druid Kafka consumer group %s to cluster %s", consumerGroup, druidClient.cluster)
			go druidClient.startConsumerGroupChecker(consumerGroup)
		}
		druidClient.druidGroupList[consumerGroup] = true
	}

	// Delete groups that are still false
	for consumerGroup := range druidClient.druidGroupList {
		if !druidClient.druidGroupList[consumerGroup] {
			log.Debugf("Remove Druid Kafka consumer group %s from cluster %s", consumerGroup, druidClient.cluster)
			delete(druidClient.druidGroupList, consumerGroup)
		}
	}
}

func (druidClient *DruidClient) startConsumerGroupChecker(consumerGroup string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(druidClient.app.Config.Lagcheck.DruidCheck*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		druidClient.druidGroupLock.RLock()
		if _, ok := druidClient.druidGroupList[consumerGroup]; !ok {
			druidClient.druidGroupLock.RUnlock()
			log.Debugf("Stopping checker for Druid Kafka consumer group %s in cluster %s", consumerGroup, druidClient.cluster)
			break
		}
		druidClient.druidGroupLock.RUnlock()

		// Check this group's offsets
		log.Debugf("Get Druid Kafka offsets for group %s in cluster %s", consumerGroup, druidClient.cluster)
		go druidClient.getOffsetsForConsumerGroupFromDruidAPI(consumerGroup)

		// Sleep for the check interval
		time.Sleep(time.Duration(druidClient.app.Config.Lagcheck.DruidCheck) * time.Second)
	}
}
