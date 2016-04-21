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
	"math/rand"
	"sync"
	"time"
)

type NotifyCenter struct {
	app            *ApplicationContext
	interval       int64
	refreshTicker  *time.Ticker
	quitChan       chan struct{}
	groupIds       map[string]map[string]event
	groupList      map[string]map[string]bool
	groupLock      sync.RWMutex
	resultsChannel chan *ConsumerGroupStatus
}

type event struct {
	Id    string
	Start time.Time
}

func NewNotifyCenter(app *ApplicationContext) (*NotifyCenter, error) {
	return &NotifyCenter{
		app:            app,
		quitChan:       make(chan struct{}),
		groupIds:       make(map[string]map[string]event),
		groupList:      make(map[string]map[string]bool),
		groupLock:      sync.RWMutex{},
		resultsChannel: make(chan *ConsumerGroupStatus),
	}, nil
}

func (notifier *NotifyCenter) handleEvaluationResponse(result *ConsumerGroupStatus) {
	// TODO: notify all
}

func (notifier *NotifyCenter) refreshConsumerGroups() {
	notifier.groupLock.Lock()
	defer notifier.groupLock.Unlock()

	for cluster, _ := range notifier.app.Config.Kafka {
		clusterGroups, ok := notifier.groupList[cluster]
		if !ok {
			notifier.groupList[cluster] = make(map[string]bool)
			clusterGroups = notifier.groupList[cluster]
		}

		// Get a current list of consumer groups
		storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
		notifier.app.Storage.requestChannel <- storageRequest
		consumerGroups := <-storageRequest.Result

		// Check for new groups, mark existing groups true
		for _, consumerGroup := range consumerGroups {
			// Don't bother adding groups in the blacklist
			if (notifier.app.Storage.groupBlacklist != nil) && notifier.app.Storage.groupBlacklist.MatchString(consumerGroup) {
				continue
			}

			if _, ok := clusterGroups[consumerGroup]; !ok {
				// Add new consumer group and start checking it
				log.Debugf("Start evaluating consumer group %s in cluster %s", consumerGroup, cluster)
				go notifier.startConsumerGroupEvaluator(consumerGroup, cluster)
			}
			clusterGroups[consumerGroup] = true
		}

		// Delete groups that are false
		for consumerGroup := range clusterGroups {
			if !clusterGroups[consumerGroup] {
				log.Debugf("Remove evaluator for consumer group %s in cluster %s", consumerGroup, cluster)
				delete(clusterGroups, consumerGroup)
			}
		}
	}
}

func (notifier *NotifyCenter) startConsumerGroupEvaluator(group string, cluster string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(notifier.interval*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		notifier.groupLock.RLock()
		if _, ok := notifier.groupList[cluster][group]; !ok {
			notifier.groupLock.RUnlock()
			log.Debugf("Stopping evaluator for consumer group %s in cluster %s", group, cluster)
			break
		}
		notifier.groupLock.RUnlock()

		// Send requests for group status - responses are handled by the main loop (for now)
		storageRequest := &RequestConsumerStatus{Result: notifier.resultsChannel, Cluster: cluster, Group: group}
		notifier.app.Storage.requestChannel <- storageRequest

		// Sleep for the check interval
		time.Sleep(time.Duration(notifier.interval) * time.Second)
	}
}

func (notifier *NotifyCenter) Start() {
	// Get a group list to start with (this will start the notifiers)
	notifier.refreshConsumerGroups()

	// Set a ticker to refresh the group list periodically
	notifier.refreshTicker = time.NewTicker(time.Duration(notifier.app.Config.Lagcheck.ZKGroupRefresh) * time.Second)

	// Main loop to handle refreshes and evaluation responses
	go func() {
	OUTERLOOP:
		for {
			select {
			case <-notifier.quitChan:
				break OUTERLOOP
			case <-notifier.refreshTicker.C:
				notifier.refreshConsumerGroups()
			case result := <-notifier.resultsChannel:
				go notifier.handleEvaluationResponse(result)
			}
		}
	}()
}

func (notifier *NotifyCenter) Stop() {
	if notifier.refreshTicker != nil {
		notifier.refreshTicker.Stop()
		notifier.groupLock.Lock()
		notifier.groupList = make(map[string]map[string]bool)
		notifier.groupLock.Unlock()
	}
	close(notifier.quitChan)
}
