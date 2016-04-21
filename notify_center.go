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
	"os"
	"sync"
	"time"
)

type NotifyCenter struct {
	app            *ApplicationContext
	interval       int64
	notifiers      []Notifier
	refreshTicker  *time.Ticker
	quitChan       chan struct{}
	groupList      map[string]map[string]bool
	groupLock      sync.RWMutex
	resultsChannel chan *ConsumerGroupStatus
}

func LoadNotifiers(app *ApplicationContext) error {
	notifiers := []Notifier{}
	if app.Config.Httpnotifier.Enable {
		if httpNotifier, err := NewHttpNotifier(app); err == nil {
			notifiers = append(notifiers, httpNotifier)
		}
	}
	if len(app.Config.Emailnotifier) > 0 {
		if emailNotifiers, err := NewEmailNotifier(app); err == nil {
			for _, emailer := range emailNotifiers {
				notifiers = append(notifiers, emailer)
			}
		}
	}

	nc := &NotifyCenter{
		app:            app,
		notifiers:      notifiers,
		interval:       app.Config.Notify.Interval,
		quitChan:       make(chan struct{}),
		groupList:      make(map[string]map[string]bool),
		groupLock:      sync.RWMutex{},
		resultsChannel: make(chan *ConsumerGroupStatus),
	}

	app.NotifyCenter = nc
	return nil
}

func StartNotifiers(app *ApplicationContext) {
	nc := app.NotifyCenter
	// Do not proceed until we get the Zookeeper lock
	err := app.NotifierLock.Lock()
	if err != nil {
		log.Criticalf("Cannot get ZK nc lock: %v", err)
		os.Exit(1)
	}
	log.Info("Acquired Zookeeper notify lock")
	// Get a group list to start with (this will start the ncs)
	nc.refreshConsumerGroups()

	// Set a ticker to refresh the group list periodically
	nc.refreshTicker = time.NewTicker(time.Duration(nc.app.Config.Lagcheck.ZKGroupRefresh) * time.Second)

	// Main loop to handle refreshes and evaluation responses
OUTERLOOP:
	for {
		select {
		case <-nc.quitChan:
			break OUTERLOOP
		case <-nc.refreshTicker.C:
			nc.refreshConsumerGroups()
		case result := <-nc.resultsChannel:
			go nc.handleEvaluationResponse(result)
		}
	}
}

func StopNotifiers(app *ApplicationContext) {
	// Ignore errors on unlock - we're quitting anyways, and it might not be locked
	app.NotifierLock.Unlock()
	nc := app.NotifyCenter
	if nc.refreshTicker != nil {
		nc.refreshTicker.Stop()
		nc.groupLock.Lock()
		nc.groupList = make(map[string]map[string]bool)
		nc.groupLock.Unlock()
	}
	close(nc.quitChan)
	// TODO stop all ncs
}

func (nc *NotifyCenter) handleEvaluationResponse(result *ConsumerGroupStatus) {
	for _, notifier := range nc.notifiers {
		if !notifier.Ignore(result) {
			notifier.Notify(result)
		}
	}
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
				log.Infof("Start evaluating consumer group %s in cluster %s", consumerGroup, cluster)
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
