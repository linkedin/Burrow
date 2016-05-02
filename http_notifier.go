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
	"bytes"
	log "github.com/cihub/seelog"
	"github.com/pborman/uuid"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"
)

type HttpNotifier struct {
	app            *ApplicationContext
	templatePost   *template.Template
	templateDelete *template.Template
	extras         map[string]string
	refreshTicker  *time.Ticker
	quitChan       chan struct{}
	groupIds       map[string]map[string]Event
	groupList      map[string]map[string]bool
	groupLock      sync.RWMutex
	resultsChannel chan *ConsumerGroupStatus
	httpClient     *http.Client
}

type Event struct {
	Id    string
	Start time.Time
}

func NewHttpNotifier(app *ApplicationContext) (*HttpNotifier, error) {
	// Helper functions for templates
	fmap := template.FuncMap{
		"jsonencoder":     templateJsonEncoder,
		"topicsbystatus":  classifyTopicsByStatus,
		"partitioncounts": templateCountPartitions,
		"add":             templateAdd,
		"minus":           templateMinus,
		"multiply":        templateMultiply,
		"divide":          templateDivide,
		"maxlag":          maxLagHelper,
	}

	// Compile the templates
	templatePost, err := template.New("post").Funcs(fmap).ParseFiles(app.Config.Httpnotifier.TemplatePost)
	if err != nil {
		log.Criticalf("Cannot parse HTTP notifier POST template: %v", err)
		os.Exit(1)
	}
	templatePost = templatePost.Templates()[0]

	templateDelete, err := template.New("delete").Funcs(fmap).ParseFiles(app.Config.Httpnotifier.TemplateDelete)
	if err != nil {
		log.Criticalf("Cannot parse HTTP notifier DELETE template: %v", err)
		os.Exit(1)
	}
	templateDelete = templateDelete.Templates()[0]

	// Parse the extra parameters for the templates
	extras := make(map[string]string)
	for _, extra := range app.Config.Httpnotifier.Extras {
		parts := strings.Split(extra, "=")
		extras[parts[0]] = parts[1]
	}

	return &HttpNotifier{
		app:            app,
		templatePost:   templatePost,
		templateDelete: templateDelete,
		extras:         extras,
		quitChan:       make(chan struct{}),
		groupIds:       make(map[string]map[string]Event),
		groupList:      make(map[string]map[string]bool),
		groupLock:      sync.RWMutex{},
		resultsChannel: make(chan *ConsumerGroupStatus),
		httpClient: &http.Client{
			Timeout: time.Duration(app.Config.Httpnotifier.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(app.Config.Httpnotifier.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil
}

func (notifier *HttpNotifier) handleEvaluationResponse(result *ConsumerGroupStatus) {
	if int(result.Status) >= notifier.app.Config.Httpnotifier.PostThreshold {
		// We only use IDs if we are sending deletes
		idStr := ""
		startTime := time.Now()
		if notifier.app.Config.Httpnotifier.SendDelete {
			if _, ok := notifier.groupIds[result.Cluster]; !ok {
				// Create the cluster map
				notifier.groupIds[result.Cluster] = make(map[string]Event)
			}
			if _, ok := notifier.groupIds[result.Cluster][result.Group]; !ok {
				// Create Event and Id
				eventId := uuid.NewRandom()
				idStr = eventId.String()
				notifier.groupIds[result.Cluster][result.Group] = Event{
					Id:    idStr,
					Start: startTime,
				}
			} else {
				idStr = notifier.groupIds[result.Cluster][result.Group].Id
				startTime = notifier.groupIds[result.Cluster][result.Group].Start
			}
		}

		// NOTE - I'm leaving the JsonEncode item in here so as not to break compatibility. New helpers go in the FuncMap above
		bytesToSend := new(bytes.Buffer)
		err := notifier.templatePost.Execute(bytesToSend, struct {
			Cluster    string
			Group      string
			Id         string
			Start      time.Time
			Extras     map[string]string
			Result     *ConsumerGroupStatus
			JsonEncode func(interface{}) string
		}{
			Cluster:    result.Cluster,
			Group:      result.Group,
			Id:         idStr,
			Start:      startTime,
			Extras:     notifier.extras,
			Result:     result,
			JsonEncode: templateJsonEncoder,
		})
		if err != nil {
			log.Errorf("Failed to assemble POST: %v", err)
			return
		}

		// Send POST to HTTP endpoint
		req, err := http.NewRequest("POST", notifier.app.Config.Httpnotifier.Url, bytesToSend)
		req.Header.Set("Content-Type", "application/json")

		resp, err := notifier.httpClient.Do(req)
		if err != nil {
			log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %v", result.Group, result.Cluster, result.Status, idStr, err)
			return
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()

		if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
			log.Debugf("Sent POST for group %s in cluster %s at severity %v (Id %s)", result.Group, result.Cluster, result.Status, idStr)
		} else {
			log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %s", result.Group,
				result.Cluster, result.Status, idStr, resp.Status)
		}
	}

	if notifier.app.Config.Httpnotifier.SendDelete && (result.Status == StatusOK) {
		if _, ok := notifier.groupIds[result.Cluster][result.Group]; ok {
			// Send DELETE to HTTP endpoint
			bytesToSend := new(bytes.Buffer)
			err := notifier.templateDelete.Execute(bytesToSend, struct {
				Cluster string
				Group   string
				Id      string
				Start   time.Time
				Extras  map[string]string
			}{
				Cluster: result.Cluster,
				Group:   result.Group,
				Id:      notifier.groupIds[result.Cluster][result.Group].Id,
				Start:   notifier.groupIds[result.Cluster][result.Group].Start,
				Extras:  notifier.extras,
			})
			if err != nil {
				log.Errorf("Failed to assemble DELETE for group %s in cluster %s (Id %s): %v", result.Group,
					result.Cluster, notifier.groupIds[result.Cluster][result.Group].Id, err)
				return
			}

			req, err := http.NewRequest("DELETE", notifier.app.Config.Httpnotifier.Url, bytesToSend)
			req.Header.Set("Content-Type", "application/json")

			resp, err := notifier.httpClient.Do(req)
			if err != nil {
				log.Errorf("Failed to send DELETE for group %s in cluster %s (Id %s): %v", result.Group,
					result.Cluster, notifier.groupIds[result.Cluster][result.Group].Id, err)
				return
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()

			if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
				log.Debugf("Sent DELETE for group %s in cluster %s (Id %s)", result.Group, result.Cluster,
					notifier.groupIds[result.Cluster][result.Group].Id)
			} else {
				log.Errorf("Failed to send DELETE for group %s in cluster %s (Id %s): %s", result.Group,
					result.Cluster, notifier.groupIds[result.Cluster][result.Group].Id, resp.Status)
			}

			// Remove ID for group that is now clear
			delete(notifier.groupIds[result.Cluster], result.Group)
		}
	}
}

func (notifier *HttpNotifier) refreshConsumerGroups() {
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

		// Mark all existing groups false
		for consumerGroup := range notifier.groupList {
			clusterGroups[consumerGroup] = false
		}

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

		// Delete groups that are still false
		for consumerGroup := range clusterGroups {
			if !clusterGroups[consumerGroup] {
				log.Debugf("Remove evaluator for consumer group %s in cluster %s", consumerGroup, cluster)
				delete(clusterGroups, consumerGroup)
			}
		}
	}
}

func (notifier *HttpNotifier) startConsumerGroupEvaluator(group string, cluster string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(notifier.app.Config.Httpnotifier.Interval*1000)) * time.Millisecond)

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
		time.Sleep(time.Duration(notifier.app.Config.Httpnotifier.Interval) * time.Second)
	}
}

func (notifier *HttpNotifier) Start() {
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

func (notifier *HttpNotifier) Stop() {
	if notifier.refreshTicker != nil {
		notifier.refreshTicker.Stop()
		notifier.groupLock.Lock()
		notifier.groupList = make(map[string]map[string]bool)
		notifier.groupLock.Unlock()
	}
	close(notifier.quitChan)
}
