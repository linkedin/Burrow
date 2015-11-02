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
	"net"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"
)

type HttpNotifier struct {
	app            *ApplicationContext
	templatePost   *template.Template
	templateDelete *template.Template
	extras         map[string]string
	ticker         *time.Ticker
	quitChan       chan struct{}
	groupIds       map[string]map[string]Event
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
		"jsonencoder":    templateJsonEncoder,
		"topicsbystatus": classifyTopicsByStatus,
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
		resultsChannel: make(chan *ConsumerGroupStatus),
		httpClient: &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   time.Duration(app.Config.Httpnotifier.Timeout) * time.Second,
					KeepAlive: time.Duration(app.Config.Httpnotifier.Keepalive) * time.Second,
				}).Dial,
				TLSHandshakeTimeout: time.Duration(app.Config.Httpnotifier.Timeout) * time.Second,
			},
		},
	}, nil
}

func (notifier *HttpNotifier) sendEvaluationRequests() {
	for cluster, _ := range notifier.app.Config.Kafka {
		// Get a current list of consumer groups
		storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
		notifier.app.Storage.requestChannel <- storageRequest
		groups := <-storageRequest.Result

		// Send requests for group status
		for _, group := range groups {
			storageRequest := &RequestConsumerStatus{Result: notifier.resultsChannel, Cluster: cluster, Group: group}
			notifier.app.Storage.requestChannel <- storageRequest
		}
	}
}

func (notifier *HttpNotifier) handleEvaluationResponse(result *ConsumerGroupStatus) {
	if result.Status >= StatusWarning {
		if _, ok := notifier.groupIds[result.Cluster]; !ok {
			// Create the cluster map
			notifier.groupIds[result.Cluster] = make(map[string]Event)
		}
		if _, ok := notifier.groupIds[result.Cluster][result.Group]; !ok {
			// Create Event and Id
			eventId := uuid.NewRandom()
			notifier.groupIds[result.Cluster][result.Group] = Event{
				Id:    eventId.String(),
				Start: time.Now(),
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
			Id:         notifier.groupIds[result.Cluster][result.Group].Id,
			Start:      notifier.groupIds[result.Cluster][result.Group].Start,
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
			log.Errorf("Failed to send POST (Id %s): %v", notifier.groupIds[result.Cluster][result.Group].Id, err)
			return
		}
		defer resp.Body.Close()

		if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
			log.Infof("Sent POST for group %s in cluster %s at severity %v (Id %s)", result.Group,
				result.Cluster, result.Status, notifier.groupIds[result.Cluster][result.Group].Id)
		} else {
			log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %s", result.Group,
				result.Cluster, result.Status, notifier.groupIds[result.Cluster][result.Group].Id, resp.Status)
		}
	} else {
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
				log.Errorf("Failed to assemble DELETE: %v", err)
				return
			}

			req, err := http.NewRequest("DELETE", notifier.app.Config.Httpnotifier.Url, bytesToSend)
			req.Header.Set("Content-Type", "application/json")

			resp, err := notifier.httpClient.Do(req)
			if err != nil {
				log.Errorf("Failed to send DELETE: %v", err)
				return
			}
			defer resp.Body.Close()

			if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
				log.Infof("Sent DELETE for group %s in cluster %s (Id %s)", result.Group, result.Cluster,
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

func (notifier *HttpNotifier) Start() {
	// Start the ticker
	notifier.ticker = time.NewTicker(time.Duration(notifier.app.Config.Httpnotifier.Interval) * time.Second)

	go func() {
	OUTERLOOP:
		for {
			select {
			case <-notifier.quitChan:
				notifier.ticker.Stop()
				break OUTERLOOP
			case <-notifier.ticker.C:
				go notifier.sendEvaluationRequests()
			case result := <-notifier.resultsChannel:
				go notifier.handleEvaluationResponse(result)
			}
		}
	}()
}

func (notifier *HttpNotifier) Stop() {
	close(notifier.quitChan)
}
