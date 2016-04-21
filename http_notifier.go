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
	"net"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"
)

type HttpNotifier struct {
	url            string
	templatePost   *template.Template
	templateDelete *template.Template
	threshold      int
	sendDelete     bool
	extras         map[string]string
	groupIds       map[string]map[string]Event
	httpClient     *http.Client
}

type Event struct {
	Id    string
	Start time.Time
}

func (notify *HttpNotifier) NotifierName() string {
	return "http-notify"
}

func (notifier *HttpNotifier) Notify(msg Message) error {
	switch msg.(type) {
	case *ConsumerGroupStatus:
		result := msg.(*ConsumerGroupStatus)
		return notifier.sendConsumerGroupStatusNotify(result)
	default:
		return nil
	}
	return nil
}

func (notifier *HttpNotifier) Ignore(msg Message) bool {
	switch msg.(type) {
	case *ConsumerGroupStatus:
		result, _ := msg.(*ConsumerGroupStatus)
		return int(result.Status) < notifier.threshold
	}
	return true
}

func NewHttpNotifier(app *ApplicationContext) (*HttpNotifier, error) {
	httpConfig := app.Config.Httpnotifier

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
	templatePost, err := template.New("post").Funcs(fmap).ParseFiles(httpConfig.TemplatePost)
	if err != nil {
		log.Criticalf("Cannot parse HTTP notifier POST template: %v", err)
		os.Exit(1)
	}
	templatePost = templatePost.Templates()[0]

	templateDelete, err := template.New("delete").Funcs(fmap).ParseFiles(httpConfig.TemplateDelete)
	if err != nil {
		log.Criticalf("Cannot parse HTTP notifier DELETE template: %v", err)
		os.Exit(1)
	}
	templateDelete = templateDelete.Templates()[0]

	// Parse the extra parameters for the templates
	extras := make(map[string]string)
	for _, extra := range httpConfig.Extras {
		parts := strings.Split(extra, "=")
		extras[parts[0]] = parts[1]
	}

	return &HttpNotifier{
		url:            httpConfig.Url,
		threshold:      httpConfig.PostThreshold,
		sendDelete:     httpConfig.SendDelete,
		templatePost:   templatePost,
		templateDelete: templateDelete,
		extras:         extras,
		groupIds:       make(map[string]map[string]Event),
		httpClient: &http.Client{
			Timeout: time.Duration(httpConfig.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(httpConfig.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil
}

func (notifier *HttpNotifier) sendConsumerGroupStatusNotify(result *ConsumerGroupStatus) error {
	// We only use IDs if we are sending deletes
	idStr := ""
	startTime := time.Now()
	if notifier.sendDelete {
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
		return err
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest("POST", notifier.url, bytesToSend)
	req.Header.Set("Content-Type", "application/json")

	resp, err := notifier.httpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %v", result.Group, result.Cluster, result.Status, idStr, err)
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
		log.Debugf("Sent POST for group %s in cluster %s at severity %v (Id %s)", result.Group, result.Cluster, result.Status, idStr)
	} else {
		log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %s", result.Group,
			result.Cluster, result.Status, idStr, resp.Status)
	}

	if notifier.sendDelete && (result.Status == StatusOK) {
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
				return err
			}

			req, err := http.NewRequest("DELETE", notifier.url, bytesToSend)
			req.Header.Set("Content-Type", "application/json")

			resp, err := notifier.httpClient.Do(req)
			if err != nil {
				log.Errorf("Failed to send DELETE for group %s in cluster %s (Id %s): %v", result.Group,
					result.Cluster, notifier.groupIds[result.Cluster][result.Group].Id, err)
				return err
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
	return nil
}
