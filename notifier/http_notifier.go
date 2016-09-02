/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"bytes"
	log "github.com/cihub/seelog"
	"github.com/linkedin/Burrow/protocol"
	"github.com/pborman/uuid"
	"io"
	"io/ioutil"
	"net/http"
	"text/template"
	"time"
	"fmt"
)

type HttpNotifier struct {
	RequestOpen        HttpNotifierRequest
	RequestClose       HttpNotifierRequest
	Threshold          int
	SendClose          bool
	Extras             map[string]string
	HttpClient         *http.Client
	groupIds           map[string]map[string]Event
}

type HttpNotifierRequest struct {
	Url string
	TemplateFile string
	Method string
	template *template.Template
}

type Event struct {
	Id    string
	Start time.Time
}

func (notify *HttpNotifier) NotifierName() string {
	return "http-notify"
}

func (notifier *HttpNotifier) Notify(msg Message) error {
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

	err := notifier.RequestOpen.ensureTemplateCompiled("post", fmap)
	if err != nil {
		log.Criticalf("Cannot parse HTTP notifier open template: %v", err)
		return err
	}

	err = notifier.RequestClose.ensureTemplateCompiled("delete", fmap)
	if err != nil {
		log.Criticalf("Cannot parse HTTP notifier close template: %v", err)
		return err
	}

	if notifier.groupIds == nil {
		notifier.groupIds = make(map[string]map[string]Event)
	}

	return notifier.sendConsumerGroupStatusNotify(msg)
}

func (notifier *HttpNotifier) Ignore(msg Message) bool {
	return int(msg.Status) < notifier.Threshold
}

func (notifier *HttpNotifier) sendConsumerGroupStatusNotify(msg Message) error {
	// We only use IDs if we are sending deletes
	idStr := ""
	startTime := time.Now()
	if notifier.SendClose {
		if _, ok := notifier.groupIds[msg.Cluster]; !ok {
			// Create the cluster map
			notifier.groupIds[msg.Cluster] = make(map[string]Event)
		}
		if !notifier.Ignore(msg) {
			if _, ok := notifier.groupIds[msg.Cluster][msg.Group]; !ok {
				// Create Event and Id
				eventId := uuid.NewRandom()
				idStr = eventId.String()
				notifier.groupIds[msg.Cluster][msg.Group] = Event{
					Id:    idStr,
					Start: startTime,
				}
			} else {
				idStr = notifier.groupIds[msg.Cluster][msg.Group].Id
				startTime = notifier.groupIds[msg.Cluster][msg.Group].Start
			}
		}
	}

	if !notifier.Ignore(msg) {
		// NOTE - I'm leaving the JsonEncode item in here so as not to break compatibility. New helpers go in the FuncMap above
		err := notifier.RequestOpen.send(struct {
			Cluster    string
			Group      string
			Id         string
			Start      time.Time
			Extras     map[string]string
			Result     Message
			JsonEncode func(interface{}) string
		}{
			Cluster:    msg.Cluster,
			Group:      msg.Group,
			Id:         idStr,
			Start:      startTime,
			Extras:     notifier.Extras,
			Result:     msg,
			JsonEncode: templateJsonEncoder,
		}, notifier.HttpClient, fmt.Sprintf("open for group %s in cluster %s at severity %v (Id %s)", msg.Group, msg.Cluster, msg.Status, idStr))

		if err != nil {
			return err
		}
	}

	if notifier.SendClose && (msg.Status == protocol.StatusOK) {
		if _, ok := notifier.groupIds[msg.Cluster][msg.Group]; ok {
			err := notifier.RequestClose.send(struct {
				Cluster string
				Group   string
				Id      string
				Start   time.Time
				Extras  map[string]string
			}{
				Cluster: msg.Cluster,
				Group:   msg.Group,
				Id:      notifier.groupIds[msg.Cluster][msg.Group].Id,
				Start:   notifier.groupIds[msg.Cluster][msg.Group].Start,
				Extras:  notifier.Extras,
			}, notifier.HttpClient, fmt.Sprintf("close for group %s in cluster %s (Id %s)", msg.Group,
					msg.Cluster, notifier.groupIds[msg.Cluster][msg.Group].Id))

			if err != nil {
				return err
			}

			// Remove ID for group that is now clear
			delete(notifier.groupIds[msg.Cluster], msg.Group)
		}
	}
	return nil
}

func (request *HttpNotifierRequest) ensureTemplateCompiled(name string, fmap template.FuncMap) error {
	if request.template != nil {
		return nil
	}

	template, err := template.New(name).Funcs(fmap).ParseFiles(request.TemplateFile)
	if err != nil {
		return err
	}
	request.template = template.Templates()[0]

	return nil
}

func (request *HttpNotifierRequest) send(templateData interface{}, httpClient *http.Client, details string) error {
	bytesToSend := new(bytes.Buffer)
	err := request.template.Execute(bytesToSend, templateData)
	if err != nil {
		log.Errorf("Failed to assemble %s: %v", details, err)
		return err
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest(request.Method, request.Url, bytesToSend)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to send %s: %v", details, err)
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
		log.Debugf("Sent %s", details)
	} else {
		log.Errorf("Failed to send %s: %s", details, resp.Status)
	}

	return nil
}