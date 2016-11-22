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
	"io"
	"io/ioutil"
	"net/http"
	"text/template"
	"time"

	log "github.com/cihub/seelog"
	"github.com/pborman/uuid"
	"github.com/prasincs/Burrow/protocol"
)

type HttpNotifier struct {
	Url                string
	TemplatePostFile   string
	TemplateDeleteFile string
	Threshold          int
	SendDelete         bool
	Extras             map[string]string
	HttpClient         *http.Client
	templatePost       *template.Template
	templateDelete     *template.Template
	groupIds           map[string]map[string]Event
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

	if notifier.templatePost == nil {
		// Compile the templates
		templatePost, err := template.New("post").Funcs(fmap).ParseFiles(notifier.TemplatePostFile)
		if err != nil {
			log.Criticalf("Cannot parse HTTP notifier POST template: %v", err)
			return err
		}
		notifier.templatePost = templatePost.Templates()[0]
	}

	if notifier.templateDelete == nil {
		templateDelete, err := template.New("delete").Funcs(fmap).ParseFiles(notifier.TemplateDeleteFile)
		if err != nil {
			log.Criticalf("Cannot parse HTTP notifier DELETE template: %v", err)
			return err
		}
		notifier.templateDelete = templateDelete.Templates()[0]
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
	if notifier.SendDelete {
		if _, ok := notifier.groupIds[msg.Cluster]; !ok {
			// Create the cluster map
			notifier.groupIds[msg.Cluster] = make(map[string]Event)
		}
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

	// NOTE - I'm leaving the JsonEncode item in here so as not to break compatibility. New helpers go in the FuncMap above
	bytesToSend := new(bytes.Buffer)
	err := notifier.templatePost.Execute(bytesToSend, struct {
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
	})
	if err != nil {
		log.Errorf("Failed to assemble POST: %v", err)
		return err
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest("POST", notifier.Url, bytesToSend)
	req.Header.Set("Content-Type", "application/json")

	resp, err := notifier.HttpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %v", msg.Group, msg.Cluster, msg.Status, idStr, err)
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
		log.Debugf("Sent POST for group %s in cluster %s at severity %v (Id %s)", msg.Group, msg.Cluster, msg.Status, idStr)
	} else {
		log.Errorf("Failed to send POST for group %s in cluster %s at severity %v (Id %s): %s", msg.Group,
			msg.Cluster, msg.Status, idStr, resp.Status)
	}

	if notifier.SendDelete && (msg.Status == protocol.StatusOK) {
		if _, ok := notifier.groupIds[msg.Cluster][msg.Group]; ok {
			// Send DELETE to HTTP endpoint
			bytesToSend := new(bytes.Buffer)
			err := notifier.templateDelete.Execute(bytesToSend, struct {
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
			})
			if err != nil {
				log.Errorf("Failed to assemble DELETE for group %s in cluster %s (Id %s): %v", msg.Group,
					msg.Cluster, notifier.groupIds[msg.Cluster][msg.Group].Id, err)
				return err
			}

			req, err := http.NewRequest("DELETE", notifier.Url, bytesToSend)
			req.Header.Set("Content-Type", "application/json")

			resp, err := notifier.HttpClient.Do(req)
			if err != nil {
				log.Errorf("Failed to send DELETE for group %s in cluster %s (Id %s): %v", msg.Group,
					msg.Cluster, notifier.groupIds[msg.Cluster][msg.Group].Id, err)
				return err
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()

			if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
				log.Debugf("Sent DELETE for group %s in cluster %s (Id %s)", msg.Group, msg.Cluster,
					notifier.groupIds[msg.Cluster][msg.Group].Id)
			} else {
				log.Errorf("Failed to send DELETE for group %s in cluster %s (Id %s): %s", msg.Group,
					msg.Cluster, notifier.groupIds[msg.Cluster][msg.Group].Id, resp.Status)
			}

			// Remove ID for group that is now clear
			delete(notifier.groupIds[msg.Cluster], msg.Group)
		}
	}
	return nil
}
