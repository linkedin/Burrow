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
	log "github.com/cihub/seelog"
	"net/http"
	"time"
	"bytes"
	"io/ioutil"
	"fmt"
	"encoding/json"
	"errors"
)

type NewRelicInsightsNotifier struct {
	Url              string
	InsertKey        string
	EventType        string
	SendPerPartition bool
	Threshold        int
	Groups	         []string
	HttpClient       *http.Client
	notifyGroups     map[string]struct{}
}

type ConsumerInsightsEvent struct {
	EventType string `json:"eventType"`
	Timestamp int64  `json:"timestamp"`
	Cluster   string `json:"cluster"`
	Group     string `json:"group"`
	Status    string `json:"status"`
}

type PartitionInsightsEvent struct {
	EventType string `json:"eventType"`
	Timestamp int64  `json:"timestamp"`
	Cluster   string `json:"cluster"`
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Status    string `json:"status"`
	Lag       int64  `json:"lag"`
}


func (notifier *NewRelicInsightsNotifier) NotifierName() string {
	return "new-relic-insights-notify"
}

func (notifier *NewRelicInsightsNotifier) Ignore(msg Message) bool {
	if notifier.notifyGroups == nil {
		// put groups into map for easy lookup
		notifier.notifyGroups = make(map[string]struct{})
		for _, clusterGroup := range notifier.Groups {
			notifier.notifyGroups[clusterGroup] = struct{}{}
		}
	}

	clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
	_, isInGroupsMap := notifier.notifyGroups[clusterGroup]

	// ignore if status is below threshold or consumer group wasn't specified in config
	// if no groups were specified, default to notifying for any consumer group
	notifyForGroup := isInGroupsMap || len(notifier.Groups) == 0
	return int(msg.Status) < notifier.Threshold || !notifyForGroup
}

func (notifier *NewRelicInsightsNotifier) Notify(msg Message) error {
	startTime := int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)

	var events []interface{}
	if notifier.SendPerPartition {
		for _, partition := range msg.Partitions {
			events = append(events, PartitionInsightsEvent {
				EventType: notifier.EventType,
				Timestamp: startTime,
				Cluster:   msg.Cluster,
				Group:     msg.Group,
				Topic:     partition.Topic,
				Partition: partition.Partition,
				Status:    partition.Status.String(),
				Lag:       partition.End.Offset - partition.Start.Offset,
			})
		}
	} else {
		events = append(events, ConsumerInsightsEvent {
			EventType: notifier.EventType,
			Timestamp: startTime,
			Cluster:   msg.Cluster,
			Group:     msg.Group,
			Status:    msg.Status.String(),
		})
	}
	return notifier.sendConsumerGroupStatusNotify(msg, events)
}

func (notifier *NewRelicInsightsNotifier) sendConsumerGroupStatusNotify(msg Message, events []interface{}) error {
	data, err := json.Marshal(events)
	if err != nil {
		log.Errorf("Failed to assemble request body for New Relic Insights: %v", err)
		return err
	}

	eventBytes := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", notifier.Url, eventBytes)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Insert-Key", notifier.InsertKey)

	if res, err := notifier.HttpClient.Do(req); err != nil {
		log.Errorf("Unable to send data to New Relic Insights:%+v", err)
		return err
	} else {
		defer res.Body.Close()
		statusCode := res.StatusCode
		if statusCode >= 200 && statusCode <= 299 {
			log.Debugf("Sent New Relic Insights POST for group %s in cluster %s at severity %v",
				msg.Group, msg.Cluster, msg.Status)
			return nil
		} else {
			body, _ := ioutil.ReadAll(res.Body)
			log.Errorf("Failed to send New Relic Insights POST for group %s in cluster %s. %s status returned: %s",
				msg.Group, msg.Cluster, msg.Status, string(body))
			return errors.New("POST to New Relic Insights failed")
		}
	}
}
