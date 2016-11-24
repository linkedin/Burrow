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
	"github.com/linkedin/Burrow/notifier"
	"github.com/linkedin/Burrow/protocol"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type NotifyCenter struct {
	app            *ApplicationContext
	interval       int64
	notifiers      []notifier.Notifier
	refreshTicker  *time.Ticker
	quitChan       chan struct{}
	groupList      map[string]map[string]bool
	groupLock      sync.RWMutex
	resultsChannel chan *protocol.ConsumerGroupStatus
}

func LoadNotifiers(app *ApplicationContext) error {
	notifiers := []notifier.Notifier{}
	if app.Config.Httpnotifier.UrlOpen != "" {
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
	if app.Config.Slacknotifier.Enable {
		if slackNotifier, err := NewSlackNotifier(app); err == nil {
			notifiers = append(notifiers, slackNotifier)
		}
	}

	nc := &NotifyCenter{
		app:            app,
		notifiers:      notifiers,
		interval:       app.Config.Notify.Interval,
		quitChan:       make(chan struct{}),
		groupList:      make(map[string]map[string]bool),
		groupLock:      sync.RWMutex{},
		resultsChannel: make(chan *protocol.ConsumerGroupStatus),
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

func (nc *NotifyCenter) handleEvaluationResponse(result *protocol.ConsumerGroupStatus) {
	msg := notifier.Message(*result)
	for _, notifier := range nc.notifiers {
		notifier.Notify(msg)
	}
}

func (nc *NotifyCenter) refreshConsumerGroups() {
	nc.groupLock.Lock()
	defer nc.groupLock.Unlock()

	for cluster, _ := range nc.app.Config.Kafka {
		clusterGroups, ok := nc.groupList[cluster]
		if !ok {
			nc.groupList[cluster] = make(map[string]bool)
			clusterGroups = nc.groupList[cluster]
		}

		// Get a current list of consumer groups
		storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
		nc.app.Storage.requestChannel <- storageRequest
		consumerGroups := <-storageRequest.Result

		// Check for new groups, mark existing groups true
		for _, consumerGroup := range consumerGroups {
			// Don't bother adding groups in the blacklist
			if !nc.app.Storage.AcceptConsumerGroup(consumerGroup) {
				continue
			}

			if _, ok := clusterGroups[consumerGroup]; !ok {
				// Add new consumer group and start checking it
				log.Infof("Start evaluating consumer group %s in cluster %s", consumerGroup, cluster)
				go nc.startConsumerGroupEvaluator(consumerGroup, cluster)
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

func (nc *NotifyCenter) startConsumerGroupEvaluator(group string, cluster string) {
	// Sleep for a random portion of the check interval
	time.Sleep(time.Duration(rand.Int63n(nc.interval*1000)) * time.Millisecond)

	for {
		// Make sure this group still exists
		nc.groupLock.RLock()
		if _, ok := nc.groupList[cluster][group]; !ok {
			nc.groupLock.RUnlock()
			log.Debugf("Stopping evaluator for consumer group %s in cluster %s", group, cluster)
			break
		}
		nc.groupLock.RUnlock()

		// Send requests for group status - responses are handled by the main loop (for now)
		storageRequest := &RequestConsumerStatus{Result: nc.resultsChannel, Cluster: cluster, Group: group}
		nc.app.Storage.requestChannel <- storageRequest

		// Sleep for the check interval
		time.Sleep(time.Duration(nc.interval) * time.Second)
	}
}

func NewEmailNotifier(app *ApplicationContext) ([]*notifier.EmailNotifier, error) {
	log.Info("Start email notify")
	emailers := []*notifier.EmailNotifier{}
	for to, cfg := range app.Config.Emailnotifier {
		if cfg.Enable {
			emailer := &notifier.EmailNotifier{
				Threshold:    cfg.Threshold,
				TemplateFile: app.Config.Smtp.Template,
				Server:       app.Config.Smtp.Server,
				Port:         app.Config.Smtp.Port,
				Username:     app.Config.Smtp.Username,
				Password:     app.Config.Smtp.Password,
				AuthType:     app.Config.Smtp.AuthType,
				Interval:     cfg.Interval,
				From:         app.Config.Smtp.From,
				To:           to,
				Groups:       cfg.Groups,
			}
			emailers = append(emailers, emailer)
		}
	}

	return emailers, nil
}

func NewHttpNotifier(app *ApplicationContext) (*notifier.HttpNotifier, error) {
	httpConfig := app.Config.Httpnotifier

	// Parse the extra parameters for the templates
	extras := make(map[string]string)
	for _, extra := range httpConfig.Extras {
		parts := strings.Split(extra, "=")
		extras[parts[0]] = parts[1]
	}

	return &notifier.HttpNotifier{
		RequestOpen: notifier.HttpNotifierRequest{
			Url:          httpConfig.UrlOpen,
			Method:       httpConfig.MethodOpen,
			TemplateFile: httpConfig.TemplateOpen,
		},
		RequestClose: notifier.HttpNotifierRequest{
			Url:          httpConfig.UrlClose,
			Method:       httpConfig.MethodClose,
			TemplateFile: httpConfig.TemplateClose,
		},
		Threshold:          httpConfig.PostThreshold,
		SendClose:          httpConfig.SendClose,
		Extras:             extras,
		HttpClient: &http.Client{
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

func NewSlackNotifier(app *ApplicationContext) (*notifier.SlackNotifier, error) {
	log.Info("Start Slack Notify")

	return &notifier.SlackNotifier{
		Url:       app.Config.Slacknotifier.Url,
		Groups:    app.Config.Slacknotifier.Groups,
		Threshold: app.Config.Slacknotifier.Threshold,
		Channel:   app.Config.Slacknotifier.Channel,
		Username:  app.Config.Slacknotifier.Username,
		IconUrl:   app.Config.Slacknotifier.IconUrl,
		IconEmoji: app.Config.Slacknotifier.IconEmoji,
		HttpClient: &http.Client{
			Timeout: time.Duration(app.Config.Slacknotifier.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(app.Config.Slacknotifier.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil
}
