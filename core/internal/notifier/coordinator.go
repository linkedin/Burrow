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
	"errors"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/pborman/uuid"
	"github.com/linkedin/Burrow/core/configuration"
	"regexp"
	"text/template"
	"strings"
)

type NotifierModule interface {
	protocol.Module
	GetName() string
	GetConfig() *configuration.NotifierConfig
	GetLogger() *zap.Logger
	AcceptConsumerGroup (*protocol.ConsumerGroupStatus) bool
	Notify (*protocol.ConsumerGroupStatus, string, time.Time, bool)
}

type ConsumerGroup struct {
	Id    string
	Start time.Time
	Last  map[string]time.Time
}

type ClusterGroups struct {
	Groups map[string]*ConsumerGroup
	Lock   *sync.RWMutex
}

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger
	modules     map[string]protocol.Module

	minInterval       int64
	groupRefresh      *time.Ticker
	evalInterval      *time.Ticker
	evaluatorResponse chan *protocol.ConsumerGroupStatus
	running           sync.WaitGroup
	quitChannel       chan struct{}

	templateParseFunc func (...string) (*template.Template, error)
	notifyModuleFunc  func (NotifierModule, *protocol.ConsumerGroupStatus, time.Time, string)

	clusters          map[string]*ClusterGroups
	clusterLock       *sync.RWMutex
}

func GetModuleForClass(app *protocol.ApplicationContext,
	                   className string,
	                   groupWhitelist *regexp.Regexp,
	                   extras map[string]string,
	                   templateOpen *template.Template,
	                   templateClose *template.Template) protocol.Module {
	switch className {
	case "http":
		return &HttpNotifier{
			App:            app,
			Log:            app.Logger.With(
				                zap.String("type", "module"),
								zap.String("coordinator", "notifier"),
								zap.String("name", "http"),
							),
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "email":
		return &EmailNotifier{
			App:            app,
			Log:            app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "email"),
			),
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "slack":
		return &SlackNotifier{
			App:            app,
			Log:            app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "slack"),
			),
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "null":
		return &NullNotifier{
			App:            app,
			Log:            app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "null"),
			),
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	default:
		panic("Unknown notifier className provided: " + className)
	}
}

func (nc *Coordinator) Configure() {
	nc.modules = make(map[string]protocol.Module)

	nc.clusters = make(map[string]*ClusterGroups)
	nc.clusterLock = &sync.RWMutex{}
	nc.minInterval = math.MaxInt64

	nc.quitChannel = make(chan struct{})
	nc.evaluatorResponse = make(chan *protocol.ConsumerGroupStatus)

	// Set the function for parsing templates and calling module Notify (configurable to enable testing)
	if nc.templateParseFunc == nil {
		nc.templateParseFunc = template.New("notifier").Funcs(helperFunctionMap).ParseFiles
	}
	if nc.notifyModuleFunc == nil {
		nc.notifyModuleFunc = nc.notifyModule
	}

	// Create all configured notifier modules, add to list of notifier
	// Note - we do a lot more work here than for other coordinators. This is because the notifier modules really just
	//        contain the logic to send the notification. Many of the parts, such as the whitelist and templates, are
	//        common to all notifier modules
	for name, config := range nc.App.Configuration.Notifier {
		moduleConfig := nc.App.Configuration.Notifier[name]

		// Set some defaults for common module fields
		if moduleConfig.Interval == 0 {
			moduleConfig.Interval = 60
		}
		if moduleConfig.Threshold == 0 {
			// protocol.StatusWarning
			moduleConfig.Threshold = 2
		}

		// Compile the whitelist for the consumer groups to notify for
		var groupWhitelist *regexp.Regexp
		if moduleConfig.GroupWhitelist != "" {
			re, err := regexp.Compile(moduleConfig.GroupWhitelist)
			if err != nil {
				nc.Log.Panic("Failed to compile group whitelist", zap.String("module", name))
				panic(err)
			}
			groupWhitelist = re
		}

		// Set up extra fields for the templates
		extras := make(map[string]string)
		for _, extra := range moduleConfig.Extras {
			parts := strings.Split(extra, "=")
			if len(parts) < 2 {
				nc.Log.Panic("extras badly formatted", zap.String("module", name))
				panic(errors.New("configuration error"))
			}
			extras[parts[0]] = strings.Join(parts[1:], "=")
		}

		// Compile the templates
		var templateOpen, templateClose *template.Template
		tmpl, err := nc.templateParseFunc(moduleConfig.TemplateOpen)
		if err != nil {
			nc.Log.Panic("Failed to compile TemplateOpen", zap.Error(err), zap.String("module", name))
			panic(err)
		}
		templateOpen = tmpl.Templates()[0]

		if nc.App.Configuration.Notifier[name].SendClose {
			tmpl, err = nc.templateParseFunc(moduleConfig.TemplateClose)
			if err != nil {
				nc.Log.Panic("Failed to compile TemplateClose", zap.Error(err), zap.String("module", name))
				panic(err)
			}
			templateClose = tmpl.Templates()[0]
		}

		module := GetModuleForClass(nc.App, config.ClassName, groupWhitelist, extras, templateOpen, templateClose)
		module.Configure(name)
		nc.modules[name] = module

		if moduleConfig.Interval < nc.minInterval {
			nc.minInterval = moduleConfig.Interval
		}
	}
}

func (nc *Coordinator) Start() error {
	// The notifier coordinator is responsible for fetching group evaluations and handing them off to the individual
	// notifier modules.
	go nc.responseLoop()

	err := helpers.StartCoordinatorModules(nc.modules)
	if err != nil {
		return errors.New("Error starting notifier module: " + err.Error())
	}

	// TODO - should probably be configurable
	nc.groupRefresh = time.NewTicker(60 * time.Second)
	nc.evalInterval = time.NewTicker(time.Duration(nc.minInterval) * time.Second)
	go nc.tickerLoop()

	return nil
}

func (nc *Coordinator) Stop() error {
	nc.groupRefresh.Stop()
	nc.evalInterval.Stop()

	close(nc.quitChannel)

	// The individual notifier modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(nc.modules)
	return nil
}

// We keep this function trivial because tickers are not easy to mock/test in golang
func (nc *Coordinator) tickerLoop() {
	for {
		select {
		case <- nc.groupRefresh.C:
			nc.sendClusterRequest()
		case <- nc.evalInterval.C:
			nc.sendEvaluatorRequests()
		case <- nc.quitChannel:
			return
		}
	}
}

func (nc *Coordinator) sendClusterRequest() {
	// Send a request to the storage module for a list of clusters, and spawn a goroutine to process it
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	go func() {
		nc.running.Add(1)
		defer nc.running.Done()
		nc.processClusterList(request.Reply)
	}()
	helpers.TimeoutSendStorageRequest(nc.App.StorageChannel, request, 1)
}

func (nc *Coordinator) sendEvaluatorRequests() {
	// Fire off evaluation requests for every group we know about
	nc.clusterLock.RLock()
	for cluster, consumerGroup := range nc.clusters {
		consumerGroup.Lock.RLock()
		for consumer := range consumerGroup.Groups {
			go func(sendCluster string, sendConsumer string) {
				nc.App.EvaluatorChannel <- &protocol.EvaluatorRequest{
					Reply:   nc.evaluatorResponse,
					Cluster: sendCluster,
					Group:   sendConsumer,
				}
			}(cluster, consumer)
		}
		consumerGroup.Lock.RUnlock()
	}
	nc.clusterLock.RUnlock()
}

func (nc *Coordinator) responseLoop() {
	for {
		select {
		case response := <-nc.evaluatorResponse:
			// As long as the response is not NotFound, send it to the modules
			if response.Status != protocol.StatusNotFound {
				clusterGroups := nc.clusters[response.Cluster].Groups
				cgroup, ok := clusterGroups[response.Group]
				if !ok {
					// The group must have just been deleted
					continue
				}

				if cgroup.Start.IsZero() && (response.Status > protocol.StatusOK) {
					// New incident - assign an ID and start time
					cgroup.Id = uuid.NewRandom().String()
					cgroup.Start = time.Now()
				}

				for _, genericModule := range nc.modules {
					module := genericModule.(NotifierModule)
					if module.AcceptConsumerGroup(response) {
						go nc.notifyModuleFunc(module, response, cgroup.Start, cgroup.Id)
					}
				}

				if response.Status == protocol.StatusOK {
					// Incident closed - clear the start time and event ID
					cgroup.Id = ""
					cgroup.Start = time.Time{}
				}
			}
		case <- nc.quitChannel:
			return
		}
	}
}

func (nc *Coordinator) processClusterList(replyChan chan interface{}) {
	response := <- replyChan
	switch response.(type) {
	case []string:
		clusterList, _ := response.([]string)
		requestMap := make(map[string]*protocol.StorageRequest)

		nc.clusterLock.Lock()
		for _, cluster := range clusterList {
			// Make sure we have a map entry for the cluster
			if _, ok := nc.clusters[cluster]; !ok {
				nc.clusters[cluster] = &ClusterGroups{
					Lock:   &sync.RWMutex{},
					Groups: make(map[string]*ConsumerGroup),
				}
			}

			// Create a request for the group list for this cluster
			requestMap[cluster] = &protocol.StorageRequest{
				RequestType: protocol.StorageFetchConsumers,
				Reply:       make(chan interface{}),
				Cluster:     cluster,
			}
		}

		// Delete clusters that no longer exist
		for cluster := range nc.clusters {
			if _, ok := requestMap[cluster]; !ok {
				delete(nc.clusters, cluster)
			}
		}
		nc.clusterLock.Unlock()

		// Fire off requests for group lists to the storage module, with goroutines to process the responses
		for cluster, request := range requestMap {
			go nc.processConsumerList(cluster, request.Reply)
			helpers.TimeoutSendStorageRequest(nc.App.StorageChannel, request, 1)
		}
	}
}

func (nc *Coordinator) processConsumerList(cluster string, replyChan chan interface{}) {
	response := <- replyChan
	switch response.(type) {
	case []string:
		consumerList, _ := response.([]string)
		nc.clusterLock.RLock()
		if _, ok := nc.clusters[cluster]; ok {
			nc.clusters[cluster].Lock.Lock()
			for _, group := range consumerList {
				nc.clusters[cluster].Groups[group] = &ConsumerGroup{
					Last: make(map[string]time.Time),
				}
			}
			nc.clusters[cluster].Lock.Unlock()
		}
		nc.clusterLock.RUnlock()
	}
}

func (nc *Coordinator) notifyModule(module NotifierModule, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
	nc.running.Add(1)
	defer nc.running.Done()

	currentTime := time.Now()
	moduleConfiguration := module.GetConfig()
	stateGood := (status.Status == protocol.StatusOK) || (int(status.Status) < moduleConfiguration.Threshold)

	cgroup, ok := nc.clusters[status.Cluster].Groups[status.Group]
	if !ok {
		// The group must have just been deleted
		return
	}
	lastTime := cgroup.Last[module.GetName()]
	if stateGood && lastTime.IsZero() {
		return
	}

	if stateGood {
		if moduleConfiguration.SendClose {
			module.Notify(status, eventId, startTime, stateGood)
		}
		cgroup.Last[module.GetName()] = time.Time{}
	} else {
		// Only send the notification if it's been at least our Interval since the last one for this group
		if currentTime.Sub(cgroup.Last[module.GetName()]) > (time.Duration(moduleConfiguration.Interval) * time.Second) {
			module.Notify(status, eventId, startTime, stateGood)
			cgroup.Last[module.GetName()] = currentTime
		}
	}
}
