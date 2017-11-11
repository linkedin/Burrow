/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
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
	"errors"
	"math"
	"regexp"
	"sync"
	"text/template"
	"time"

	"github.com/pborman/uuid"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type Module interface {
	protocol.Module
	GetName() string
	GetGroupWhitelist() *regexp.Regexp
	GetLogger() *zap.Logger
	AcceptConsumerGroup(*protocol.ConsumerGroupStatus) bool
	Notify(*protocol.ConsumerGroupStatus, string, time.Time, bool)
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
	App     *protocol.ApplicationContext
	Log     *zap.Logger
	modules map[string]protocol.Module

	minInterval       int64
	groupRefresh      helpers.Ticker
	evalInterval      helpers.Ticker
	evaluatorResponse chan *protocol.ConsumerGroupStatus
	running           sync.WaitGroup
	quitChannel       chan struct{}

	templateParseFunc func(...string) (*template.Template, error)
	notifyModuleFunc  func(Module, *protocol.ConsumerGroupStatus, time.Time, string)

	clusters    map[string]*ClusterGroups
	clusterLock *sync.RWMutex
}

func GetModuleForClass(app *protocol.ApplicationContext,
	moduleName string,
	className string,
	groupWhitelist *regexp.Regexp,
	extras map[string]string,
	templateOpen *template.Template,
	templateClose *template.Template) protocol.Module {

	logger := app.Logger.With(
		zap.String("type", "module"),
		zap.String("coordinator", "notifier"),
		zap.String("class", className),
		zap.String("name", moduleName),
	)

	switch className {
	case "http":
		return &HttpNotifier{
			App:            app,
			Log:            logger,
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "email":
		return &EmailNotifier{
			App:            app,
			Log:            logger,
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "slack":
		return &SlackNotifier{
			App:            app,
			Log:            logger,
			groupWhitelist: groupWhitelist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "null":
		return &NullNotifier{
			App:            app,
			Log:            logger,
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
	nc.Log.Info("configuring")
	nc.modules = make(map[string]protocol.Module)

	nc.clusters = make(map[string]*ClusterGroups)
	nc.clusterLock = &sync.RWMutex{}
	nc.minInterval = math.MaxInt64

	nc.quitChannel = make(chan struct{})
	nc.running = sync.WaitGroup{}
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
	for name := range viper.GetStringMap("notifier") {
		configRoot := "notifier." + name

		// Set some defaults for common module fields
		viper.SetDefault(configRoot+".interval", 60)
		viper.SetDefault(configRoot+".threshold", 2)

		// Compile the whitelist for the consumer groups to notify for
		var groupWhitelist *regexp.Regexp
		whitelist := viper.GetString(configRoot + ".group-whitelist")
		if whitelist != "" {
			re, err := regexp.Compile(whitelist)
			if err != nil {
				nc.Log.Panic("Failed to compile group whitelist", zap.String("module", name))
				panic(err)
			}
			groupWhitelist = re
		}

		// Set up extra fields for the templates
		extras := viper.GetStringMapString(configRoot + ".extras")

		// Compile the templates
		var templateOpen, templateClose *template.Template
		tmpl, err := nc.templateParseFunc(viper.GetString(configRoot + ".template-open"))
		if err != nil {
			nc.Log.Panic("Failed to compile TemplateOpen", zap.Error(err), zap.String("module", name))
			panic(err)
		}
		templateOpen = tmpl.Templates()[0]

		if viper.GetBool(configRoot + ".send-close") {
			tmpl, err = nc.templateParseFunc(viper.GetString(configRoot + ".template-close"))
			if err != nil {
				nc.Log.Panic("Failed to compile TemplateClose", zap.Error(err), zap.String("module", name))
				panic(err)
			}
			templateClose = tmpl.Templates()[0]
		}

		module := GetModuleForClass(nc.App, name, viper.GetString(configRoot+".class-name"), groupWhitelist, extras, templateOpen, templateClose)
		module.Configure(name, configRoot)
		nc.modules[name] = module

		interval := viper.GetInt64(configRoot + ".interval")
		if interval < nc.minInterval {
			nc.minInterval = interval
		}
	}

	// If there are no modules specified, the minInterval will still be MaxInt64. Set it to a large number of seconds
	if nc.minInterval == math.MaxInt64 {
		nc.minInterval = 310536000
	}

	// Set up the tickers but do not start them
	// TODO - should probably be configurable
	nc.groupRefresh = helpers.NewPausableTicker(60 * time.Second)
	nc.evalInterval = helpers.NewPausableTicker(time.Duration(nc.minInterval) * time.Second)
}

func (nc *Coordinator) Start() error {
	nc.Log.Info("starting")

	// The notifier coordinator is responsible for fetching group evaluations and handing them off to the individual
	// notifier modules.
	go nc.responseLoop()

	err := helpers.StartCoordinatorModules(nc.modules)
	if err != nil {
		return errors.New("Error starting notifier module: " + err.Error())
	}

	// Run the group refresh regardless of whether or not we're sending notifications
	nc.groupRefresh.Start()

	// Run a goroutine to manage whether or not we're performing evaluations
	go nc.manageEvalLoop()

	// Run our main loop to watch tickers and take actions
	go nc.tickerLoop()

	return nil
}

func (nc *Coordinator) Stop() error {
	nc.Log.Info("stopping")

	nc.groupRefresh.Stop()
	nc.evalInterval.Stop()

	close(nc.quitChannel)

	// The individual notifier modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(nc.modules)
	return nil
}

func (nc *Coordinator) manageEvalLoop() {
	lock := nc.App.Zookeeper.NewLock(nc.App.ZookeeperRoot + "/notifier")

	for {
		time.Sleep(100 * time.Millisecond)
		err := lock.Lock()
		if err != nil {
			nc.Log.Warn("failed to get zk lock", zap.Error(err))
			continue
		}

		// We've got the lock, start the evaluation ticker
		nc.evalInterval.Start()
		nc.Log.Info("starting evaluations", zap.Error(err))

		// Wait for ZK session expiration, and stop sending notifications if it happens
		nc.App.ZookeeperExpired.L.Lock()
		nc.App.ZookeeperExpired.Wait()
		nc.App.ZookeeperExpired.L.Unlock()
		nc.evalInterval.Stop()
		nc.Log.Info("stopping evaluations", zap.Error(err))

		// Wait for the ZK connection to come back before trying again
		for !nc.App.ZookeeperConnected {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// We keep this function trivial because tickers are not easy to mock/test in golang
func (nc *Coordinator) tickerLoop() {
	for {
		select {
		case <-nc.groupRefresh.GetChannel():
			nc.sendClusterRequest()
		case <-nc.evalInterval.GetChannel():
			nc.sendEvaluatorRequests()
		case <-nc.quitChannel:
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

func moduleAcceptConsumerGroup(module Module, status *protocol.ConsumerGroupStatus) bool {
	if int(status.Status) < viper.GetInt("notifier."+module.GetName()+".threshold") {
		return false
	}

	// No whitelist means everything passes
	groupWhitelist := module.GetGroupWhitelist()
	if (groupWhitelist == nil) || (groupWhitelist.MatchString(status.Group)) {
		return module.AcceptConsumerGroup(status)
	}

	return false
}

func (nc *Coordinator) responseLoop() {
	for {
		select {
		case response := <-nc.evaluatorResponse:
			// If response is nil, the group no longer exists
			if response == nil {
				continue
			}

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
					module := genericModule.(Module)
					if moduleAcceptConsumerGroup(module, response) {
						go nc.notifyModuleFunc(module, response, cgroup.Start, cgroup.Id)
					}
				}

				if response.Status == protocol.StatusOK {
					// Incident closed - clear the start time and event ID
					cgroup.Id = ""
					cgroup.Start = time.Time{}
				}
			}
		case <-nc.quitChannel:
			return
		}
	}
}

func (nc *Coordinator) processClusterList(replyChan chan interface{}) {
	response := <-replyChan
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

func (nc *Coordinator) processConsumerList(cluster string, replyChan chan interface{}) {
	response := <-replyChan
	consumerList, _ := response.([]string)

	nc.clusterLock.RLock()
	defer nc.clusterLock.RUnlock()

	if _, ok := nc.clusters[cluster]; ok {
		nc.clusters[cluster].Lock.Lock()
		consumerMap := make(map[string]struct{})
		for _, group := range consumerList {
			consumerMap[group] = struct{}{}
			nc.clusters[cluster].Groups[group] = &ConsumerGroup{
				Last: make(map[string]time.Time),
			}
		}

		// Use the map we just made to delete consumers that no longer exist
		for group := range nc.clusters[cluster].Groups {
			if _, ok := consumerMap[group]; !ok {
				delete(nc.clusters[cluster].Groups, group)
			}
		}
		nc.clusters[cluster].Lock.Unlock()
	}
}

func (nc *Coordinator) notifyModule(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
	nc.running.Add(1)
	defer nc.running.Done()

	currentTime := time.Now()
	moduleName := module.GetName()
	stateGood := (status.Status == protocol.StatusOK) || (int(status.Status) < viper.GetInt("notifier."+moduleName+".threshold"))

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
		if viper.GetBool("notifier." + moduleName + ".send-close") {
			module.Notify(status, eventId, startTime, stateGood)
		}
		cgroup.Last[module.GetName()] = time.Time{}
	} else {
		// Only send the notification if it's been at least our Interval since the last one for this group
		if currentTime.Sub(cgroup.Last[module.GetName()]) > (time.Duration(viper.GetInt("notifier."+moduleName+".interval")) * time.Second) {
			module.Notify(status, eventId, startTime, stateGood)
			cgroup.Last[module.GetName()] = currentTime
		}
	}
}

// Common method for executing a template in modules
func ExecuteTemplate(tmpl *template.Template, extras map[string]string, status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time) (*bytes.Buffer, error) {
	bytesToSend := new(bytes.Buffer)
	err := tmpl.Execute(bytesToSend, struct {
		Cluster string
		Group   string
		Id      string
		Start   time.Time
		Extras  map[string]string
		Result  protocol.ConsumerGroupStatus
	}{
		Cluster: status.Cluster,
		Group:   status.Group,
		Id:      eventId,
		Start:   startTime,
		Extras:  extras,
		Result:  *status,
	})
	if err != nil {
		return nil, err
	}
	return bytesToSend, nil
}
