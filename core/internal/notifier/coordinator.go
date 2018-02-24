/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package notifier - Status notification subsystem.
// The notifier subsystem watches the status for all consumer groups and uses the configured modules to send
// information about the status of those groups to outside systems, such as via email or calls to HTTP endpoints. The
// message bodies are built using templates, and notifications can be sent for both active problems as well as when
// those problems close.
//
// Modules
//
// Currently, the following modules are provided:
//
// * email - Send an email
//
// * http - Call a remote HTTP endpoint
//
// * null - This is a no-op notifier that is used for testing only
package notifier

import (
	"errors"
	"math"
	"math/rand"
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

// Module defines a means of sending out notifications of consumer group status (such as email), as well as regular
// expressions describing what groups to notify for. The module itself only provides the logic for how to send a
// notification in the Notify func - timing loops, and handling requests for group evaluation, are handled in the
// coordinator centrally.
type Module interface {
	protocol.Module
	GetName() string
	GetGroupWhitelist() *regexp.Regexp
	GetGroupBlacklist() *regexp.Regexp
	GetLogger() *zap.Logger
	AcceptConsumerGroup(*protocol.ConsumerGroupStatus) bool
	Notify(*protocol.ConsumerGroupStatus, string, time.Time, bool)
}

type consumerGroup struct {
	ID         string
	Start      time.Time
	LastNotify map[string]time.Time
	LastEval   time.Time
}

type clusterGroups struct {
	Groups map[string]*consumerGroup
	Lock   *sync.RWMutex
}

// Coordinator manages all notifier modules, making sure they are configured, started, and stopped at the
// appropriate time. Unlike other coordinators, it also performs a significant amount of ongoing work.
type Coordinator struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	modules map[string]protocol.Module

	minInterval       int64
	groupRefresh      helpers.Ticker
	doEvaluations     bool
	evaluatorResponse chan *protocol.ConsumerGroupStatus
	running           sync.WaitGroup
	quitChannel       chan struct{}

	templateParseFunc func(...string) (*template.Template, error)
	notifyModuleFunc  func(Module, *protocol.ConsumerGroupStatus, time.Time, string)

	clusters    map[string]*clusterGroups
	clusterLock *sync.RWMutex
}

// getModuleForClass returns the correct module based on the passed className. As part of the Configure steps, if there
// is any error, it will panic with an appropriate message describing the problem.
func getModuleForClass(app *protocol.ApplicationContext,
	moduleName string,
	className string,
	groupWhitelist *regexp.Regexp,
	groupBlacklist *regexp.Regexp,
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
		return &HTTPNotifier{
			App:            app,
			Log:            logger,
			groupWhitelist: groupWhitelist,
			groupBlacklist: groupBlacklist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "email":
		return &EmailNotifier{
			App:            app,
			Log:            logger,
			groupWhitelist: groupWhitelist,
			groupBlacklist: groupBlacklist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	case "null":
		return &NullNotifier{
			App:            app,
			Log:            logger,
			groupWhitelist: groupWhitelist,
			groupBlacklist: groupBlacklist,
			extras:         extras,
			templateOpen:   templateOpen,
			templateClose:  templateClose,
		}
	default:
		panic("Unknown notifier className provided: " + className)
	}
}

// Configure is called to create each of the configured notifier modules and call their Configure funcs to validate
// their individual configurations and set them up. If there are any problems, it is expected that these funcs will
// panic with a descriptive error message, as configuration failures are not recoverable errors.
func (nc *Coordinator) Configure() {
	nc.Log.Info("configuring")
	nc.modules = make(map[string]protocol.Module)

	nc.clusters = make(map[string]*clusterGroups)
	nc.clusterLock = &sync.RWMutex{}
	nc.minInterval = math.MaxInt64

	nc.quitChannel = make(chan struct{})
	nc.running = sync.WaitGroup{}
	nc.evaluatorResponse = make(chan *protocol.ConsumerGroupStatus)

	// Set the function for parsing templates and calling module Notify (configurable to enable testing)
	if nc.templateParseFunc == nil {
		nc.templateParseFunc = func(filenames ...string) (*template.Template, error) {
			return template.New("notifier").Funcs(helperFunctionMap).ParseFiles(filenames...)
		}
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
		viper.SetDefault(configRoot+".send-interval", viper.GetInt64(configRoot+".interval"))
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

		// Compile the blacklist for the consumer groups to not notify for
		var groupBlacklist *regexp.Regexp
		blacklist := viper.GetString(configRoot + ".group-blacklist")
		if blacklist != "" {
			re, err := regexp.Compile(blacklist)
			if err != nil {
				nc.Log.Panic("Failed to compile group blacklist", zap.String("module", name))
				panic(err)
			}
			groupBlacklist = re
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

		module := getModuleForClass(nc.App, name, viper.GetString(configRoot+".class-name"), groupWhitelist, groupBlacklist, extras, templateOpen, templateClose)
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
}

// Start calls each of the configured notifier modules' underlying Start funcs. If any module Start returns an error,
// this func stops immediately and returns that error to the caller. No further modules will be loaded after that.
//
// We also start a timer to periodically update the list of known clusters and consumer groups, as well as the
// goroutine which manages whether or not we are performing group evaluation requests. We also start the responseLoop
// func that handles all evaluation replies and calls the module Notify methods as appropriate.
func (nc *Coordinator) Start() error {
	nc.Log.Info("starting")

	// The notifier coordinator is responsible for fetching group evaluations and handing them off to the individual
	// notifier modules.
	nc.running.Add(1)
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
	nc.running.Add(1)
	go nc.tickerLoop()

	return nil
}

// Stop stops the group refresh ticker, and causes the evaluation response handler and the evaluation request manager to
// both stop. It then calls each of the configured notifier modules' underlying Stop funcs. It is expected that the
// module Stop will not return until the module has been completely stopped. While an error can be returned, this func
// always returns no error, as a failure during stopping is not a critical failure
func (nc *Coordinator) Stop() error {
	nc.Log.Info("stopping")

	nc.groupRefresh.Stop()
	nc.doEvaluations = false

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

		// We've got the lock, start the evaluation loop
		nc.doEvaluations = true
		nc.running.Add(1)
		go nc.sendEvaluatorRequests()
		nc.Log.Info("starting evaluations", zap.Error(err))

		// Wait for ZK session expiration, and stop doing evaluations if it happens
		nc.App.ZookeeperExpired.L.Lock()
		nc.App.ZookeeperExpired.Wait()
		nc.App.ZookeeperExpired.L.Unlock()
		nc.doEvaluations = false
		nc.Log.Info("stopping evaluations", zap.Error(err))

		// Wait for the ZK connection to come back before trying again
		for !nc.App.ZookeeperConnected {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// We keep this function trivial because tickers are not easy to mock/test in golang
func (nc *Coordinator) tickerLoop() {
	defer nc.running.Done()

	for {
		select {
		case <-nc.groupRefresh.GetChannel():
			nc.sendClusterRequest()
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

	nc.running.Add(1)
	go nc.processClusterList(request.Reply)
	helpers.TimeoutSendStorageRequest(nc.App.StorageChannel, request, 1)
}

func (nc *Coordinator) sendEvaluatorRequests() {
	defer nc.running.Done()

	for nc.doEvaluations {
		// Loop through all clusters and groups and send any evaluation requests that are due
		timeNow := time.Now()
		sendBefore := timeNow.Add(-time.Duration(nc.minInterval) * time.Second)

		// Fire off evaluation requests for every group we know about
		nc.clusterLock.RLock()
		for cluster, consumerGroup := range nc.clusters {
			consumerGroup.Lock.RLock()
			for consumer, groupInfo := range consumerGroup.Groups {
				if groupInfo.LastEval.Before(sendBefore) {
					nc.Log.Debug("Evaluating group", zap.String("group", consumer))
					go func(sendCluster string, sendConsumer string) {
						nc.App.EvaluatorChannel <- &protocol.EvaluatorRequest{
							Reply:   nc.evaluatorResponse,
							Cluster: sendCluster,
							Group:   sendConsumer,
						}
					}(cluster, consumer)
					groupInfo.LastEval = timeNow
				}
			}
			consumerGroup.Lock.RUnlock()
		}
		nc.clusterLock.RUnlock()

		// Sleep briefly to prevent a tight loop
		time.Sleep(time.Millisecond)
	}
}

func (nc *Coordinator) responseLoop() {
	defer nc.running.Done()

	for {
		select {
		case response := <-nc.evaluatorResponse:
			// If response is nil, the group no longer exists
			if response == nil {
				continue
			}

			// As long as the response is not NotFound, send it to the modules
			if response.Status != protocol.StatusNotFound {
				nc.running.Add(1)
				go nc.checkAndSendResponseToModules(response)
			}
		case <-nc.quitChannel:
			return
		}
	}
}

func (nc *Coordinator) checkAndSendResponseToModules(response *protocol.ConsumerGroupStatus) {
	defer nc.running.Done()

	nc.clusterLock.RLock()
	defer nc.clusterLock.RUnlock()
	cluster := nc.clusters[response.Cluster]

	cluster.Lock.RLock()
	defer cluster.Lock.RUnlock()
	cgroup, ok := cluster.Groups[response.Group]
	if !ok {
		// The group must have just been deleted
		return
	}

	if cgroup.Start.IsZero() && (response.Status > protocol.StatusOK) {
		// New incident - assign an ID and start time
		cgroup.ID = uuid.NewRandom().String()
		cgroup.Start = time.Now()
	}

	for _, genericModule := range nc.modules {
		module := genericModule.(Module)

		// No whitelist means everything passes
		groupWhitelist := module.GetGroupWhitelist()
		groupBlacklist := module.GetGroupBlacklist()
		if (groupWhitelist != nil) && (!groupWhitelist.MatchString(response.Group)) {
			continue
		}
		if (groupBlacklist != nil) && groupBlacklist.MatchString(response.Group) {
			continue
		}
		if module.AcceptConsumerGroup(response) {
			nc.running.Add(1)
			nc.notifyModuleFunc(module, response, cgroup.Start, cgroup.ID)
		}
	}

	if response.Status == protocol.StatusOK {
		// Incident closed - clear the start time and event ID
		cgroup.ID = ""
		cgroup.Start = time.Time{}
	}
}

func (nc *Coordinator) processClusterList(replyChan chan interface{}) {
	defer nc.running.Done()

	response := <-replyChan
	clusterList, _ := response.([]string)
	requestMap := make(map[string]*protocol.StorageRequest)

	nc.clusterLock.Lock()
	for _, cluster := range clusterList {
		// Make sure we have a map entry for the cluster
		if _, ok := nc.clusters[cluster]; !ok {
			nc.clusters[cluster] = &clusterGroups{
				Lock:   &sync.RWMutex{},
				Groups: make(map[string]*consumerGroup),
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
		nc.running.Add(1)
		go nc.processConsumerList(cluster, request.Reply)
		helpers.TimeoutSendStorageRequest(nc.App.StorageChannel, request, 1)
	}
}

func (nc *Coordinator) processConsumerList(cluster string, replyChan chan interface{}) {
	defer nc.running.Done()

	response := <-replyChan
	consumerList, _ := response.([]string)

	nc.clusterLock.RLock()
	defer nc.clusterLock.RUnlock()

	if _, ok := nc.clusters[cluster]; ok {
		nc.clusters[cluster].Lock.Lock()
		consumerMap := make(map[string]struct{})
		for _, group := range consumerList {
			consumerMap[group] = struct{}{}
			if _, ok := nc.clusters[cluster].Groups[group]; !ok {
				nc.clusters[cluster].Groups[group] = &consumerGroup{
					LastNotify: make(map[string]time.Time),
					LastEval:   time.Now().Add(-time.Duration(rand.Int63n(nc.minInterval*1000)) * time.Millisecond),
				}
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

func (nc *Coordinator) notifyModule(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventID string) {
	defer nc.running.Done()

	// Note - it is assumed that a read lock is already held when calling notifyModule
	cgroup, ok := nc.clusters[status.Cluster].Groups[status.Group]
	if !ok {
		// The group must have just been deleted
		return
	}

	// Closed incidents get sent regardless of the threshold for the module
	moduleName := module.GetName()
	if (!startTime.IsZero()) && (status.Status == protocol.StatusOK) && viper.GetBool("notifier."+moduleName+".send-close") {
		module.Notify(status, eventID, startTime, true)
		cgroup.LastNotify[module.GetName()] = time.Time{}
		return
	}

	// Only send a notification if the current status is above the module's threshold
	if int(status.Status) < viper.GetInt("notifier."+module.GetName()+".threshold") {
		return
	}

	// Only send the notification if it's been at least our Interval since the last one for this group
	currentTime := time.Now()
	if currentTime.Sub(cgroup.LastNotify[module.GetName()]) > (time.Duration(viper.GetInt("notifier."+moduleName+".send-interval")) * time.Second) {
		module.Notify(status, eventID, startTime, false)
		cgroup.LastNotify[module.GetName()] = currentTime
	}
}
