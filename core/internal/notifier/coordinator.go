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
)

type NotifierModule interface {
	protocol.Module
	AcceptConsumerGroup (*protocol.ConsumerGroupStatus) bool
	Notify (*protocol.ConsumerGroupStatus)
}

type ClusterGroups struct {
	Groups []string
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

	clusters          map[string]*ClusterGroups
	clusterLock       *sync.RWMutex
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) protocol.Module {
	switch className {
	case "http":
		return &HttpNotifier{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "http"),
			),
		}
	case "email":
		return &EmailNotifier{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "email"),
			),
		}
	case "slack":
		return &SlackNotifier{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "slack"),
			),
		}
	case "null":
		return &NullNotifier{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "notifier"),
				zap.String("name", "null"),
			),
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

	// Create all configured notifier modules, add to list of notifier
	for name, config := range nc.App.Configuration.Notifier {
		module := GetModuleForClass(nc.App, config.ClassName)
		module.Configure(name)
		nc.modules[name] = module

		if nc.App.Configuration.Notifier[name].Interval < nc.minInterval {
			nc.minInterval = nc.App.Configuration.Notifier[name].Interval
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
		for _, consumer := range consumerGroup.Groups {
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
				for _, genericModule := range nc.modules {
					module := genericModule.(NotifierModule)
					if module.AcceptConsumerGroup(response) {
						go func() {
							nc.running.Add(1)
							defer nc.running.Done()
							module.Notify(response)
						}()
					}
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
			nc.clusters[cluster].Groups = consumerList
			nc.clusters[cluster].Lock.Unlock()
		}
		nc.clusterLock.RUnlock()
	}
}
