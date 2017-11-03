/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package storage

import (
	"errors"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"sync"
)

type Module interface {
	protocol.Module
	GetCommunicationChannel() chan *protocol.StorageRequest
}

/* Module requests
 * No response:
 * Consumer offset (cluster, topic, partition, group, offset, ts)
 * Consumer deletion (cluster, group, topic=null)
 * Consumer topic deletion (cluster, topic, group, partition=0)
 * Broker offset (cluster, topic, partition, group=null, offset, ts?)
 * Broker topic deletion (cluster, topic, partition=0)
 *
 * Response:
 * Fetch cluster list
 * Fetch consumer list
 * Fetch consumer detail
 * Fetch topic list
 * Fetch topic detail (partitions/offsets)
 */

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger
	quitChannel	chan struct{}
	modules     map[string]protocol.Module
	running     sync.WaitGroup
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) Module {
	switch className {
	case "inmemory":
		return &InMemoryStorage{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "storage"),
				zap.String("name", "inmemory"),
			),
		}
	default:
		panic("Unknown storage className provided: " + className)
	}
}

func (sc *Coordinator) Configure() {
	sc.Log.Info("configuring")
	sc.quitChannel = make(chan struct{})
	sc.modules = make(map[string]protocol.Module)
	sc.running = sync.WaitGroup{}

	// Create all configured storage modules, add to list of storage
	if len(sc.App.Configuration.Storage) != 1 {
		panic("Only one storage module must be configured")
	}
	for name, config := range sc.App.Configuration.Storage {
		module := GetModuleForClass(sc.App, config.ClassName)
		module.Configure(name)
		sc.modules[name] = module
	}
}

func (sc *Coordinator) Start() error {
	sc.Log.Info("starting")

	// Start Storage modules
	err := helpers.StartCoordinatorModules(sc.modules)
	if err != nil {
		return errors.New("Error starting storage module: " + err.Error())
	}

	// Start request forwarder
	go sc.mainLoop()
	return nil
}

func (sc *Coordinator) Stop() error {
	sc.Log.Info("stopping")

	close(sc.quitChannel)
	sc.running.Wait()

	// The individual storage modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(sc.modules)
	return nil
}

func (sc *Coordinator) mainLoop() {
	sc.running.Add(1)
	defer sc.running.Done()

	// We only support 1 module right now, so only send to that module
	var channel chan *protocol.StorageRequest
	for _, module := range sc.modules {
		channel = module.(Module).GetCommunicationChannel()
	}

	for {
		select {
		case request := <-sc.App.StorageChannel:
			// Yes, this forwarder is silly. However, in the future we want to support multiple storage modules
			// concurrently. However, that will require implementing a router that properly handles sets and
			// fetches and makes sure only 1 module responds to fetches
			channel <- request
		case <-sc.quitChannel:
			return
		}
	}
}