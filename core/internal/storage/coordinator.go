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
)

type OffsetMessage protocol.PartitionOffset
type FetchMessage protocol.StorageFetchRequest

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
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) protocol.Module {
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
	sc.quitChannel = make(chan struct{})
	sc.modules = make(map[string]protocol.Module)

	// Create all configured storage modules, add to list of storage
	if len(sc.App.Configuration.Storage) == 0 {
		panic("At least one storage module must be configured")
	}
	for name, config := range sc.App.Configuration.Storage {
		module := GetModuleForClass(sc.App, config.ClassName)
		module.Configure(name)
		sc.modules[name] = module
	}
}

func (sc *Coordinator) Start() error {
	// Start Storage modules
	err := helpers.StartCoordinatorModules(sc.modules)
	if err != nil {
		return errors.New("Error starting storage module: " + err.Error())
	}

	// Start request forwarder
	go func() {
		for {
			select {
			case request := <-sc.App.StorageChannel:
				// Right now, send all requests to all storage modules. This means that for fetch requests, if there
				// is more than one module configured, there may be multiple responses sent back. In the future we
				// will want to route requests, or have a config for which module gets fetch requests
				for _, module := range sc.modules {
					module.GetCommunicationChannel() <- request
				}
			case <-sc.quitChannel:
				return
			}
		}
	}()

	return nil
}

func (sc *Coordinator) Stop() error {
	close(sc.quitChannel)

	// The individual storage modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(sc.modules)
	return nil
}
