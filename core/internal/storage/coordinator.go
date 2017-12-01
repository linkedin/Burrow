/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package storage - Data storage subsystem.
// The storage subsystem receives information from the cluster and consumer subsystems and serves that information out
// to other subsystems on request.
//
// Modules
//
// Currently, only one module is provided:
//
// * inmemory - Store all information in a set of in-memory maps
package storage

import (
	"errors"
	"sync"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

// Module (storage) is responsible for maintaining all the broker and consumer offsets for all clusters that Burrow
// watches. It must accept and respond to all protocol.StorageRequest types. This interface conforms to the overall
// protocol.Module interface, but it adds a func to fetch the channel that the module is listening on for requests, so
// that requests can be forwarded to it by the coordinator.
type Module interface {
	protocol.Module
	GetCommunicationChannel() chan *protocol.StorageRequest
}

// Coordinator (storage) manages a single storage module (only one module is supported at this time), making sure it
// is configured, started, and stopped at the appropriate time. It is also responsible for listening to the
// StorageChannel that is provided in the application context and forwarding those requests to the storage module. If
// no storage module has been configured explicitly, the coordinator starts the inmemory module as a default.
type Coordinator struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	quitChannel chan struct{}
	modules     map[string]protocol.Module
	running     sync.WaitGroup
}

// getModuleForClass returns the correct module based on the passed className. As part of the Configure steps, if there
// is any error, it will panic with an appropriate message describing the problem.
func getModuleForClass(app *protocol.ApplicationContext, moduleName string, className string) Module {
	switch className {
	case "inmemory":
		return &InMemoryStorage{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "storage"),
				zap.String("class", className),
				zap.String("name", moduleName),
			),
		}
	default:
		panic("Unknown storage className provided: " + className)
	}
}

// Configure is called to create the configured storage module and call its Configure func to validate the
// configuration and set it up. The coordinator will panic is more than one module is configured, and if no modules have
// been configured, it will set up a default inmemory storage module. If there are any problems, it is expected that
// this func will panic with a descriptive error message, as configuration failures are not recoverable errors.
func (sc *Coordinator) Configure() {
	sc.Log.Info("configuring")
	sc.quitChannel = make(chan struct{})
	sc.modules = make(map[string]protocol.Module)
	sc.running = sync.WaitGroup{}

	modules := viper.GetStringMap("storage")
	switch len(modules) {
	case 0:
		// Create a default module
		viper.Set("storage.default.class-name", "inmemory")
		modules = viper.GetStringMap("storage")
	case 1:
		// Have one module. Just continue
		break
	default:
		panic("Only one storage module must be configured")
	}

	// Create all configured storage modules, add to list of storage
	for name := range modules {
		configRoot := "storage." + name
		module := getModuleForClass(sc.App, name, viper.GetString(configRoot+".class-name"))
		module.Configure(name, configRoot)
		sc.modules[name] = module
	}
}

// Start calls the storage module's underlying Start func. If the module Start returns an error, this func stops
// immediately and returns that error to the caller.
//
// We also start a request forwarder goroutine. This listens to the StorageChannel that is provided in the application
// context that all modules receive, and forwards those requests to the storage modules. At the present time, the
// storage subsystem only supports one module, so this is a simple "accept and forward".
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

// Stop calls the configured storage module's underlying Stop func. It is expected that the module Stop will not return
// until the module has been completely stopped. While an error can be returned, this func always returns no error, as
// a failure during stopping is not a critical failure
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
