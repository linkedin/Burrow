/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package evaluator - Group evaluation subsystem.
// The evaluator subsystem is responsible for fetching group information from the storage subsystem and calculating the
// group's status based on that. It responds to EvaluatorRequest objects that are send via a channel, and replies with
// a ConsumerGroupStatus.
//
// Modules
//
// Currently, only one module is provided:
//
// * caching - Evaluate a consumer group and cache the results in memory for a short period of time
package evaluator

import (
	"errors"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

// Module is responsible for answering requests to evaluate the status of a consumer group. It fetches offset
// information from the storage subsystem and transforms that into a protocol.ConsumerGroupStatus response. It conforms
// to the overall protocol.Module interface, but it adds a func to fetch the channel that the module is listening on for
// requests, so that requests can be forwarded to it by the coordinator
type Module interface {
	protocol.Module
	GetCommunicationChannel() chan *protocol.EvaluatorRequest
}

// Coordinator manages a single evaluator module (only one module is supported at this time), making sure it is
// configured, started, and stopped at the appropriate time. It is also responsible for listening to the
// EvaluatorChannel that is provided in the application context and forwarding those requests to the evaluator module.
// If no evaluator module has been configured explicitly, the coordinator starts the caching module as a default.
type Coordinator struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	quitChannel chan struct{}
	modules     map[string]protocol.Module
}

// getModuleForClass returns the correct module based on the passed className. As part of the Configure steps, if there
// is any error, it will panic with an appropriate message describing the problem.
func getModuleForClass(app *protocol.ApplicationContext, moduleName string, className string) protocol.Module {
	switch className {
	case "caching":
		return &CachingEvaluator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "evaluator"),
				zap.String("class", className),
				zap.String("name", moduleName),
			),
		}
	default:
		panic("Unknown evaluator className provided: " + className)
	}
}

// Configure is called to create the configured evaluator module and call its Configure func to validate the
// configuration and set it up. The coordinator will panic is more than one module is configured, and if no modules have
// been configured, it will set up a default caching evaluator module. If there are any problems, it is expected that
// this func will panic with a descriptive error message, as configuration failures are not recoverable errors.
func (ec *Coordinator) Configure() {
	ec.Log.Info("configuring")

	ec.quitChannel = make(chan struct{})
	ec.modules = make(map[string]protocol.Module)

	modules := viper.GetStringMap("evaluator")
	switch len(modules) {
	case 0:
		// Create a default module
		viper.Set("evaluator.default.class-name", "caching")
		modules = viper.GetStringMap("evaluator")
	case 1:
		// Have one module. Just continue
		break
	default:
		panic("Only one evaluator module must be configured")
	}

	// Create all configured evaluator modules, add to list of evaluators
	for name := range modules {
		configRoot := "evaluator." + name
		module := getModuleForClass(ec.App, name, viper.GetString(configRoot+".class-name"))
		module.Configure(name, configRoot)
		ec.modules[name] = module
	}
}

// Start calls the evaluator module's underlying Start func. If the module Start returns an error, this func stops
// immediately and returns that error to the caller.
//
// We also start a request forwarder goroutine. This listens to the EvaluatorChannel that is provided in the application
// context that all modules receive, and forwards those requests to the evaluator modules. At the present time, the
// evaluator only supports one module, so this is a simple "accept and forward".
func (ec *Coordinator) Start() error {
	ec.Log.Info("starting")

	// Start Evaluator modules
	err := helpers.StartCoordinatorModules(ec.modules)
	if err != nil {
		return errors.New("Error starting evaluator module: " + err.Error())
	}

	// Start request forwarder
	go func() {
		// We only support 1 module right now, so only send to that module
		var channel chan *protocol.EvaluatorRequest
		for _, module := range ec.modules {
			channel = module.(Module).GetCommunicationChannel()
		}

		for {
			select {
			case request := <-ec.App.EvaluatorChannel:
				// Yes, this forwarder is silly. However, in the future we want to support multiple evaluator modules
				// concurrently. However, that will require implementing a router that properly handles requests and
				// makes sure that only 1 evaluator responds
				channel <- request
			case <-ec.quitChannel:
				return
			}
		}
	}()

	return nil
}

// Stop calls the configured evaluator module's underlying Stop func. It is expected that the module Stop will not
// return until the module has been completely stopped. While an error can be returned, this func always returns no
// error, as a failure during stopping is not a critical failure
func (ec *Coordinator) Stop() error {
	ec.Log.Info("stopping")

	close(ec.quitChannel)

	// The individual storage modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(ec.modules)
	return nil
}
