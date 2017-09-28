/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package evaluator

import (
	"errors"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type RequestMessage protocol.EvaluatorRequest

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger
	quitChannel	chan struct{}
	modules     map[string]protocol.Module
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) protocol.Module {
	switch className {
	case "caching":
		return &CachingEvaluator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "evaluator"),
				zap.String("name", "caching"),
			),
		}
	default:
		panic("Unknown evaluator className provided: " + className)
	}
}

func (ec *Coordinator) Configure() {
	ec.quitChannel = make(chan struct{})
	ec.modules = make(map[string]protocol.Module)

	// Create all configured evaluator modules, add to list of evaluators
	if len(ec.App.Configuration.Cluster) == 0 {
		panic("At least one cluster module must be configured")
	}
	for name, config := range ec.App.Configuration.Evaluator {
		module := GetModuleForClass(ec.App, config.ClassName)
		module.Configure(name)
		ec.modules[name] = module
	}
}

func (ec *Coordinator) Start() error {
	// Start Evaluator modules
	err := helpers.StartCoordinatorModules(ec.modules)
	if err != nil {
		return errors.New("Error starting evaluator module: " + err.Error())
	}

	// Start request forwarder
	go func() {
		for {
			select {
			case request := <-ec.App.EvaluatorChannel:
				// Right now, send all requests to all evaluator modules. This means that if there is more than one
				// module configured, there may be multiple responses sent back. In the future we will want to route
				// requests, or have a config for which module gets requests
				for _, module := range ec.modules {
					module.GetCommunicationChannel() <- request
				}
			case <-ec.quitChannel:
				return
			}
		}
	}()

	return nil
}

func (ec *Coordinator) Stop() error {
	close(ec.quitChannel)

	// The individual storage modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(ec.modules)
	return nil
}
