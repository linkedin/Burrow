/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
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

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type Module interface {
	protocol.Module
	GetCommunicationChannel() chan *protocol.EvaluatorRequest
}

type RequestMessage protocol.EvaluatorRequest

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger
	quitChannel chan struct{}
	modules     map[string]protocol.Module
}

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

func (ec *Coordinator) Stop() error {
	ec.Log.Info("stopping")

	close(ec.quitChannel)

	// The individual storage modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(ec.modules)
	return nil
}
