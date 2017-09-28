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
	default:
		panic("Unknown notifier className provided: " + className)
	}
}

func (nc *Coordinator) Configure() {
	nc.quitChannel = make(chan struct{})
	nc.modules = make(map[string]protocol.Module)

	// Create all configured notifier modules, add to list of notifier
	for name, config := range nc.App.Configuration.Evaluator {
		module := GetModuleForClass(nc.App, config.ClassName)
		module.Configure(name)
		nc.modules[name] = module
	}
}

func (nc *Coordinator) Start() error {
	// Start Evaluator modules
	err := helpers.StartCoordinatorModules(nc.modules)
	if err != nil {
		return errors.New("Error starting notifier module: " + err.Error())
	}

	// Start request forwarder
	go func() {
		for {
			select {
			case request := <-nc.App.EvaluatorChannel:
				// Right now, send all requests to all evaluator modules. This means that if there is more than one
				// module configured, there may be multiple responses sent back. In the future we will want to route
				// requests, or have a config for which module gets requests
				for _, module := range nc.modules {
					module.GetCommunicationChannel() <- request
				}
			case <-nc.quitChannel:
				return
			}
		}
	}()

	return nil
}

func (nc *Coordinator) Stop() error {
	close(nc.quitChannel)

	// The individual notifier modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(nc.modules)
	return nil
}
