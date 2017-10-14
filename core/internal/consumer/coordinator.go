/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package consumer

import (
	"errors"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type Coordinator struct {
	App     *protocol.ApplicationContext
	Log     *zap.Logger
	modules map[string]protocol.Module
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) protocol.Module {
	switch className {
	case "kafka":
		return &KafkaClient{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "consumer"),
				zap.String("name", "kafka"),
			),
		}
	case "kafkazk":
		return &KafkaZkClient{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "consumer"),
				zap.String("name", "kafka_zk"),
			),
		}
	default:
		panic("Unknown consumer className provided: " + className)
	}
}

func (cc *Coordinator) Configure() {
	cc.modules = make(map[string]protocol.Module)

	// Create all configured consumer modules, add to list of consumers
	if len(cc.App.Configuration.Consumer) == 0 {
		panic("At least one consumer module must be configured")
	}
	for name, config := range cc.App.Configuration.Consumer {
		if _, ok := cc.App.Configuration.Cluster[cc.App.Configuration.Consumer[name].Cluster]; !ok {
			panic("Consumer '" + name + "' references an unknown cluster '" + cc.App.Configuration.Consumer[name].Cluster +"'")
		}
		module := GetModuleForClass(cc.App, config.ClassName)
		module.Configure(name)
		cc.modules[name] = module
	}
}

func (cc *Coordinator) Start() error {
	// Start Consumer modules
	err := helpers.StartCoordinatorModules(cc.modules)
	if err != nil {
		return errors.New("Error starting consumer module: " + err.Error())
	}
	return nil
}

func (cc *Coordinator) Stop() error {
	// The individual consumer modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(cc.modules)
	return nil
}
