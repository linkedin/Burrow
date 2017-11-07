/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package cluster

import (
	"errors"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/internal/helpers"
)

type Coordinator struct {
	App     *protocol.ApplicationContext
	Log     *zap.Logger
	modules map[string]protocol.Module
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) protocol.Module {
	switch className {
	case "kafka":
		return &KafkaCluster{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "cluster"),
				zap.String("name", "kafka"),
			),
		}
	default:
		panic("Unknown cluster className provided: " + className)
	}
}

func (bc *Coordinator) Configure() {
	bc.Log.Info("configuring")

	bc.modules = make(map[string]protocol.Module)

	// Create all configured cluster modules, add to list of clusters
	if len(bc.App.Configuration.Cluster) == 0 {
		panic("At least one cluster module must be configured")
	}
	for name, config := range bc.App.Configuration.Cluster {
		module := GetModuleForClass(bc.App, config.ClassName)
		module.Configure(name)
		bc.modules[name] = module
	}
}

func (bc *Coordinator) Start() error {
	bc.Log.Info("starting")

	// Start Cluster modules
	err := helpers.StartCoordinatorModules(bc.modules)
	if err != nil {
		return errors.New("Error starting cluster module: " + err.Error())
	}
	return nil
}

func (bc *Coordinator) Stop() error {
	bc.Log.Info("stopping")

	// The individual cluster modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(bc.modules)
	return nil
}
