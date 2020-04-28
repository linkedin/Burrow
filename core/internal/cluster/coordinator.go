/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package cluster - Kafka cluster subsystem.
// The cluster subsystem is responsible for getting topic and partition information, as well as current broker offsets,
// from Kafka clusters and sending that information to the storage subsystem. It does not handle any consumer group
// information.
//
// Modules
//
// Currently, the following modules are provided:
//
// * kafka - Fetch topic, partition, and offset information from a Kafka cluster
package cluster

import (
	"errors"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

// A "cluster" is a single Kafka cluster that is going to be monitored by Burrow. The cluster module is responsible for
// connecting to the Kafka cluster, monitoring the topic list, and periodically fetching the broker end offset (latest
// offset) for each partition. This information is sent to the storage subsystem, where it can be retrieved by the
// evaluator and HTTP server.

// Coordinator manages all cluster modules, making sure they are configured, started, and stopped at the appropriate
// time.
type Coordinator struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	modules map[string]protocol.Module
}

// getModuleForClass returns the correct module based on the passed className. As part of the Configure steps, if there
// is any error, it will panic with an appropriate message describing the problem.
func getModuleForClass(app *protocol.ApplicationContext, moduleName string, className string) protocol.Module {
	switch className {
	case "kafka":
		return &KafkaCluster{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "cluster"),
				zap.String("class", className),
				zap.String("name", moduleName),
			),
		}
	default:
		panic("Unknown cluster className provided: " + className)
	}
}

// Configure is called to create each of the configured cluster modules and call their Configure funcs to validate
// their individual configurations and set them up. If there are any problems, it is expected that these funcs will
// panic with a descriptive error message, as configuration failures are not recoverable errors.
func (bc *Coordinator) Configure() {
	bc.Log.Info("configuring")

	bc.modules = make(map[string]protocol.Module)

	// Create all configured cluster modules, add to list of clusters
	modules := viper.GetStringMap("cluster")
	for name := range modules {
		configRoot := "cluster." + name
		module := getModuleForClass(bc.App, name, viper.GetString(configRoot+".class-name"))
		module.Configure(name, configRoot)
		bc.modules[name] = module
	}
}

// Start calls each of the configured cluster modules' underlying Start funcs. As the coordinator itself has no ongoing
// work to do, it does not start any other goroutines. If any module Start returns an error, this func stops immediately
// and returns that error to the caller. No further modules will be loaded after that.
func (bc *Coordinator) Start() error {
	bc.Log.Info("starting")

	// Start Cluster modules
	err := helpers.StartCoordinatorModules(bc.modules)
	if err != nil {
		return errors.New("Error starting cluster module: " + err.Error())
	}
	return nil
}

// Stop calls each of the configured cluster modules' underlying Stop funcs. It is expected that the module Stop will
// not return until the module has been completely stopped. While an error can be returned, this func always returns no
// error, as a failure during stopping is not a critical failure
func (bc *Coordinator) Stop() error {
	bc.Log.Info("stopping")

	// The individual cluster modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(bc.modules)
	return nil
}
