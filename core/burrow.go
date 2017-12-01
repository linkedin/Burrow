/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package core - Core Burrow logic.
// The core package is where all the internal logic for Burrow is located. It provides several helpers for setting up
// logging and application management (such as PID files), as well as the Start method that runs Burrow itself.
//
// The documentation for the rest of the internals, including all the available modules, is available at
// https://godoc.org/github.com/linkedin/Burrow/core/internal/?m=all. For the most part, end users of Burrow should not
// need to refer to this documentation, as it is targeted at developers of Burrow modules. Details on what modules are
// available and how to configure them are available at https://github.com/linkedin/Burrow/wiki
package core

import (
	"os"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/cluster"
	"github.com/linkedin/Burrow/core/internal/consumer"
	"github.com/linkedin/Burrow/core/internal/evaluator"
	"github.com/linkedin/Burrow/core/internal/httpserver"
	"github.com/linkedin/Burrow/core/internal/notifier"
	"github.com/linkedin/Burrow/core/internal/storage"
	"github.com/linkedin/Burrow/core/internal/zookeeper"
	"github.com/linkedin/Burrow/core/protocol"
)

func newCoordinators(app *protocol.ApplicationContext) [7]protocol.Coordinator {
	// This order is important - it makes sure that the things taking requests start up before things sending requests
	return [7]protocol.Coordinator{
		&zookeeper.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "zookeeper"),
			),
		},
		&storage.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "storage"),
			),
		},
		&evaluator.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "evaluator"),
			),
		},
		&httpserver.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "httpserver"),
			),
		},
		&notifier.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "notifier"),
			),
		},
		&cluster.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "cluster"),
			),
		},
		&consumer.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "consumer"),
			),
		},
	}
}

func configureCoordinators(app *protocol.ApplicationContext, coordinators [7]protocol.Coordinator) {
	// Configure methods are allowed to panic, as their errors are non-recoverable
	// Catch panics here and flag in the application context if we can't continue
	defer func() {
		if r := recover(); r != nil {
			app.Logger.Panic(r.(string))
			app.ConfigurationValid = false
		}
	}()

	// Configure the coordinators in order
	for _, coordinator := range coordinators {
		coordinator.Configure()
	}
	app.ConfigurationValid = true
}

// Start is called to start the Burrow application. This is exposed so that it is possible to use Burrow as a library
// from within another application. Prior to calling this func, the configuration must have been loaded by viper from
// some underlying source (e.g. a TOML configuration file, or explicitly set in code after reading from another source).
// This func will block upon being called.
//
// If the calling application would like to control logging, it can pass a pointer to an instantiated
// protocol.ApplicationContext struct that has the Logger and LogLevel fields set. Otherwise, Start will create a
// logger based on configurations in viper.
//
// exitChannel is a signal channel that is provided by the calling application in order to signal Burrow to shut down.
// Burrow does not currently check the signal type: if any message is received on the channel, or if the channel is
// closed, Burrow will exit and Start will return 0.
//
// Start will return a 1 on any failure, including invalid configurations or a failure to start Burrow modules.
func Start(app *protocol.ApplicationContext, exitChannel chan os.Signal) int {
	// Validate that the ApplicationContext is complete
	if (app == nil) || (app.Logger == nil) || (app.LogLevel == nil) {
		// Didn't get a valid ApplicationContext, so we'll set up our own, with the logger
		app = &protocol.ApplicationContext{}
		app.Logger, app.LogLevel = ConfigureLogger()
		defer app.Logger.Sync()
	}
	app.Logger.Info("Started Burrow")

	// Set up a specific child logger for main
	log := app.Logger.With(zap.String("type", "main"), zap.String("name", "burrow"))

	// Set up an array of coordinators in the order they are to be loaded (and closed)
	coordinators := newCoordinators(app)

	// Set up two main channels to use for the evaluator and storage coordinators. This is how burrow communicates
	// internally:
	//   * Consumers and Clusters send offsets to the storage coordinator to populate all the state information
	//   * The Notifiers send evaluation requests to the evaluator coordinator to check group status
	//   * The Evaluators send requests to the storage coordinator for group offset and lag information
	//   * The HTTP server sends requests to both the evaluator and storage coordinators to fulfill API requests
	app.EvaluatorChannel = make(chan *protocol.EvaluatorRequest)
	app.StorageChannel = make(chan *protocol.StorageRequest)

	// Configure coordinators and exit if anything fails
	configureCoordinators(app, coordinators)
	if !app.ConfigurationValid {
		return 1
	}

	// Start the coordinators in order
	for i, coordinator := range coordinators {
		err := coordinator.Start()
		if err != nil {
			// Reverse our way out, stopping coordinators, then exit
			for j := i - 1; j >= 0; j-- {
				coordinators[j].Stop()
			}
			return 1
		}
	}

	// Wait until we're told to exit
	<-exitChannel
	log.Info("Shutdown triggered")

	// Stop the coordinators in the reverse order. This assures that request senders are stopped before request servers
	for i := len(coordinators) - 1; i >= 0; i-- {
		coordinators[i].Stop()
	}

	// Exit cleanly
	return 0
}
