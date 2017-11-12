/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package core

import (
	"fmt"
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
		&evaluator.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "evaluator"),
			),
		},
		&notifier.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "notifier"),
			),
		},
		&httpserver.Coordinator{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "coordinator"),
				zap.String("name", "httpserver"),
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

func Start(app *protocol.ApplicationContext, exitChannel chan os.Signal) int {
	// Validate that the ApplicationContext is complete
	if (app == nil) || (app.Logger == nil) || (app.LogLevel == nil) {
		fmt.Fprintln(os.Stderr, "Burrow was called with an incomplete ApplicationContext")
		return 1
	}

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
