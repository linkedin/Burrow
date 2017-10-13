/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
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
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/internal/storage"
	"github.com/linkedin/Burrow/core/internal/cluster"
	"github.com/linkedin/Burrow/core/internal/consumer"
	"github.com/linkedin/Burrow/core/internal/evaluator"
	"github.com/linkedin/Burrow/core/internal/httpserver"
	"github.com/linkedin/Burrow/core/internal/notifier"
	"github.com/linkedin/Burrow/core/configuration"
)

func NewZookeeperClient(app *protocol.ApplicationContext) (*zk.Conn, error) {
	// We need a function to set the logger for the ZK connection
	zkSetLogger := func(c *zk.Conn) {
		logger := app.Logger.With(
			zap.String("type", "coordinator"),
			zap.String("name", "zookeeper"),
		)
		c.SetLogger(zap.NewStdLog(logger))

	}

	// Start a local Zookeeper client (used for application locks)
	// NOTE - samuel/go-zookeeper does not support chroot, so we pass along the configured root path in config
	app.Logger.Info("Starting module",
		zap.String("type", "coordinator"),
		zap.String("name", "zookeeper"))

	zkConn, _, err := zk.Connect(app.Configuration.Zookeeper.Hosts,
		time.Duration(app.Configuration.Zookeeper.Timeout)*time.Second,
		zkSetLogger)

	if err != nil {
		app.Logger.Panic("Failure to start module",
			zap.String("type", "coordinator"),
			zap.String("name", "zookeeper"),
			zap.String("error", err.Error()))
		return nil, err
	}
	return zkConn, nil
}

func NewCoordinators(app *protocol.ApplicationContext) [6]protocol.Coordinator {
	// This order is important - it makes sure that the things taking requests start up before things sending requests
	return [6]protocol.Coordinator{
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

func Start(app *protocol.ApplicationContext, exitChannel chan os.Signal) int {
	var err error

	// Validate that the ApplicationContext is complete
	if (app == nil) || (app.Logger == nil) || (app.LogLevel == nil) {
		fmt.Fprintln(os.Stderr, "Burrow was called with an incomplete ApplicationContext")
		return 1
	}

	// Validate the configuration that was passed in
	if err := configuration.ValidateConfig(app.Configuration); err != nil {
		fmt.Fprintf(os.Stderr, "Cannot validate configuration: %v", err)
		return 1
	}

	// Set up a specific child logger for main
	log := app.Logger.With(zap.String("type", "main"), zap.String("name", "burrow"))

	// Start a local Zookeeper client (used for application locks)
	app.Zookeeper, err = NewZookeeperClient(app)
	if err != nil {
		return 1
	}
	app.ZookeeperRoot = app.Configuration.Zookeeper.RootPath
	defer app.Zookeeper.Close()

	// Set up an array of coordinators in the order they are to be loaded (and closed)
	coordinators := NewCoordinators(app)

	// Set up two main channels to use for the evaluator and storage coordinators. This is how burrow communicates
	// internally:
	//   * Consumers and Clusters send offsets to the storage coordinator to populate all the state information
	//   * The Notifiers send evaluation requests to the evaluator coordinator to check group status
	//   * The Evaluators send requests to the storage coordinator for group offset and lag information
	//   * The HTTP server sends requests to both the evaluator and storage coordinators to fulfill API requests
	app.EvaluatorChannel = make(chan *protocol.EvaluatorRequest)
	app.StorageChannel = make(chan *protocol.StorageRequest)

	// Configure the coordinators in order. Note that they will panic if there is a configuration problem
	for _, coordinator := range coordinators {
		coordinator.Configure()
	}

	// Start the coordinators in order
	for _, coordinator := range coordinators {
		coordinator.Start()
	}

	// Wait until we're told to exit
	<-exitChannel
	log.Info("Shutdown triggered")

	// Stop the coordinators in the reverse order. This assures that request senders are stopped before request servers
	for i := len(coordinators)-1; i >= 0; i-- {
		coordinators[i].Stop()
	}

	// Exit cleanly
	return 0
}
