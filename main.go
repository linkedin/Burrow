/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"flag"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type KafkaCluster struct {
	Client    *KafkaClient
	Zookeeper *ZookeeperClient
}

type ApplicationContext struct {
	Config       *BurrowConfig
	Storage      *OffsetStorage
	Clusters     map[string]*KafkaCluster
	Server       *HttpServer
	Emailer      *Emailer
	HttpNotifier *HttpNotifier
	NotifierLock *zk.Lock
}

func loadNotifiers(app *ApplicationContext) error {
	// Set up the Emailer, if configured
	if len(app.Config.Email) > 0 {
		log.Info("Configuring Email notifier")
		emailer, err := NewEmailer(app)
		if err != nil {
			log.Criticalf("Cannot configure email notifier: %v", err)
			return err
		}
		app.Emailer = emailer
	}

	// Set up the HTTP Notifier, if configured
	if app.Config.Httpnotifier.Url != "" {
		log.Info("Configuring HTTP notifier")
		httpnotifier, err := NewHttpNotifier(app)
		if err != nil {
			log.Criticalf("Cannot configure HTTP notifier: %v", err)
			return err
		}
		app.HttpNotifier = httpnotifier
	}

	return nil
}

func startNotifiers(app *ApplicationContext) {
	// Do not proceed until we get the Zookeeper lock
	err := app.NotifierLock.Lock()
	if err != nil {
		log.Criticalf("Cannot get ZK notifier lock: %v", err)
		os.Exit(1)
	}
	log.Info("Acquired Zookeeper notifier lock")

	if app.Emailer != nil {
		log.Info("Starting Email notifier")
		app.Emailer.Start()
	}
	if app.HttpNotifier != nil {
		log.Info("Starting HTTP notifier")
		app.HttpNotifier.Start()
	}
}

func stopNotifiers(app *ApplicationContext) {
	// Ignore errors on unlock - we're quitting anyways, and it might not be locked
	app.NotifierLock.Unlock()

	if app.Emailer != nil {
		log.Info("Stopping Email notifier")
		app.Emailer.Stop()
	}
	if app.HttpNotifier != nil {
		log.Info("Stopping HTTP notifier")
		app.HttpNotifier.Stop()
	}
}

// Why two mains? Golang doesn't let main() return, which means defers will not run.
// So we do everything in a separate main, that way we can easily exit out with an error code and still run defers
func burrowMain() int {
	// The only command line arg is the config file
	var cfgfile = flag.String("config", "burrow.cfg", "Full path to the configuration file")
	flag.Parse()

	// Load and validate the configuration
	fmt.Fprintln(os.Stderr, "Reading configuration from", *cfgfile)
	appContext := &ApplicationContext{Config: ReadConfig(*cfgfile)}
	if err := ValidateConfig(appContext); err != nil {
		log.Criticalf("Cannot validate configuration: %v", err)
		return 1
	}

	// Create the PID file to lock out other processes. Defer removal so it's the last thing to go
	createPidFile(appContext.Config.General.LogDir + "/" + appContext.Config.General.PIDFile)
	defer removePidFile(appContext.Config.General.LogDir + "/" + appContext.Config.General.PIDFile)

	// Set up stderr/stdout to go to a separate log file
	openOutLog(appContext.Config.General.LogDir + "/burrow.out")
	fmt.Println("Started Burrow at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))

	// If a logging config is specified, replace the existing loggers
	if appContext.Config.General.LogConfig != "" {
		NewLogger(appContext.Config.General.LogConfig)
	}

	// Start a local Zookeeper client (used for application locks)
	log.Info("Starting Zookeeper client")
	zkhosts := make([]string, len(appContext.Config.Zookeeper.Hosts))
	for i, host := range appContext.Config.Zookeeper.Hosts {
		zkhosts[i] = fmt.Sprintf("%s:%v", host, appContext.Config.Zookeeper.Port)
	}
	zkconn, _, err := zk.Connect(zkhosts, time.Duration(appContext.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		log.Criticalf("Cannot start Zookeeper client: %v", err)
		return 1
	}
	defer zkconn.Close()

	// Start an offsets storage module
	log.Info("Starting Offsets Storage module")
	appContext.Storage, err = NewOffsetStorage(appContext)
	if err != nil {
		log.Criticalf("Cannot configure offsets storage module: %v", err)
		return 1
	}
	defer appContext.Storage.Stop()

	// Start an HTTP server
	log.Info("Starting HTTP server")
	appContext.Server, err = NewHttpServer(appContext)
	if err != nil {
		log.Criticalf("Cannot start HTTP server: %v", err)
		return 1
	}
	defer appContext.Server.Stop()

	// Start Kafka clients and Zookeepers for each cluster
	appContext.Clusters = make(map[string]*KafkaCluster, len(appContext.Config.Kafka))
	for cluster, _ := range appContext.Config.Kafka {
		log.Infof("Starting Zookeeper client for cluster %s", cluster)
		zkconn, err := NewZookeeperClient(appContext, cluster)
		if err != nil {
			log.Criticalf("Cannot start Zookeeper client for cluster %s: %v", cluster, err)
			return 1
		}
		defer zkconn.Stop()

		log.Infof("Starting Kafka client for cluster %s", cluster)
		client, err := NewKafkaClient(appContext, cluster)
		if err != nil {
			log.Criticalf("Cannot start Kafka client for cluster %s: %v", cluster, err)
			return 1
		}
		defer client.Stop()

		zkOffsetPaths := appContext.Config.Kafka[cluster].ZookeeperOffsetPaths
		if zkOffsetPaths != nil {
			log.Infof("Starting Kafka offset client for cluster %s. Offset paths is %s", cluster, zkOffsetPaths)
			zkOffsetClient, err := NewZooKeeperOffsetClient(appContext, cluster)
			if err != nil {
				log.Criticalf("Cannot start Kafka offset client for cluster %s: %v", cluster, err)
				return 1
			}
			defer zkOffsetClient.Stop()
		}

		appContext.Clusters[cluster] = &KafkaCluster{Client: client, Zookeeper: zkconn}
	}

	// Set up the Zookeeper lock for notification
	appContext.NotifierLock = zk.NewLock(zkconn, appContext.Config.Zookeeper.LockPath, zk.WorldACL(zk.PermAll))

	// Load the notifiers, but do not start them
	err = loadNotifiers(appContext)
	if err != nil {
		// Error was already logged
		return 1
	}

	// Notifiers are started in a goroutine if we get the ZK lock
	go startNotifiers(appContext)
	defer stopNotifiers(appContext)

	// Register signal handlers for exiting
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM)

	// Wait until we're told to exit
	<-exitChannel
	log.Info("Shutdown triggered")
	return 0
}

func main() {
	rv := burrowMain()
	if rv != 0 {
		fmt.Println("Burrow failed at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
	} else {
		fmt.Println("Stopped Burrow at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
	}
	os.Exit(rv)
}
