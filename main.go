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
	//"os/signal"
	"runtime"
	//"syscall"
	"time"
)

type KafkaCluster struct {
	Client    *KafkaClient
	Zookeeper *ZookeeperClient
}

type StormCluster struct {
	Storm *StormClient
}

type ApplicationContext struct {
	Config       *BurrowConfig
	Storage      *OffsetStorage
	Clusters     map[string]*KafkaCluster
	Storms       map[string]*StormCluster
	Server       *HttpServer
	NotifyCenter *NotifyCenter
	NotifierLock *zk.Lock
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
	zkconn, _, err := zk.Connect(appContext.Config.Zookeeper.Hosts, time.Duration(appContext.Config.Zookeeper.Timeout)*time.Second)
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

		appContext.Clusters[cluster] = &KafkaCluster{Client: client, Zookeeper: zkconn}
	}

	// Start Storm Clients for each storm cluster
	appContext.Storms = make(map[string]*StormCluster, len(appContext.Config.Storm))
	for cluster, _ := range appContext.Config.Storm {
		log.Infof("Starting Storm client for cluster %s", cluster)
		stormClient, err := NewStormClient(appContext, cluster)
		if err != nil {
			log.Criticalf("Cannot start Storm client for cluster %s: %v", cluster, err)
			return 1
		}
		defer stormClient.Stop()

		appContext.Storms[cluster] = &StormCluster{Storm: stormClient}
	}

	// Set up the Zookeeper lock for notification
	appContext.NotifierLock = zk.NewLock(zkconn, appContext.Config.Zookeeper.LockPath, zk.WorldACL(zk.PermAll))

	// Load the notifiers, but do not start them
	err = LoadNotifiers(appContext)
	if err != nil {
		// Error was already logged
		return 1
	}

	// Notifiers are started in a goroutine if we get the ZK lock
	go StartNotifiers(appContext)
	defer StopNotifiers(appContext)

	// Register signal handlers for exiting
	exitChannel := make(chan os.Signal, 1)
	//signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM)

	// Wait until we're told to exit
	<-exitChannel
	log.Info("Shutdown triggered")
	return 0
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	rv := burrowMain()
	if rv != 0 {
		fmt.Println("Burrow failed at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
	} else {
		fmt.Println("Stopped Burrow at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
	}
	os.Exit(rv)
}
