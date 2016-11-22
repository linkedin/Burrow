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
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	log "github.com/cihub/seelog"
	"github.com/prasincs/Burrow/burrow"
	"github.com/samuel/go-zookeeper/zk"
)

func createPidFile(filename string) {
	// Create a PID file, making sure it doesn't already exist
	pidfile, err := os.OpenFile(filename, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Criticalf("Cannot write PID file: %v", err)
		os.Exit(1)
	}
	fmt.Fprintf(pidfile, "%v", os.Getpid())
	pidfile.Close()
}

func removePidFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		fmt.Printf("Failed to remove PID file: %v\n", err)
	}
}

func openOutLog(filename string) *os.File {
	// Move existing out file to a dated file if it exists
	if _, err := os.Stat(filename); err == nil {
		if err = os.Rename(filename, filename+"."+time.Now().Format("2006-01-02_15:04:05")); err != nil {
			log.Criticalf("Cannot move old out file: %v", err)
			os.Exit(1)
		}
	}

	// Redirect stdout and stderr to out file
	logFile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	syscall.Dup2(int(logFile.Fd()), 1)
	syscall.Dup2(int(logFile.Fd()), 2)
	return logFile
}

// Why two mains? Golang doesn't let main() return, which means defers will not run.
// So we do everything in a separate main, that way we can easily exit out with an error code and still run defers
func burrowMain() int {
	// The only command line arg is the config file
	var cfgfile = flag.String("config", "burrow.cfg", "Full path to the configuration file")
	flag.Parse()

	// Load and validate the configuration
	fmt.Fprintln(os.Stderr, "Reading configuration from", *cfgfile)
	appContext := &burrow.ApplicationContext{Config: burrow.ReadConfig(*cfgfile)}
	if err := burrow.ValidateConfig(appContext); err != nil {
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
		burrow.NewLogger(appContext.Config.General.LogConfig)
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
	appContext.Storage, err = burrow.NewOffsetStorage(appContext)
	if err != nil {
		log.Criticalf("Cannot configure offsets storage module: %v", err)
		return 1
	}
	defer appContext.Storage.Stop()

	// Start an HTTP server
	log.Info("Starting HTTP server")
	appContext.Server, err = burrow.NewHttpServer(appContext)
	if err != nil {
		log.Criticalf("Cannot start HTTP server: %v", err)
		return 1
	}
	defer appContext.Server.Stop()

	// Start Kafka clients and Zookeepers for each cluster
	appContext.Clusters = make(map[string]*burrow.KafkaCluster, len(appContext.Config.Kafka))
	for cluster, _ := range appContext.Config.Kafka {
		log.Infof("Starting Zookeeper client for cluster %s", cluster)
		zkconn, err := burrow.NewZookeeperClient(appContext, cluster)
		if err != nil {
			log.Criticalf("Cannot start Zookeeper client for cluster %s: %v", cluster, err)
			return 1
		}
		defer zkconn.Stop()

		log.Infof("Starting Kafka client for cluster %s", cluster)
		client, err := burrow.NewKafkaClient(appContext, cluster)
		if err != nil {
			log.Criticalf("Cannot start Kafka client for cluster %s: %v", cluster, err)
			return 1
		}
		defer client.Stop()

		appContext.Clusters[cluster] = &burrow.KafkaCluster{Client: client, Zookeeper: zkconn}
	}

	// Start Storm Clients for each storm cluster
	appContext.Storms = make(map[string]*burrow.StormCluster, len(appContext.Config.Storm))
	for cluster, _ := range appContext.Config.Storm {
		log.Infof("Starting Storm client for cluster %s", cluster)
		stormClient, err := burrow.NewStormClient(appContext, cluster)
		if err != nil {
			log.Criticalf("Cannot start Storm client for cluster %s: %v", cluster, err)
			return 1
		}
		defer stormClient.Stop()

		appContext.Storms[cluster] = &burrow.StormCluster{Storm: stormClient}
	}

	// Set up the Zookeeper lock for notification
	appContext.NotifierLock = zk.NewLock(zkconn, appContext.Config.Zookeeper.LockPath, zk.WorldACL(zk.PermAll))

	// Load the notifiers, but do not start them
	err = burrow.LoadNotifiers(appContext)
	if err != nil {
		// Error was already logged
		return 1
	}

	// Notifiers are started in a goroutine if we get the ZK lock
	go burrow.StartNotifiers(appContext)
	defer burrow.StopNotifiers(appContext)

	// Register signal handlers for exiting
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM)

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
