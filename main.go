/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Burrow provides advanced Kafka Consumer Lag Checking.
// It is a monitoring companion for Apache Kafka that provides consumer lag checking as a service without the need for
// specifying thresholds. It monitors committed offsets for all consumers and calculates the status of those consumers
// on demand. An HTTP endpoint is provided to request status on demand, as well as provide other Kafka cluster
// information. There are also configurable notifiers that can send status out via email or HTTP calls to another
// service.
//
// CLI or Library
//
// Burrow is designed to be run as a standalone application (CLI), and this is what the main package provides. In some
// situations it may be better for you to wrap Burrow with another application - for example, in environments where you
// have your own application structure to provide configuration and logging. To this end, Burrow can also be used as a
// library within another app.
//
// When embedding Burrow, please refer to https://github.com/linkedin/Burrow/blob/master/main.go for details on what
// preparation should happen before starting it. This is the wrapper that provides the CLI interface. The main logic
// for Burrow is in the core package, while the protocol package provides some of the common interfaces that are used.
//
// Additional Documentation
//
// More documentation on Burrow, including configuration and HTTP requests, can be found at
// https://github.com/linkedin/Burrow/wiki
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/viper"

	"github.com/linkedin/Burrow/core"
)

// exitCode wraps a return value for the application
type exitCode struct{ Code int }

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(exitCode); ok {
			if exit.Code != 0 {
				fmt.Fprintln(os.Stderr, "Burrow failed at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
			} else {
				fmt.Fprintln(os.Stderr, "Stopped Burrow at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
			}

			os.Exit(exit.Code)
		}
		panic(e) // not an exitCode, bubble up
	}
}

func main() {
	// This makes sure that we panic and run defers correctly
	defer handleExit()

	runtime.GOMAXPROCS(runtime.NumCPU())

	// The only command line arg is the config file
	configPath := flag.String("config-dir", ".", "Directory that contains the configuration file")
	flag.Parse()

	// Load the configuration from the file
	viper.SetConfigName("burrow")
	viper.AddConfigPath(*configPath)
	fmt.Fprintln(os.Stderr, "Reading configuration from", *configPath)
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed reading configuration:", err.Error())
		panic(exitCode{1})
	}

	// setup viper to be able to read env variables with a configured prefix
	viper.SetDefault("general.env-var-prefix", "burrow")
	envPrefix := viper.GetString("general.env-var-prefix")
	viper.SetEnvPrefix(envPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()

	// Create the PID file to lock out other processes
	viper.SetDefault("general.pidfile", "burrow.pid")
	pidFile := viper.GetString("general.pidfile")
	if !core.CheckAndCreatePidFile(pidFile) {
		// Any error on checking or creating the PID file causes an immediate exit
		panic(exitCode{1})
	}
	defer core.RemovePidFile(pidFile)

	// Set up stderr/stdout to go to a separate log file, if enabled
	stdoutLogfile := viper.GetString("general.stdout-logfile")
	if stdoutLogfile != "" {
		core.OpenOutLog(stdoutLogfile)
	}

	// Register signal handlers for exiting
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// This triggers handleExit (after other defers), which will then call os.Exit properly
	panic(exitCode{core.Start(nil, exitChannel)})
}
