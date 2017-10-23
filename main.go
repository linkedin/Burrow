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

	"github.com/linkedin/Burrow/core"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
)

// exit code handler
type Exit struct{ Code int }
func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(Exit); ok == true {
			if exit.Code != 0 {
				fmt.Fprintln(os.Stderr, "Burrow failed at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
			} else {
				fmt.Fprintln(os.Stderr, "Stopped Burrow at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
			}

			os.Exit(exit.Code)
		}
		panic(e) // not an Exit, bubble up
	}
}

func main() {
	// This makes sure that we panic and run defers correctly
	defer handleExit()

	runtime.GOMAXPROCS(runtime.NumCPU())

	// The only command line arg is the config file
	var cfgfile = flag.String("config", "burrow.cfg", "Full path to the configuration file")
	flag.Parse()

	// Load the configuration from the file
	fmt.Fprintln(os.Stderr, "Reading configuration from", *cfgfile)
	appContext := &protocol.ApplicationContext{
		Configuration: configuration.ReadConfig(*cfgfile),
	}
	// Create the PID file to lock out other processes
	if ! core.CheckAndCreatePidFile(appContext.Configuration.General.PIDFile) {
		// Any error on checking or creating the PID file causes an immediate exit
		panic(Exit{1})
	}
	defer core.RemovePidFile(appContext.Configuration.General.PIDFile)

	// Set up stderr/stdout to go to a separate log file, if enabled
	if appContext.Configuration.General.StdoutLogfile != "" {
		core.OpenOutLog(appContext.Configuration.General.StdoutLogfile)
	}

	// Set up the logger
	appContext.Logger, appContext.LogLevel = core.ConfigureLogger(appContext.Configuration)
	defer appContext.Logger.Sync()
	appContext.Logger.Info("Started Burrow")

	// Register signal handlers for exiting
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// This triggers handleExit (after other defers), which will then call os.Exit properly
	panic(Exit{core.Start(appContext, exitChannel)})
}
