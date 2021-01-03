/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package protocol - Burrow types and interfaces.
// The protocol module provides the definitions for most of the common Burrow types and interfaces that are used in the
// rest of the application. The documentation here is primarily targeted at developers of Burrow modules, and not the
// end user.
package protocol

import (
	"go.uber.org/zap"
)

// ApplicationContext is a structure that holds objects that are used across all coordinators and modules. This is
// used in lieu of passing individual arguments to all functions.
type ApplicationContext struct {
	// Logger is a configured zap.Logger instance. It is to be used by the main routine directly, and the main routine
	// creates loggers for each of the coordinators to use that have fields set to identify that coordinator.
	//
	// This field can be set prior to calling core.Start() in order to pre-configure the logger. If it is not set,
	// core.Start() will set up a default logger using the application config.
	Logger *zap.Logger

	// LogLevel is an AtomicLevel instance that has been used to set the default level of the Logger. It is used to
	// dynamically adjust the logging level (such as via an HTTP call)
	//
	// If Logger has been set prior to calling core.Start(), LogLevel must be set as well.
	LogLevel *zap.AtomicLevel

	// This is used by the main Burrow routines to signal that the configuration is valid. The rest of the code should
	// not care about this, as the application will exit if the configuration is not valid.
	ConfigurationValid bool

	// This is the channel over which any module should send a consumer group evaluation request. It is serviced by the
	// evaluator Coordinator, and passed to an appropriate evaluator module.
	EvaluatorChannel chan *EvaluatorRequest

	// This is the channel over which any module should send storage requests for storage of offsets and group
	// information, or to fetch the same information. It is serviced by the storage Coordinator.
	StorageChannel chan *StorageRequest

	// This is a boolean flag which is set by the last subsystem, the consumer, in order to signal when Burrow is ready
	AppReady bool
}

// Module is a common interface for all modules so that they can be manipulated by the coordinators in the same way.
// The interface provides a way to configure the module, and then methods to start it and stop it safely. Each
// coordinator may have its own Module interface definition, as well, that adds specific requirements for that type of
// module.
//
// The struct that implements this interface is expected to have an App and Log literal, at the very least. The App
// literal will contain the protocol.ApplicationContext object with resources that the module may use. The Log literal
// will be set up with a logger that has fields set that identify the module. These are set up by the module's
// coordinator before Configure is called.
type Module interface {
	// Configure is called to initially set up the module. The name of the module, as well as the root string to be
	// used when looking up configurations with viper, are provided. In this func, the module must completely validate
	// it's own configuration, and panic if it is not correct. It may also set up data structures that are critical
	// for the module. It must NOT make any connections to resources outside of the module itself, including either
	// the storage or evaluator channels in the application context.
	Configure(name string, configRoot string)

	// Start is called to start the operation of the module. In this func, the module should make connections to
	// external resources and start operation. This func must return (any running code must be started as a goroutine).
	// If there is a problem starting up, the module should stop anything it has already started and return a non-nil
	// error.
	Start() error

	// Stop is called to stop operation of the module. In this func, the module should clean up any goroutines it has
	// started and close any external connections. While it can return an error if there is a problem, the errors are
	// mostly ignored.
	Stop() error
}

// Coordinator is a common interface for all subsystem coordinators so that the core routine can manage them in a
// consistent manner. The interface provides a way to configure the coordinator, and then methods to start it and stop
// it safely. It is expected that when any of these funcs are called, the coordinator will then call the corresponding
// func on its modules.
//
// The struct that implements this interface is expected to have an App and Log literal, at the very least. The App
// literal will contain the protocol.ApplicationContext object with resources that the coordinator and modules may use.
// The Log literal will be set up with a logger that has fields set that identify the coordinator. These are set up by
// the core routine before Configure is called. The coordinator can use Log to create the individual loggers for the
// modules it controls.
type Coordinator interface {
	// Configure is called to initially set up the coordinator. In this func, it should validate any configurations
	// that the coordinator requires, and then call the Configure func for each of its modules. If there are any errors
	// in configuration, it is expected that this call will panic. The coordinator may also set up data structures that
	// are critical for the subsystem, such as communication channels. It must NOT make any connections to resources
	// outside of the coordinator itself, including either the storage or evaluator channels in the application context.
	Configure()

	// Start is called to start the operation of the coordinator. In this func, the coordinator should call the Start
	// func for any of its modules, and then start any additional logic the coordinator needs to run. This func must
	// return (any running code must be started as a goroutine). If there is a problem starting up, the coordinator
	// should stop anything it has already started and return a non-nil error.
	Start() error

	// Stop is called to stop operation of the coordinator. In this func, the coordinator should call the Stop func for
	// any of its modules, and stop any goroutines that it has started. While it can return an error if there is a
	// problem, the errors are mostly ignored.
	Stop() error
}
