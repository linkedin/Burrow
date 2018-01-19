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
	"sync"

	"github.com/samuel/go-zookeeper/zk"
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

	// This is a ZookeeperClient that can be used by any coordinator or module in order to store metadata about their
	// operation, or to create locks. Any module that uses this client must honor the ZookeeperRoot, which is the root
	// path under which all ZNodes should be created.
	Zookeeper     ZookeeperClient
	ZookeeperRoot string

	// This is a boolean flag which is set or unset by the Zookeeper coordinator to signal when the connection is
	// established. It should be used in coordination with the ZookeeperExpired condition to monitor when the session
	// has expired. This indicates locks have been released or watches must be reset.
	ZookeeperConnected bool
	ZookeeperExpired   *sync.Cond

	// This is the channel over which any module should send a consumer group evaluation request. It is serviced by the
	// evaluator Coordinator, and passed to an appropriate evaluator module.
	EvaluatorChannel chan *EvaluatorRequest

	// This is the channel over which any module should send storage requests for storage of offsets and group
	// information, or to fetch the same information. It is serviced by the storage Coordinator.
	StorageChannel chan *StorageRequest
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

// ZookeeperClient is a minimal interface for working with a Zookeeper connection. We provide this interface, rather
// than using the underlying library directly, as it makes it easier to test code that uses Zookeeper. This interface
// should be expanded with additional methods as needed.
//
// Note that the interface is specified in the protocol package, rather than in the helper package or the zookeeper
// coordinator package, as it has to be referenced by ApplicationContext. Moving it elsewhere generates a dependency
// loop.
type ZookeeperClient interface {
	// Close the connection to Zookeeper
	Close()

	// For the given path in Zookeeper, return a slice of strings which list the immediate child nodes. This method also
	// sets a watch on the children of the specified path, providing an event channel that will receive a message when
	// the watch fires
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)

	// For the given path in Zookeeper, return the data in the node as a byte slice. This method also sets a watch on
	// the children of the specified path, providing an event channel that will receive a message when the watch fires
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)

	// For the given path in Zookeeper, return a boolean stating whether or not the node exists. This method also sets
	// a watch on the node (exists if it does not currently exist, or a data watch otherwise), providing an event
	// channel that will receive a message when the watch fires
	ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error)

	// Create makes a new ZNode at the specified path with the contents set to the data byte-slice. Flags can be
	// provided to specify that this is an ephemeral or sequence node, and an ACL must be provided. If no ACL is\
	// desired, specify
	//  zk.WorldACL(zk.PermAll)
	Create(string, []byte, int32, []zk.ACL) (string, error)

	// NewLock creates a lock using the provided path. Multiple Zookeeper clients, using the same lock path, can
	// synchronize with each other to assure that only one client has the lock at any point.
	NewLock(path string) ZookeeperLock
}

// ZookeeperLock is an interface for the operation of a lock in Zookeeper. Multiple Zookeeper clients, using the same
// lock path, can synchronize with each other to assure that only one client has the lock at any point.
type ZookeeperLock interface {
	// Lock acquires the lock, blocking until it is able to do so, and returns nil. If the lock cannot be acquired, such
	// as if the session has been lost, a non-nil error will be returned instead.
	Lock() error

	// Unlock releases the lock, returning nil. If there is an error releasing the lock, such as if it is not held, an
	// error is returned instead.
	Unlock() error
}
