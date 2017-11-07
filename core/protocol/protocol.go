/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package protocol

import (
	"sync"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
)

type ApplicationContext struct {
	// These fields need to be populated before Start is called
	Configuration      *configuration.Configuration
	ConfigurationValid bool
	Logger             *zap.Logger
	LogLevel           *zap.AtomicLevel

	// These fields will be created by Start after it is called
	Zookeeper          ZookeeperClient
	ZookeeperRoot      string
	ZookeeperConnected bool
	ZookeeperExpired   *sync.Cond
	EvaluatorChannel   chan *EvaluatorRequest
	StorageChannel     chan *StorageRequest
}

/* The Module interface is used for all Burrow modules, including:
 *   - Consumer
 *   - Broker (cluster)
 *   - Evaluator
 *   - Notifier
 *   - Storage
 *
 * Module structs must have an App and Log literal, and should store their own name. The Log will get set up by
 * the coordinator with the proper fields to start with.
 */
type Module interface {
	Configure(name string)
	Start() error
	Stop() error
}

/* The Coordinator is responsible for managing a bunch of Modules. All coordinators have a common interface, which
 * simplifies the main logic. They also must have an App and Log literal, which will be set by the main routine before
 * calling Configure
 */
type Coordinator interface {
	Configure()
	Start() error
	Stop() error
}

// Zookeeper client interface
// Note that this is a minimal interface for what Burrow needs - add functions as necessary
type ZookeeperClient interface {
	Close()
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	Create(string, []byte, int32, []zk.ACL) (string, error)
	NewLock(path string) ZookeeperLock
}

type ZookeeperLock interface {
	Lock() error
	Unlock() error
}
