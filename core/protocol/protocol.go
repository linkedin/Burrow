/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
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
	"encoding/json"
	"go.uber.org/zap"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/toddpalino/Burrow/core/configuration"
)

type ApplicationContext struct {
	// These fields need to be populated before Start is called
	Configuration    *configuration.Configuration
	Logger           *zap.Logger
	LogLevel         *zap.AtomicLevel

	// These fields will be created by Start after it is called
	Zookeeper        *zk.Conn
	ZookeeperRoot    string
	EvaluatorChannel chan interface{}
	StorageChannel   chan interface{}
}

type ConsumerOffset struct {
	Offset     int64 `json:"offset"`
	Timestamp  int64 `json:"timestamp"`
	Lag        int64 `json:"lag"`
	Artificial bool  `json:"-"`
	MaxOffset  int64 `json:"max_offset"`
}

func (c StatusConstant) String() string {
	if (c >= 0) && (c < StatusConstant(len(StatusStrings))) {
		return StatusStrings[c]
	} else {
		return "UNKNOWN"
	}
}
func (c StatusConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}
func (c StatusConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

/* The Module interface is used for all Burrow modules, including:
 *   - Consumer
 *   - Broker (cluster)
 *   - Evaluator
 *   - Notifier
 *   - Storage
 *
 * Because it is common, we pass all of the main communication channels to each one, as well
 * as the master Zookeeper client (which can be used for coordination when needed).
 */
type Module	interface {
	Configure(name string)
	GetCommunicationChannel() chan interface{}
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
