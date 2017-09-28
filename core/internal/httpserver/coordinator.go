/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package httpserver

import (
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger
}

func (hc *Coordinator) Configure() {
	// Do HTTP server specific config steps here
}

func (hc *Coordinator) Start() error {
	// Start HTTP Server
	return nil
}

func (hc *Coordinator) Stop() error {
	// Do nothing for now
	return nil
}
