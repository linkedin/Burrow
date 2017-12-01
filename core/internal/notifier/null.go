/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"regexp"
	"text/template"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

// NullNotifier is a no-op notifier that can be used for testing purposes in place of a mock. It does not make any
// external calls, and will record if specific funcs are called.
type NullNotifier struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name           string
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	extras         map[string]string
	templateOpen   *template.Template
	templateClose  *template.Template

	// CalledConfigure is set to true if the Configure method is called
	CalledConfigure bool

	// CalledStart is set to true if the Start method is called
	CalledStart bool

	// CalledStop is set to true if the Stop method is called
	CalledStop bool

	// CalledNotify is set to true if the Notify method is called
	CalledNotify bool

	// CalledAcceptConsumerGroup is set to true if the AcceptConsumerGroup method is called
	CalledAcceptConsumerGroup bool
}

// Configure sets the module name, but performs no other functions for the null notifier
func (module *NullNotifier) Configure(name string, configRoot string) {
	module.name = name
	module.CalledConfigure = true
}

// Start is a no-op for the null notifier. It always returns no error
func (module *NullNotifier) Start() error {
	module.CalledStart = true
	return nil
}

// Stop is a no-op for the null notifier. It always returns no error
func (module *NullNotifier) Stop() error {
	module.CalledStop = true
	return nil
}

// GetName returns the configured name of this module
func (module *NullNotifier) GetName() string {
	return module.name
}

// GetGroupWhitelist returns the compiled group whitelist (or nil, if there is not one)
func (module *NullNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

// GetGroupBlacklist returns the compiled group blacklist (or nil, if there is not one)
func (module *NullNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
}

// GetLogger returns the configured zap.Logger for this notifier
func (module *NullNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// AcceptConsumerGroup has no additional function for the null notifier, and so always returns true
func (module *NullNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	module.CalledAcceptConsumerGroup = true
	return true
}

// Notify is a no-op for the null notifier
func (module *NullNotifier) Notify(status *protocol.ConsumerGroupStatus, eventID string, startTime time.Time, stateGood bool) {
	module.CalledNotify = true
}
