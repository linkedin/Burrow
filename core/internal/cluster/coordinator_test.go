/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package cluster

import (
	"testing"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func fixtureCoordinator() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger:         zap.NewNop(),
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	viper.Reset()
	viper.Set("client-profile..client-id", "testid")
	viper.Set("cluster.test.class-name", "kafka")
	viper.Set("cluster.test.servers", []string{"broker1.example.com:1234"})

	return &coordinator
}

func TestCoordinator_ImplementsCoordinator(t *testing.T) {
	assert.Implements(t, (*protocol.Coordinator)(nil), new(Coordinator))
}

func TestCoordinator_Configure(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	assert.Lenf(t, coordinator.modules, 1, "Expected 1 module configured, not %v", len(coordinator.modules))
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("cluster.anothertest.class-name", "kafka")
	viper.Set("cluster.anothertest.servers", []string{"broker1.example.com:1234"})
	coordinator.Configure()
}

func TestCoordinator_StartStop(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Swap out the coordinator modules with a mock for testing
	mockModule := &helpers.MockModule{}
	mockModule.On("Start").Return(nil)
	mockModule.On("Stop").Return(nil)
	coordinator.modules["test"] = mockModule

	coordinator.Start()
	mockModule.AssertCalled(t, "Start")

	coordinator.Stop()
	mockModule.AssertCalled(t, "Stop")
}
