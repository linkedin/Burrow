/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package storage

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/linkedin/Burrow/core/protocol"
	"time"
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
	viper.Set("storage.test.class-name", "inmemory")
	viper.Set("cluster.testcluster.class-name", "kafka")
	viper.Set("cluster.testcluster.servers", []string{"broker1.example.com:1234"})

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

func TestCoordinator_Configure_NoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Reset()

	coordinator.Configure()

	assert.Lenf(t, coordinator.modules, 1, "Expected 1 module configured, not %v", len(coordinator.modules))
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("storage.anothertest.class-name", "inmemory")

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Start(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()
	coordinator.Start()

	// Best is to test a request that we know the response to
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	coordinator.App.StorageChannel <- request
	response := <-request.Reply

	assert.IsType(t, []string{}, response, "Expected response to be of type []string")
	val := response.([]string)
	assert.Len(t, val, 1, "One entry not returned")
	assert.Equalf(t, val[0], "testcluster", "Expected return value to be 'testcluster', not %v", val[0])

	_, ok := <-request.Reply
	assert.False(t, ok, "Expected channel to be closed")

	time.Sleep(10 * time.Millisecond)
	coordinator.Stop()
}

func TestCoordinator_MultipleRequests(t *testing.T) {
	coordinator := CoordinatorWithOffsets()
	coordinator.Stop()
}
