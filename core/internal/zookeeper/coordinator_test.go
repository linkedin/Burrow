// +build !race

/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package zookeeper

import (
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureCoordinator() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger: zap.NewNop(),
	}

	viper.Reset()
	viper.Set("zookeeper.root-path", "/test/path/burrow")
	viper.Set("zookeeper.servers", []string{"zk.example.com:2181"})
	viper.Set("zookeeper.timeout", 5)

	return &coordinator
}

func TestCoordinator_ImplementsCoordinator(t *testing.T) {
	assert.Implements(t, (*protocol.Coordinator)(nil), new(Coordinator))
}

func TestCoordinator_Configure(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	assert.NotNil(t, coordinator.connectFunc, "Expected connectFunc to get set")
}

func TestCoordinator_StartStop(t *testing.T) {
	coordinator := fixtureCoordinator()

	// mock the connectFunc to return a mock client
	mockClient := helpers.MockZookeeperClient{}
	eventChan := make(chan zk.Event)
	coordinator.connectFunc = func(servers []string, timeout time.Duration, logger *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error) {
		return &mockClient, eventChan, nil
	}

	mockClient.On("Create", "/test", []byte{}, int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrNodeExists)
	mockClient.On("Create", "/test/path", []byte{}, int32(0), zk.WorldACL(zk.PermAll)).Return("", zk.ErrNodeExists)
	mockClient.On("Create", "/test/path/burrow", []byte{}, int32(0), zk.WorldACL(zk.PermAll)).Return("", nil)
	mockClient.On("Close").Run(func(args mock.Arguments) { close(eventChan) }).Return()

	coordinator.Configure()
	err := coordinator.Start()
	assert.Nil(t, err, "Expected Start to not return an error")
	assert.Equal(t, &mockClient, coordinator.App.Zookeeper, "Expected App.Zookeeper to be set to the mock client")
	assert.Equalf(t, "/test/path/burrow", coordinator.App.ZookeeperRoot, "Expected App.ZookeeperRoot to be /test/path/burrow, not %v", coordinator.App.ZookeeperRoot)
	assert.True(t, coordinator.App.ZookeeperConnected, "Expected App.ZookeeperConnected to be true")
	assert.NotNil(t, coordinator.App.ZookeeperExpired, "Expected App.ZookeeperExpired to be set")

	err = coordinator.Stop()
	assert.Nil(t, err, "Expected Stop to not return an error")
}

func TestCoordinator_mainLoop(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.running = sync.WaitGroup{}
	coordinator.App.ZookeeperConnected = true
	coordinator.App.ZookeeperExpired = &sync.Cond{L: &sync.Mutex{}}

	eventChan := make(chan zk.Event)
	go coordinator.mainLoop(eventChan)

	// Nothing should change
	eventChan <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateDisconnected,
	}
	assert.True(t, coordinator.App.ZookeeperConnected, "Expected App.ZookeeperConnected to remain true")

	// On Expiration, the condition should be set and connected should be false
	coordinator.App.ZookeeperExpired.L.Lock()
	eventChan <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateExpired,
	}
	coordinator.App.ZookeeperExpired.Wait()
	coordinator.App.ZookeeperExpired.L.Unlock()
	assert.False(t, coordinator.App.ZookeeperConnected, "Expected App.ZookeeperConnected to be false")

	eventChan <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateConnected,
	}
	time.Sleep(100 * time.Millisecond)
	assert.True(t, coordinator.App.ZookeeperConnected, "Expected App.ZookeeperConnected to be true")

	close(eventChan)
	coordinator.running.Wait()
}

// Example for the Coordinator docs on how to do connection state monitoring
func ExampleCoordinator_stateMonitoring() {
	// Ignore me - needed to make the example clean
	app := &protocol.ApplicationContext{}

	for {
		// Wait for the Zookeeper connection to be connected
		for !app.ZookeeperConnected {
			// Sleep before looping around to prevent a tight loop
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Zookeeper is connected
		// Do all the work you need to do setting up watches, locks, etc.

		// Wait on the condition that signals that the session has expired
		app.ZookeeperExpired.L.Lock()
		app.ZookeeperExpired.Wait()
		app.ZookeeperExpired.L.Unlock()

		// The Zookeeper session has been lost
		// Do any work that you need to in order to clean up, or stop work that was happening inside a lock

		// Loop around to wait for the Zookeeper session to be established again
	}
}
