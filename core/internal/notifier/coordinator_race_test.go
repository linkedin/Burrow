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

package notifier

import (
	"time"

	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"sync"
)

// This tests the full set of calls to send evaluator requests. It triggers the race detector because of setting
// doEvaluations to false to end the loop.
func TestCoordinator_sendEvaluatorRequests(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// A test cluster and group to send requests for
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		LastNotify: make(map[string]time.Time),
		LastEval:   time.Now().Add(-time.Duration(coordinator.minInterval) * time.Second),
	}
	coordinator.clusters["testcluster2"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster2"].Groups["testgroup2"] = &consumerGroup{
		LastNotify: make(map[string]time.Time),
		LastEval:   time.Now().Add(-time.Duration(coordinator.minInterval) * time.Second),
	}

	coordinator.doEvaluations = true
	coordinator.running.Add(1)
	go coordinator.sendEvaluatorRequests()

	// We expect to get 2 requests
	for i := 0; i < 2; i++ {
		request := <-coordinator.App.EvaluatorChannel
		switch request.Cluster {
		case "testcluster":
			assert.Equalf(t, "testcluster", request.Cluster, "Expected request cluster to be testcluster, not %v", request.Cluster)
			assert.Equalf(t, "testgroup", request.Group, "Expected request group to be testgroup, not %v", request.Group)
			assert.False(t, request.ShowAll, "Expected ShowAll to be false")
		case "testcluster2":
			assert.Equalf(t, "testcluster2", request.Cluster, "Expected request cluster to be testcluster2, not %v", request.Cluster)
			assert.Equalf(t, "testgroup2", request.Group, "Expected request group to be testgroup2, not %v", request.Group)
			assert.False(t, request.ShowAll, "Expected ShowAll to be false")
		default:
			assert.Failf(t, "Received unexpected request for cluster %v, group %v", request.Cluster, request.Group)
		}
	}

	select {
	case <-coordinator.App.EvaluatorChannel:
		assert.Fail(t, "Received extra request on the evaluator channel")
	default:
		// All is good - we didn't expect to find another request
	}
	coordinator.doEvaluations = false
}

// We know this will trigger the race detector, because of the way we manipulate the ZK state
func TestCoordinator_manageEvalLoop_Start(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Add mock calls for the Zookeeper client - Lock immediately returns with no error
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").Return(nil)
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

	go coordinator.manageEvalLoop()
	time.Sleep(200 * time.Millisecond)

	mockLock.AssertExpectations(t)
	mockZk.AssertExpectations(t)
	assert.True(t, coordinator.doEvaluations, "Expected doEvaluations to be true")
}

// We know this will trigger the race detector, because of the way we manipulate the ZK state
func TestCoordinator_manageEvalLoop_Expiration(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Add mock calls for the Zookeeper client - Lock immediately returns with no error
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").Return(nil)
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

	go coordinator.manageEvalLoop()
	time.Sleep(200 * time.Millisecond)

	// ZK gets disconnected and expired
	coordinator.App.ZookeeperConnected = false
	coordinator.App.ZookeeperExpired.Broadcast()
	time.Sleep(300 * time.Millisecond)

	mockLock.AssertExpectations(t)
	mockZk.AssertExpectations(t)
	assert.False(t, coordinator.doEvaluations, "Expected doEvaluations to be false")
}

// We know this will trigger the race detector, because of the way we manipulate the ZK state
func TestCoordinator_manageEvalLoop_Reconnect(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Add mock calls for the Zookeeper client - Lock immediately returns with no error
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").Return(nil)
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

	go coordinator.manageEvalLoop()
	time.Sleep(200 * time.Millisecond)

	// ZK gets disconnected and expired
	coordinator.App.ZookeeperConnected = false
	coordinator.App.ZookeeperExpired.Broadcast()
	time.Sleep(200 * time.Millisecond)

	// ZK gets reconnected
	coordinator.App.ZookeeperConnected = true
	time.Sleep(300 * time.Millisecond)

	mockLock.AssertExpectations(t)
	mockZk.AssertExpectations(t)
	assert.True(t, coordinator.doEvaluations, "Expected doEvaluations to be true")
}
