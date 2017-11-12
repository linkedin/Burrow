/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package evaluator

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
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
	viper.Set("evaluator.test.class-name", "caching")
	viper.Set("evaluator.test.expire-cache", 30)
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

func TestCoordinator_Configure_NoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Reset()
	viper.Set("cluster.test.class-name", "kafka")
	viper.Set("cluster.test.servers", []string{"broker1.example.com:1234"})

	coordinator.Configure()
	assert.Lenf(t, coordinator.modules, 1, "Expected 1 module configured, not %v", len(coordinator.modules))
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("evaluator.anothertest.class-name", "caching")
	viper.Set("evaluator.anothertest.expire-cache", 30)

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Start(t *testing.T) {
	evaluatorCoordinator, storageCoordinator := StorageAndEvaluatorCoordinatorsWithOffsets()

	// Best is to test a request that we know the response to
	request := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "testgroup",
		ShowAll: true,
	}
	evaluatorCoordinator.App.EvaluatorChannel <- request
	response := <-request.Reply

	assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	assert.Equalf(t, float32(1.0), response.Complete, "Expected complete to be 1.0, not %v", response.Complete)
	assert.Equalf(t, 1, response.TotalPartitions, "Expected total_partitions to be 1, not %v", response.TotalPartitions)
	assert.Equalf(t, uint64(2421), response.TotalLag, "Expected total_lag to be 2421, not %v", response.TotalLag)
	assert.Equalf(t, "testcluster", response.Cluster, "Expected cluster to be testcluster, not %v", response.Cluster)
	assert.Equalf(t, "testgroup", response.Group, "Expected group to be testgroup, not %v", response.Group)
	assert.Lenf(t, response.Partitions, 1, "Expected 1 partition status objects, not %v", len(response.Partitions))

	evaluatorCoordinator.Stop()
	storageCoordinator.Stop()
}

func TestCoordinator_MultipleRequests(t *testing.T) {
	evaluatorCoordinator, storageCoordinator := StorageAndEvaluatorCoordinatorsWithOffsets()

	// This test is really just to check and make sure the evaluator can handle multiple requests without deadlock
	for i := 0; i < 10; i++ {
		request := &protocol.EvaluatorRequest{
			Reply:   make(chan *protocol.ConsumerGroupStatus),
			Cluster: "testcluster",
			Group:   "testgroup",
			ShowAll: true,
		}
		evaluatorCoordinator.App.EvaluatorChannel <- request
		response := <-request.Reply

		assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	}
	// Best is to test a request that we know the response to

	evaluatorCoordinator.Stop()
	storageCoordinator.Stop()
}
