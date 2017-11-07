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
	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"go.uber.org/zap"
	"time"
)

// This file ONLY contains fixtures that are used for testing. As they can be used by other package tests, we cannot
// include them in the test file. They should not be used anywhere in normal code - just tests

func StorageCoordinatorWithOffsets() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger:         zap.NewNop(),
		Configuration:  &configuration.Configuration{},
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	coordinator.App.Configuration.Storage = make(map[string]*configuration.StorageConfig)
	coordinator.App.Configuration.Storage["test"] = &configuration.StorageConfig{
		ClassName:      "inmemory",
		Intervals:      10,
		MinDistance:    0,
		GroupWhitelist: "",
	}
	coordinator.App.Configuration.Cluster = make(map[string]*configuration.ClusterConfig)
	coordinator.App.Configuration.Cluster["testcluster"] = &configuration.ClusterConfig{}

	coordinator.Configure()
	coordinator.Start()

	// Add a broker offset
	coordinator.App.StorageChannel <- &protocol.StorageRequest{
		RequestType:         protocol.StorageSetBrokerOffset,
		Cluster:             "testcluster",
		Topic:               "testtopic",
		Partition:           0,
		TopicPartitionCount: 1,
		Offset:              4321,
		Timestamp:           9876,
	}

	// Add consumer offsets for a full ring
	startTime := (time.Now().Unix() * 1000) - 100000
	for i := 0; i < 10; i++ {
		coordinator.App.StorageChannel <- &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOffset,
			Cluster:     "testcluster",
			Topic:       "testtopic",
			Group:       "testgroup",
			Partition:   0,
			Offset:      int64(1000 + (i * 100)),
			Timestamp:   startTime + int64((i * 10000)),
		}

		// If we don't sleep while submitting these, we can end up with false test results due to race conditions
		time.Sleep(10 * time.Millisecond)
	}

	// Sleep just a little more to make sure everything's processed
	time.Sleep(100 * time.Millisecond)
	return &coordinator
}
