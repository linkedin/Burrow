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
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

// CoordinatorWithOffsets sets up a Coordinator with a single inmemory module defined. This module is loaded with
// offsets for a test cluster and group. This func should never be called in normal code. It is only provided to
// facilitate testing by other subsystems.
func CoordinatorWithOffsets() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger:         zap.NewNop(),
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	viper.Reset()
	viper.Set("storage.test.class-name", "inmemory")
	viper.Set("storage.test.intervals", 10)
	viper.Set("storage.test.min-distance", 0)
	viper.Set("storage.test.group-whitelist", "")
	viper.Set("cluster.testcluster.class-name", "kafka")

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
		Order:               9,
		Timestamp:           9876,
	}
	time.Sleep(100 * time.Millisecond)

	// Add consumer offsets for a full ring
	startTime := (time.Now().Unix() * 1000) - 100000
	for i := 0; i < 10; i++ {
		coordinator.App.StorageChannel <- &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOffset,
			Cluster:     "testcluster",
			Topic:       "testtopic",
			Group:       "testgroup",
			Partition:   0,
			Order:       int64(i + 10),
			Offset:      int64(1000 + (i * 100)),
			Timestamp:   startTime + int64((i * 10000)),
		}

		// If we don't sleep while submitting these, we can end up with false test results due to race conditions
		time.Sleep(10 * time.Millisecond)
	}

	// Add a second group with a partial ring
	for i := 0; i < 5; i++ {
		coordinator.App.StorageChannel <- &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOffset,
			Cluster:     "testcluster",
			Topic:       "testtopic",
			Group:       "testgroup2",
			Partition:   0,
			Order:       int64(10 + i),
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
