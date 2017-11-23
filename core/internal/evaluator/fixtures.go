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
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/burrow/core/internal/storage"
	"github.com/linkedin/burrow/core/protocol"
)

// This file ONLY contains fixtures that are used for testing. As they can be used by other package tests, we cannot
// include them in the test file. They should not be used anywhere in normal code - just tests

func StorageAndEvaluatorCoordinatorsWithOffsets() (*Coordinator, *storage.Coordinator) {
	storageCoordinator := storage.StorageCoordinatorWithOffsets()

	evaluatorCoordinator := Coordinator{
		Log: zap.NewNop(),
	}
	evaluatorCoordinator.App = storageCoordinator.App
	evaluatorCoordinator.App.EvaluatorChannel = make(chan *protocol.EvaluatorRequest)

	viper.Set("evaluator.test.class-name", "caching")
	viper.Set("evaluator.test.expire-cache", 30)

	evaluatorCoordinator.Configure()
	evaluatorCoordinator.Start()

	return &evaluatorCoordinator, storageCoordinator
}
