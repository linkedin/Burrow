package evaluator

import (
	"go.uber.org/zap"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/internal/storage"
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

	evaluatorCoordinator.App.Configuration.Evaluator = make(map[string]*configuration.EvaluatorConfig)
	evaluatorCoordinator.App.Configuration.Evaluator["test"] = &configuration.EvaluatorConfig{
		ClassName:   "caching",
		ExpireCache: 30,
	}

	evaluatorCoordinator.Configure()
	evaluatorCoordinator.Start()

	return &evaluatorCoordinator, storageCoordinator
}

