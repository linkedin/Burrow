package evaluator

import (
	"sync"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type CachingEvaluator struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.EvaluatorConfig
	requestChannel  chan interface{}
	running         sync.WaitGroup
}

func (module *CachingEvaluator) Configure(name string) {
	module.name = name
	module.requestChannel = make(chan interface{})
	module.myConfiguration = module.App.Configuration.Evaluator[name]
	module.running = sync.WaitGroup{}
}

func (module *CachingEvaluator) GetCommunicationChannel() chan interface{} {
	return module.requestChannel
}

func (module *CachingEvaluator) Start() error {
	module.running.Add(1)
	return nil
}

func (module *CachingEvaluator) Stop() error {
	close(module.requestChannel)
	module.running.Done()
	return nil
}
