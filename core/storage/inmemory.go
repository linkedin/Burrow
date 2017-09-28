package storage

import (
	"sync"
	"go.uber.org/zap"

	"github.com/toddpalino/Burrow/core/configuration"
	"github.com/toddpalino/Burrow/core/protocol"
)

type InMemoryStorage struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.StorageConfig
	requestChannel  chan interface{}
	running         sync.WaitGroup
}

func (module *InMemoryStorage) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Storage[name]
	module.requestChannel = make(chan interface{})
	module.running = sync.WaitGroup{}
}

func (module *InMemoryStorage) GetCommunicationChannel() chan interface{} {
	return module.requestChannel
}

func (module *InMemoryStorage) Start() error {
	module.running.Add(1)
	return nil
}

func (module *InMemoryStorage) Stop() error {
	close(module.requestChannel)
	module.running.Done()
	return nil
}
