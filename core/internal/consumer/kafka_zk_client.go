package consumer

import (
	"sync"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type KafkaZkClient struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.ConsumerConfig
	running         sync.WaitGroup
}

func (module *KafkaZkClient) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Consumer[name]
	module.running = sync.WaitGroup{}
}

func (module *KafkaZkClient) GetCommunicationChannel() chan interface{} {
	return nil
}

func (module *KafkaZkClient) Start() error {
	module.running.Add(1)
	return nil
}

func (module *KafkaZkClient) Stop() error {
	module.running.Done()
	return nil
}
