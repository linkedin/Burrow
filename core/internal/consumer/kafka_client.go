package consumer

import (
	"sync"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type KafkaClient struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.ConsumerConfig
	clientProfile   *configuration.ClientProfile
	running         sync.WaitGroup
}

func (module *KafkaClient) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Consumer[name]
	module.running = sync.WaitGroup{}

	if module.myConfiguration.ClientProfile == "" {
		module.clientProfile = module.App.Configuration.ClientProfile["default"]
	} else if profile, ok := module.App.Configuration.ClientProfile[module.myConfiguration.ClientProfile]; ok {
		module.clientProfile = profile
	} else {
		panic("Consumer '" + name + "' references an unknown client-profile '" + module.myConfiguration.ClientProfile + "'")
	}
}

func (module *KafkaClient) GetCommunicationChannel() chan interface{} {
	return nil
}

func (module *KafkaClient) Start() error {
	module.running.Add(1)
	return nil
}

func (module *KafkaClient) Stop() error {
	module.running.Done()
	return nil
}
