package cluster

import (
	"sync"
	"go.uber.org/zap"

	"github.com/toddpalino/Burrow/core/configuration"
	"github.com/toddpalino/Burrow/core/protocol"
)

type KafkaCluster struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.ClusterConfig
	clientProfile   *configuration.ClientProfile
	running         sync.WaitGroup
}

func (module *KafkaCluster) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Cluster[name]
	module.running = sync.WaitGroup{}

	if module.myConfiguration.ClientProfile == "" {
		module.clientProfile = module.App.Configuration.ClientProfile["default"]
	} else if profile, ok := module.App.Configuration.ClientProfile[module.myConfiguration.ClientProfile]; ok {
		module.clientProfile = profile
	} else {
		panic("Cluster '" + name + "' references an unknown client-profile '" + module.myConfiguration.ClientProfile + "'")
	}
}

func (module *KafkaCluster) GetCommunicationChannel() chan interface{} {
	return nil
}

func (module *KafkaCluster) Start() error {
	module.running.Add(1)
	return nil
}

func (module *KafkaCluster) Stop() error {
	module.running.Done()
	return nil
}
