package notifier

import (
	"sync"
	"go.uber.org/zap"

	"github.com/toddpalino/Burrow/core/configuration"
	"github.com/toddpalino/Burrow/core/protocol"
)

type SlackNotifier struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.NotifierConfig
	requestChannel  chan interface{}
	running         sync.WaitGroup
}

func (module *SlackNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]
	module.requestChannel = make(chan interface{})
	module.running = sync.WaitGroup{}
}

func (module *SlackNotifier) GetCommunicationChannel() chan interface{} {
	return module.requestChannel
}

func (module *SlackNotifier) Start() error {
	module.running.Add(1)
	return nil
}

func (module *SlackNotifier) Stop() error {
	close(module.requestChannel)
	module.running.Done()
	return nil
}
