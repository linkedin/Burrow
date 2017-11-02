package notifier

import (
	"regexp"
	"sync"
	"text/template"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type EmailNotifier struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	groupWhitelist  *regexp.Regexp
	extras          map[string]string
	templateOpen    *template.Template
	templateClose   *template.Template

	name            string
	myConfiguration *configuration.NotifierConfig
	requestChannel  chan interface{}
	running         sync.WaitGroup
}

func (module *EmailNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]
	module.requestChannel = make(chan interface{})
	module.running = sync.WaitGroup{}
}

func (module *EmailNotifier) GetCommunicationChannel() chan interface{} {
	return module.requestChannel
}

func (module *EmailNotifier) Start() error {
	module.running.Add(1)
	return nil
}

func (module *EmailNotifier) Stop() error {
	close(module.requestChannel)
	module.running.Done()
	return nil
}

func (module *EmailNotifier) GetName() string {
	return module.name
}

func (module *EmailNotifier) GetConfig() *configuration.NotifierConfig {
	return module.myConfiguration
}

func (module *EmailNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *EmailNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *EmailNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}
