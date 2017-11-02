package notifier

import (
	"regexp"
	"text/template"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
	"time"
)

// This notifier is only used for testing. It is used in place of a mock when testing the coordinator so that there is
// no template loading code in the way.
type NullNotifier struct {
	App                        *protocol.ApplicationContext
	Log                        *zap.Logger
	name                       string
	myConfiguration            *configuration.NotifierConfig

	groupWhitelist             *regexp.Regexp
	extras                     map[string]string
	templateOpen               *template.Template
	templateClose              *template.Template

	CalledConfigure            bool
	CalledStart                bool
	CalledStop                 bool
	CalledNotify               bool
	CalledAcceptConsumerGroup  bool
}

func (module *NullNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]
	module.CalledConfigure = true
}

func (module *NullNotifier) Start() error {
	module.CalledStart = true
	return nil
}

func (module *NullNotifier) Stop() error {
	module.CalledStop = true
	return nil
}

func (module *NullNotifier) GetName() string {
	return module.name
}

func (module *NullNotifier) GetConfig() *configuration.NotifierConfig {
	return module.myConfiguration
}

func (module *NullNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *NullNotifier) GetLogger() *zap.Logger {
	return module.Log
}

func (module *NullNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	module.CalledAcceptConsumerGroup = true
	return true
}

func (module *NullNotifier) Notify (status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
	module.CalledNotify = true
}
