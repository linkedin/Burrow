package notifier

import (
	"errors"
	"regexp"
	"text/template"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type SlackNotifier struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	groupWhitelist  *regexp.Regexp
	extras          map[string]string
	templateOpen    *template.Template
	templateClose   *template.Template

	name            string
	myConfiguration *configuration.NotifierConfig
	profile         *configuration.SlackNotifierProfile
}

func (module *SlackNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]

	if profile, ok := module.App.Configuration.SlackNotifierProfile[module.myConfiguration.Profile]; ok {
		module.profile = profile
	} else {
		module.Log.Panic("unknown Slack notifier profile")
		panic(errors.New("configuration error"))
	}

	// Set defaults for module-specific configs if needed
}

func (module *SlackNotifier) Start() error {
	// Slack notifier does not have a running component - no start needed
	return nil
}

func (module *SlackNotifier) Stop() error {
	// Slack notifier does not have a running component - no start needed
	return nil
}

func (module *SlackNotifier) GetName() string {
	return module.name
}

func (module *SlackNotifier) GetConfig() *configuration.NotifierConfig {
	return module.myConfiguration
}

func (module *SlackNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *SlackNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *SlackNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

func (module *SlackNotifier) Notify (status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
}