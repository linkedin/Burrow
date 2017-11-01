package notifier

import (
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

// This notifier is only used for testing. It is used in place of a mock when testing the coordinator so that there is
// no template loading code in the way.
type NullNotifier struct {
	App                        *protocol.ApplicationContext
	Log                        *zap.Logger
	name                       string

	CalledConfigure            bool
	CalledStart                bool
	CalledStop                 bool
	CalledNotify               bool
	CalledAcceptConsumerGroups bool
}

func (module *NullNotifier) Configure(name string) {
	module.name = name
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

func (module *NullNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	module.CalledAcceptConsumerGroups = true
	return true
}

func (module *NullNotifier) Notify (status *protocol.ConsumerGroupStatus) {
	module.CalledNotify = true
}
