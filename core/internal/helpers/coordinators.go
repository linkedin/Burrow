package helpers

import (
	"regexp"
	"time"

	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
)

func StartCoordinatorModules(modules map[string]protocol.Module) error {
	// Start all the modules, returning an error if any fail to start
	for _, module := range modules {
		err := module.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func StopCoordinatorModules(modules map[string]protocol.Module) {
	// Stop all the modules passed in
	for _, module := range modules {
		module.Stop()
	}
}

// Use a mocked module for testing start/stop
type MockModule struct {
	mock.Mock
}
func (m *MockModule) Configure(name string) {
	m.Called(name)
}
func (m *MockModule) Start() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockModule) Stop() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockModule) GetName() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockModule) GetConfig() *configuration.NotifierConfig {
	args := m.Called()
	return args.Get(0).(*configuration.NotifierConfig)
}
func (m *MockModule) GetGroupWhitelist() *regexp.Regexp {
	args := m.Called()
	return args.Get(0).(*regexp.Regexp)
}
func (m *MockModule) GetLogger() *zap.Logger {
	args := m.Called()
	return args.Get(0).(*zap.Logger)
}
func (m *MockModule) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	args := m.Called(status)
	return args.Bool(0)
}
func (m *MockModule) Notify (status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
	m.Called(status, eventId, startTime, stateGood)
}
