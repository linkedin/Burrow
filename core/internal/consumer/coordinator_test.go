package consumer

import (
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"

	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func fixtureCoordinator() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger:         zap.NewNop(),
		Configuration:  &configuration.Configuration{},
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	coordinator.App.Configuration.ClientProfile = make(map[string]*configuration.ClientProfile)
	coordinator.App.Configuration.ClientProfile[""] = &configuration.ClientProfile{
		ClientID: "testid",
	}

	coordinator.App.Configuration.Cluster = make(map[string]*configuration.ClusterConfig)
	coordinator.App.Configuration.Cluster["test"] = &configuration.ClusterConfig{
		ClassName: "kafka",
		Servers:   []string{"broker1.example.com:1234"},
	}

	coordinator.App.Configuration.Consumer = make(map[string]*configuration.ConsumerConfig)
	coordinator.App.Configuration.Consumer["test"] = &configuration.ConsumerConfig{
		ClassName: "kafka",
		Servers:   []string{"broker1.example.com:1234"},
		Cluster:   "test",
	}

	return &coordinator
}

func TestCoordinator_ImplementsCoordinator(t *testing.T) {
	assert.Implements(t, (*protocol.Coordinator)(nil), new(Coordinator))
}

func TestCoordinator_Configure(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	assert.Lenf(t, coordinator.modules, 1, "Expected 1 module configured, not %v", len(coordinator.modules))
}

func TestCoordinator_Configure_NoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	delete(coordinator.App.Configuration.Consumer, "test")

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Configure_BadCluster(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.App.Configuration.Consumer["test"].Cluster = "nocluster"

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.App.Configuration.Consumer["anothertest"] = &configuration.ConsumerConfig{
		ClassName: "kafka",
		Servers:   []string{"broker1.example.com:1234"},
		Cluster:   "test",
	}
	coordinator.Configure()
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

func TestCoordinator_StartStop(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Swap out the coordinator modules with a mock for testing
	mockModule := &MockModule{}
	mockModule.On("Start").Return(nil)
	mockModule.On("Stop").Return(nil)
	coordinator.modules["test"] = mockModule

	coordinator.Start()
	mockModule.AssertCalled(t, "Start")

	coordinator.Stop()
	mockModule.AssertCalled(t, "Stop")
}
