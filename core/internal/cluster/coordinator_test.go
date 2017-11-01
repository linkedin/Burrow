package cluster

import (
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/internal/helpers"

	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
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
	delete(coordinator.App.Configuration.Cluster, "test")

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.App.Configuration.Cluster["anothertest"] = &configuration.ClusterConfig{
		ClassName: "kafka",
		Servers:   []string{"broker1.example.com:1234"},
	}
	coordinator.Configure()
}

func TestCoordinator_StartStop(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Swap out the coordinator modules with a mock for testing
	mockModule := &helpers.MockModule{}
	mockModule.On("Start").Return(nil)
	mockModule.On("Stop").Return(nil)
	coordinator.modules["test"] = mockModule

	coordinator.Start()
	mockModule.AssertCalled(t, "Start")

	coordinator.Stop()
	mockModule.AssertCalled(t, "Stop")
}
