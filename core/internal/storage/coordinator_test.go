package storage

import (
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"

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

	coordinator.App.Configuration.Storage = make(map[string]*configuration.StorageConfig)
	coordinator.App.Configuration.Storage["test"] = &configuration.StorageConfig{
		ClassName: "inmemory",
		Intervals: 10,
		MinDistance: 1,
		GroupWhitelist: "",
	}
	coordinator.App.Configuration.Cluster = make(map[string]*configuration.ClusterConfig)
	coordinator.App.Configuration.Cluster["testcluster"] = &configuration.ClusterConfig{}

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
	delete(coordinator.App.Configuration.Storage, "test")

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.App.Configuration.Storage["anothertest"] = &configuration.StorageConfig{
		ClassName: "inmemory",
		Intervals: 10,
		MinDistance: 1,
		GroupWhitelist: "",
	}

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Start(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()
	coordinator.Start()

	// Best is to test a request that we know the response to
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	coordinator.App.StorageChannel <- request
	response := <- request.Reply

	assert.IsType(t, []string{}, response, "Expected response to be of type []string")
	val := response.([]string)
	assert.Len(t, val, 1, "One entry not returned")
	assert.Equalf(t, val[0], "testcluster", "Expected return value to be 'testcluster', not %v", val[0])

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")

	coordinator.Stop()
}
