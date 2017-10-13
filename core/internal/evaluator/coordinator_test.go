package evaluator

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

	coordinator.App.Configuration.Evaluator = make(map[string]*configuration.EvaluatorConfig)
	coordinator.App.Configuration.Evaluator["test"] = &configuration.EvaluatorConfig{
		ClassName:   "caching",
		ExpireCache: 30,
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
	delete(coordinator.App.Configuration.Evaluator, "test")

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.App.Configuration.Evaluator["anothertest"] = &configuration.EvaluatorConfig{
		ClassName:   "caching",
		ExpireCache: 30,
	}

	assert.Panics(t, coordinator.Configure, "Expected panic")
}

func TestCoordinator_Start(t *testing.T) {
	evaluatorCoordinator, storageCoordinator := StorageAndEvaluatorCoordinatorsWithOffsets()

	// Best is to test a request that we know the response to
	request := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "testgroup",
		ShowAll: true,

	}
	evaluatorCoordinator.App.EvaluatorChannel <- request
	response := <- request.Reply

	assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	assert.True(t, response.Complete, "Expected complete to be true")
	assert.Equalf(t, 1, response.TotalPartitions, "Expected total_partitions to be 1, not %v", response.TotalPartitions)
	assert.Equalf(t, uint64(2421), response.TotalLag, "Expected total_lag to be 2421, not %v", response.TotalLag)
	assert.Equalf(t, "testcluster", response.Cluster, "Expected cluster to be testcluster, not %v", response.Cluster)
	assert.Equalf(t, "testgroup", response.Group, "Expected group to be testgroup, not %v", response.Group)
	assert.Lenf(t, response.Partitions, 1, "Expected 1 partition status objects, not %v", len(response.Partitions))

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")

	evaluatorCoordinator.Stop()
	storageCoordinator.Stop()
}

func TestCoordinator_MultipleRequests(t *testing.T) {
	evaluatorCoordinator, storageCoordinator := StorageAndEvaluatorCoordinatorsWithOffsets()

	// This test is really just to check and make sure the evaluator can handle multiple requests without deadlock
	for i:= 0; i < 10; i++ {
		request := &protocol.EvaluatorRequest{
			Reply:   make(chan *protocol.ConsumerGroupStatus),
			Cluster: "testcluster",
			Group:   "testgroup",
			ShowAll: true,

		}
		evaluatorCoordinator.App.EvaluatorChannel <- request
		response := <- request.Reply

		assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	}
	// Best is to test a request that we know the response to

	evaluatorCoordinator.Stop()
	storageCoordinator.Stop()
}
