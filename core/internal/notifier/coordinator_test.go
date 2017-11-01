package notifier

import (
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/internal/helpers"

	"testing"
	"github.com/stretchr/testify/assert"
)

func fixtureCoordinator() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger:           zap.NewNop(),
		Configuration:    &configuration.Configuration{},
		StorageChannel:   make(chan *protocol.StorageRequest),
		EvaluatorChannel: make(chan *protocol.EvaluatorRequest),
	}

	coordinator.App.Configuration.Notifier = make(map[string]*configuration.NotifierConfig)
	coordinator.App.Configuration.Notifier["test"] = &configuration.NotifierConfig{
		ClassName: "null",
		Interval:  60,
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

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledConfigure, "Expected module Configure to be called")
	assert.Equalf(t, int64(60), coordinator.minInterval, "Expected coordinator minInterval to be 60, not %v", coordinator.minInterval)
}

func TestCoordinator_Configure_NoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	delete(coordinator.App.Configuration.Notifier, "test")
	coordinator.Configure()

	assert.Equalf(t, int64(math.MaxInt64), coordinator.minInterval, "Expected coordinator minInterval to be MaxInt64, not %v", coordinator.minInterval)
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.App.Configuration.Notifier["anothertest"] = &configuration.NotifierConfig{
		ClassName: "null",
	}
	coordinator.Configure()

	assert.Lenf(t, coordinator.modules, 2, "Expected 2 modules configured, not %v", len(coordinator.modules))

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledConfigure, "Expected module 'test' Configure to be called")
	module = coordinator.modules["anothertest"].(*NullNotifier)
	assert.True(t, module.CalledConfigure, "Expected module 'anothertest' Configure to be called")
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

// This tests the full set of calls to send and process storage requests
func TestCoordinator_sendClusterRequest(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// This cluster will get deleted
	coordinator.clusters["deleteme"] = &ClusterGroups{}

	go coordinator.sendClusterRequest()
	request := <- coordinator.App.StorageChannel
	assert.Equalf(t, protocol.StorageFetchClusters, request.RequestType, "Expected request type to be StorageFetchClusters, not %v", request.RequestType)

	// Send a response back with a cluster list, which will trigger another storage request
	request.Reply <- []string{"testcluster"}
	request = <- coordinator.App.StorageChannel
	time.Sleep(50 * time.Millisecond)

	assert.Equalf(t, protocol.StorageFetchConsumers, request.RequestType, "Expected request type to be StorageFetchConsumers, not %v", request.RequestType)
	assert.Equalf(t, "testcluster", request.Cluster, "Expected request cluster to be testcluster, not %v", request.RequestType)

	// Send back the group list response, and sleep for a moment after that (otherwise we'll be racing the goroutine that updates the cluster groups
	request.Reply <- []string{"testgroup"}
	time.Sleep(50 * time.Millisecond)

	assert.Lenf(t, coordinator.clusters, 1, "Expected 1 entry in the clusters map, not %v", len(coordinator.clusters))
	cluster, ok := coordinator.clusters["testcluster"]
	assert.True(t, ok, "Expected clusters map key to be testcluster")
	assert.Lenf(t, cluster.Groups, 1, "Expected 1 entry in the group list, not %v", len(cluster.Groups))
	assert.Equalf(t, "testgroup", cluster.Groups[0], "Expected group to be testgroup, not %v", cluster.Groups[0])

}

// This tests the full set of calls to send evaluator requests
func TestCoordinator_sendEvaluatorRequests(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// A test cluster and group to send requests for
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Groups: []string{"testgroup"},
		Lock:   &sync.RWMutex{},
	}
	coordinator.clusters["testcluster2"] = &ClusterGroups{
		Groups: []string{"testgroup2"},
		Lock:   &sync.RWMutex{},
	}

	go coordinator.sendEvaluatorRequests()

	// We expect to get 2 requests
	for i := 0; i < 2; i++ {
		request := <- coordinator.App.EvaluatorChannel
		switch request.Cluster {
		case "testcluster":
			assert.Equalf(t, "testcluster", request.Cluster, "Expected request cluster to be testcluster, not %v", request.Cluster)
			assert.Equalf(t, "testgroup", request.Group, "Expected request group to be testgroup, not %v", request.Group)
			assert.False(t, request.ShowAll, "Expected ShowAll to be false")
		case "testcluster2":
			assert.Equalf(t, "testcluster2", request.Cluster, "Expected request cluster to be testcluster2, not %v", request.Cluster)
			assert.Equalf(t, "testgroup2", request.Group, "Expected request group to be testgroup2, not %v", request.Group)
			assert.False(t, request.ShowAll, "Expected ShowAll to be false")
		default:
			assert.Failf(t, "Received unexpected request for cluster %v, group %v", request.Cluster, request.Group)
		}
	}

	select {
	case <- coordinator.App.EvaluatorChannel:
		assert.Fail(t, "Received extra request on the evaluator channel")
	default:
		// All is good - we didn't expect to find another request
	}
}

// This tests the evaluator receive loop
func TestCoordinator_responseLoop(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Swap out the coordinator modules with a mock for testing
	mockModule := &helpers.MockModule{}
	coordinator.modules["test"] = mockModule

	// We're going to test 3 responses - not found, OK (which will be below threshold), and error
	responseNotFound := &protocol.ConsumerGroupStatus{Status: protocol.StatusNotFound}
	responseOK := &protocol.ConsumerGroupStatus{Status: protocol.StatusOK}
	responseError := &protocol.ConsumerGroupStatus{Status: protocol.StatusError}
	go func() {
		coordinator.evaluatorResponse <- responseNotFound
		coordinator.evaluatorResponse <- responseOK
		coordinator.evaluatorResponse <- responseError

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	// Set up the mock responses
	mockModule.On("AcceptConsumerGroup", responseOK).Return(false)
	mockModule.On("AcceptConsumerGroup", responseError).Return(true)
	mockModule.On("Notify", responseError).Return()

	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	mockModule.AssertExpectations(t)
}
