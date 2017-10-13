package evaluator

import (
	"testing"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/internal/storage"
	"github.com/linkedin/Burrow/core/protocol"

	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
)

func fixtureModule() (*storage.Coordinator, *CachingEvaluator) {
	storageCoordinator := storage.StorageCoordinatorWithOffsets()

	module := &CachingEvaluator{
		Log: zap.NewNop(),
	}
	module.App = storageCoordinator.App
	module.App.EvaluatorChannel = make(chan *protocol.EvaluatorRequest)

	module.App.Configuration.Evaluator = make(map[string]*configuration.EvaluatorConfig)
	module.App.Configuration.Evaluator["test"] = &configuration.EvaluatorConfig{
		ClassName: "caching",
		ExpireCache: 30,
	}

	// Return the module without starting it, so we can test configure, start, and stop
	return storageCoordinator, module
}

func startWithTestCluster() (*storage.Coordinator, *CachingEvaluator) {
	storageCoordinator, module := fixtureModule()
	module.Configure("test")
	module.Start()
	return storageCoordinator, module
}

func stopTestCluster(storageCoordinator *storage.Coordinator, module *CachingEvaluator) {
	module.Stop()
	storageCoordinator.Stop()
}

func TestCachingEvaluator_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(CachingEvaluator))
}

func TestCachingEvaluator_ImplementsEvaluatorModule(t *testing.T) {
	assert.Implements(t, (*EvaluatorModule)(nil), new(CachingEvaluator))
}

func TestCachingEvaluator_Configure(t *testing.T) {
	storageCoordinator, module := fixtureModule()
	module.Configure("test")
	storageCoordinator.Stop()
}

func TestCachingEvaluator_Configure_DefaultExpireCache(t *testing.T) {
	storageCoordinator, module := fixtureModule()
	module.App.Configuration.Evaluator["test"].ExpireCache = 0
	module.Configure("test")
	assert.Equal(t, int64(10), module.myConfiguration.ExpireCache, "Default ExpireCache value of 10 did not get set")
	storageCoordinator.Stop()
}

// Also tests Stop
func TestCachingEvaluator_Start(t *testing.T) {
	storageCoordinator, module := startWithTestCluster()

	// We should send a request for a non-existent group, which will return a StatusNotFound
	request := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "nosuchgroup",
		ShowAll: false,

	}
	module.GetCommunicationChannel() <- request
	response := <- request.Reply

	assert.Equalf(t, response.Status, protocol.StatusNotFound, "Expected status to be NOTFOUND, not %v", response.Status.String())

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")

	stopTestCluster(storageCoordinator, module)
}

func TestCachingEvaluator_SingleRequest_NoShowAll(t *testing.T) {
	storageCoordinator, module := startWithTestCluster()

	request := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "testgroup",
		ShowAll: false,

	}
	module.GetCommunicationChannel() <- request
	response := <- request.Reply

	assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	assert.True(t, response.Complete, "Expected complete to be true")
	assert.Equalf(t, 1, response.TotalPartitions, "Expected total_partitions to be 1, not %v", response.TotalPartitions)
	assert.Equalf(t, uint64(2421), response.TotalLag, "Expected total_lag to be 2421, not %v", response.TotalLag)
	assert.Equalf(t, "testcluster", response.Cluster, "Expected cluster to be testcluster, not %v", response.Cluster)
	assert.Equalf(t, "testgroup", response.Group, "Expected group to be testgroup, not %v", response.Group)
	assert.Lenf(t, response.Partitions, 0, "Expected 0 partition status objects, not %v", len(response.Partitions))

	response, ok := <- request.Reply
	assert.False(t, ok, "Expected channel to be closed")

	stopTestCluster(storageCoordinator, module)
}

func TestCachingEvaluator_SingleRequest_ShowAll(t *testing.T) {
	storageCoordinator, module := startWithTestCluster()

	request := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "testgroup",
		ShowAll: true,

	}
	module.GetCommunicationChannel() <- request
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

	stopTestCluster(storageCoordinator, module)
}

type testset struct {
	offsets                 []*protocol.ConsumerOffset
	currentLag              int64
	timeNow                 int64
	isLagAlwaysNotZero      bool
	checkIfOffsetsRewind    bool
	checkIfOffsetsStopped   bool
	checkIfOffsetsStalled   bool
	checkIfLagNotDecreasing bool
	status                  protocol.StatusConstant
}

// This section is the "correctness" proof for the evaluator. Please add lots of tests here, as it's critical that this
// code operate properly and give good results every time.
//
// When adding tests, remember the following things:
//     1) The Timestamp fields are in milliseconds, but the timeNow field is in seconds
//     2) The tests are performed individually and in sequence. This means that it's possible for multiple rules to be triggered
//     3) The status represents what would be returned for this set of offsets when processing rules in sequence
//     4) Use this to add tests for offset sets that you think (or know) are producing false results when improving the checks
//     5) Tests should be commented with the index number, as well as what they are trying to test and why the expected results are correct
//     5) If you change an existing test, there needs to be a good explanation as to why along with the PR
var tests = []testset{
	// 0 - returns OK because there is zero lag somewhere
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 0},
		{2000, 200000, 50},
		{3000, 300000, 100},
		{4000, 400000, 150},
		{5000, 500000, 200},
	}, 200, 600, false, false, false, false, true, protocol.StatusOK},

	// 1 - same status. does not return true for stop because time since last commit (400s) is equal to the difference (500-100), not greater than
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 0},
		{2000, 200000, 50},
		{3000, 300000, 100},
		{4000, 400000, 150},
		{5000, 500000, 200},
	}, 200, 900, false, false, false, false, true, protocol.StatusOK},

	// 2 - same status, even though stop is now true, because we still have zero lag at the start
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 0},
		{2000, 200000, 50},
		{3000, 300000, 100},
		{4000, 400000, 150},
		{5000, 500000, 200},
	}, 200, 1000, false, false, true, false, true, protocol.StatusOK},

	// 3 - status is STOP now because lag is always non-zero
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 50},
		{2000, 200000, 100},
		{3000, 300000, 150},
		{4000, 400000, 200},
		{5000, 500000, 250},
	}, 250, 1000, true, false, true, false, true, protocol.StatusStop},

	// 4 - status is OK because of zero lag, but stall is true because the offset is always the same and there is lag
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 0},
		{1000, 200000, 50},
		{1000, 300000, 100},
		{1000, 400000, 150},
		{1000, 500000, 200},
	}, 200, 600, false, false, false, true, true, protocol.StatusOK},

	// 5 - status is now STALL because the lag is always non-zero
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 100},
		{1000, 200000, 150},
		{1000, 300000, 200},
		{1000, 400000, 250},
		{1000, 500000, 300},
	}, 300, 600, true, false, false, true, true, protocol.StatusStall},

	// 6 - status is still STALL even when the lag stays the same
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 100},
		{1000, 200000, 100},
		{1000, 300000, 100},
		{1000, 400000, 100},
		{1000, 500000, 100},
	}, 100, 600, true, false, false, true, true, protocol.StatusStall},

	// 7 - status is REWIND because the offsets go backwards, even though the lag does decrease (rewind is worse)
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 100},
		{2000, 200000, 150},
		{3000, 300000, 200},
		{2000, 400000, 1250},
		{4000, 500000, 300},
	}, 300, 600, true, true, false, false, false, protocol.StatusRewind},

	// 8 - status is OK because the current lag is 0 (even though the offsets show lag), even though it would be considered stopped due to timestamps
	{[]*protocol.ConsumerOffset{
		{1000, 100000, 50},
		{2000, 200000, 100},
		{3000, 300000, 150},
		{4000, 400000, 200},
		{5000, 500000, 250},
	}, 0, 1000, true, false, true, false, true, protocol.StatusOK},
}

func TestCachingEvaluator_CheckRules(t *testing.T) {
	for i, testSet := range tests {
		result := isLagAlwaysNotZero(testSet.offsets)
		assert.Equalf(t, testSet.isLagAlwaysNotZero, result, "TEST %v: Expected isLagAlwaysNotZero to return %v, not %v", i, testSet.isLagAlwaysNotZero, result)

		result = checkIfOffsetsRewind(testSet.offsets)
		assert.Equalf(t, testSet.checkIfOffsetsRewind, result, "TEST %v: Expected checkIfOffsetsRewind to return %v, not %v", i, testSet.checkIfOffsetsRewind, result)

		result = checkIfOffsetsStopped(testSet.offsets, testSet.timeNow)
		assert.Equalf(t, testSet.checkIfOffsetsStopped, result, "TEST %v: Expected checkIfOffsetsStopped to return %v, not %v", i, testSet.checkIfOffsetsStopped, result)

		result = checkIfOffsetsStalled(testSet.offsets)
		assert.Equalf(t, testSet.checkIfOffsetsStalled, result, "TEST %v: Expected checkIfOffsetsStalled to return %v, not %v", i, testSet.checkIfOffsetsStalled, result)

		result = checkIfLagNotDecreasing(testSet.offsets)
		assert.Equalf(t, testSet.checkIfLagNotDecreasing, result, "TEST %v: Expected checkIfLagNotDecreasing to return %v, not %v", i, testSet.checkIfLagNotDecreasing, result)

		status := calculatePartitionStatus(testSet.offsets, testSet.currentLag, testSet.timeNow)
		assert.Equalf(t, testSet.status, status, "TEST %v: Expected calculatePartitionStatus to return %v, not %v", i, testSet.status.String(), status.String())
	}
}