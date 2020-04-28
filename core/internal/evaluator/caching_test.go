/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package evaluator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/storage"
	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureModule() (*storage.Coordinator, *CachingEvaluator) {
	storageCoordinator := storage.CoordinatorWithOffsets()

	module := &CachingEvaluator{
		Log: zap.NewNop(),
	}
	module.App = storageCoordinator.App
	module.App.EvaluatorChannel = make(chan *protocol.EvaluatorRequest)

	viper.Reset()
	viper.Set("evaluator.test.class-name", "caching")
	viper.Set("evaluator.test.expire-cache", 30)

	// Return the module without starting it, so we can test configure, start, and stop
	return storageCoordinator, module
}

func startWithTestCluster() (*storage.Coordinator, *CachingEvaluator) {
	storageCoordinator, module := fixtureModule()
	module.Configure("test", "evaluator.test")
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
	assert.Implements(t, (*Module)(nil), new(CachingEvaluator))
}

func TestCachingEvaluator_Configure(t *testing.T) {
	storageCoordinator, module := fixtureModule()
	module.Configure("test", "evaluator.test")
	storageCoordinator.Stop()
}

func TestCachingEvaluator_Configure_DefaultExpireCache(t *testing.T) {
	storageCoordinator, module := fixtureModule()
	viper.Reset()
	viper.Set("evaluator.test.class-name", "caching")

	module.Configure("test", "evaluator.test")
	assert.Equal(t, int(10), module.expireCache, "Default ExpireCache value of 10 did not get set")
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
	response := <-request.Reply

	assert.Equalf(t, response.Status, protocol.StatusNotFound, "Expected status to be NOTFOUND, not %v", response.Status.String())

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
	response := <-request.Reply

	assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	assert.Equalf(t, float32(1.0), response.Complete, "Expected complete to be 1.0, not %v", response.Complete)
	assert.Equalf(t, 1, response.TotalPartitions, "Expected total_partitions to be 1, not %v", response.TotalPartitions)
	assert.Equalf(t, uint64(2421), response.TotalLag, "Expected total_lag to be 2421, not %v", response.TotalLag)
	assert.Equalf(t, "testcluster", response.Cluster, "Expected cluster to be testcluster, not %v", response.Cluster)
	assert.Equalf(t, "testgroup", response.Group, "Expected group to be testgroup, not %v", response.Group)
	assert.Lenf(t, response.Partitions, 0, "Expected 0 partition status objects, not %v", len(response.Partitions))

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
	response := <-request.Reply

	assert.Equalf(t, protocol.StatusOK, response.Status, "Expected status to be OK, not %v", response.Status.String())
	assert.Equalf(t, float32(1.0), response.Complete, "Expected complete to be 1.0, not %v", response.Complete)
	assert.Equalf(t, 1, response.TotalPartitions, "Expected total_partitions to be 1, not %v", response.TotalPartitions)
	assert.Equalf(t, uint64(2421), response.TotalLag, "Expected total_lag to be 2421, not %v", response.TotalLag)
	assert.Equalf(t, "testcluster", response.Cluster, "Expected cluster to be testcluster, not %v", response.Cluster)
	assert.Equalf(t, "testgroup", response.Group, "Expected group to be testgroup, not %v", response.Group)
	assert.Lenf(t, response.Partitions, 1, "Expected 1 partition status objects, not %v", len(response.Partitions))

	stopTestCluster(storageCoordinator, module)
}

func TestCachingEvaluator_SingleRequest_Incomplete(t *testing.T) {
	storageCoordinator, module := startWithTestCluster()

	request := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "testgroup2",
	}
	module.GetCommunicationChannel() <- request
	response := <-request.Reply

	assert.Equalf(t, protocol.StatusError, response.Status, "Expected status to be ERR, not %v", response.Status.String())
	assert.Equalf(t, float32(0.0), response.Complete, "Expected complete to be 0.0, not %v", response.Complete)
	assert.Equalf(t, 1, response.TotalPartitions, "Expected total_partitions to be 1, not %v", response.TotalPartitions)
	assert.Equalf(t, uint64(2921), response.TotalLag, "Expected total_lag to be 2921, not %v", response.TotalLag)
	assert.Equalf(t, "testcluster", response.Cluster, "Expected cluster to be testcluster, not %v", response.Cluster)
	assert.Equalf(t, "testgroup2", response.Group, "Expected group to be testgroup2, not %v", response.Group)

	assert.Lenf(t, response.Partitions, 1, "Expected 1 partition status objects, not %v", len(response.Partitions))
	assert.Equalf(t, float32(0.5), response.Partitions[0].Complete, "Expected partition Complete to be 0.5, not %v", response.Partitions[0].Complete)
	assert.Equalf(t, protocol.StatusStop, response.Partitions[0].Status, "Expected partition status to be STOP, not %v", response.Partitions[0].Status.String())

	assert.NotNil(t, response.Maxlag, "Expected Maxlag to be not nil")
	assert.NotNil(t, response.Maxlag.Start, "Expected Maxlag.Start to be not nil")
	assert.Equalf(t, int64(1000), response.Maxlag.Start.Offset, "Expected Maxlag.Start.Offset to be 100, not %v", response.Maxlag.Start.Offset)
	assert.NotNil(t, response.Maxlag.End, "Expected Maxlag.End to be not nil")
	assert.Equalf(t, int64(1400), response.Maxlag.End.Offset, "Expected Maxlag.End.Offset to be 100, not %v", response.Maxlag.End.Offset)

	stopTestCluster(storageCoordinator, module)
}

type testset struct {
	offsets                 []*protocol.ConsumerOffset
	brokerOffsets           []int64
	currentLag              uint64
	timeNow                 int64
	isLagAlwaysNotZero      bool
	checkIfOffsetsRewind    bool
	checkIfOffsetsStopped   bool
	checkIfOffsetsStalled   bool
	checkIfLagNotDecreasing bool
	checkIfRecentLagZero    bool
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
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{0}},
			{2000, 2, 200000, &protocol.Lag{50}},
			{3000, 3, 300000, &protocol.Lag{100}},
			{4000, 4, 400000, &protocol.Lag{150}},
			{5000, 5, 500000, &protocol.Lag{200}},
		},
		brokerOffsets:           []int64{5200},
		currentLag:              200,
		timeNow:                 600,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusOK,
	},

	// 1 - same status. does not return true for stop because time since last commit (400s) is equal to the difference (500-100), not greater than
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{0}},
			{2000, 2, 200000, &protocol.Lag{50}},
			{3000, 3, 300000, &protocol.Lag{100}},
			{4000, 4, 400000, &protocol.Lag{150}},
			{5000, 5, 500000, &protocol.Lag{200}},
		},
		brokerOffsets:           []int64{5200},
		currentLag:              200,
		timeNow:                 900,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusOK,
	},

	// 2 - status is now STOP because the time since last commit is great enough (500s), even though lag is zero at the start (fixed due to #290)
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{0}},
			{2000, 2, 200000, &protocol.Lag{50}},
			{3000, 3, 300000, &protocol.Lag{100}},
			{4000, 4, 400000, &protocol.Lag{150}},
			{5000, 5, 500000, &protocol.Lag{200}},
		},
		brokerOffsets:           []int64{5200},
		currentLag:              200,
		timeNow:                 1000,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   true,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusStop,
	},

	// 3 - status is STOP when lag is always non-zero as well
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{50}},
			{2000, 2, 200000, &protocol.Lag{100}},
			{3000, 3, 300000, &protocol.Lag{150}},
			{4000, 4, 400000, &protocol.Lag{200}},
			{5000, 5, 500000, &protocol.Lag{250}},
		},
		brokerOffsets:           []int64{5250},
		currentLag:              250,
		timeNow:                 1000,
		isLagAlwaysNotZero:      true,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   true,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusStop,
	},

	// 4 - status is OK because of zero lag, but stall is true because the offset is always the same and there is lag (another commit with turn this to stall)
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{0}},
			{1000, 2, 200000, &protocol.Lag{50}},
			{1000, 3, 300000, &protocol.Lag{100}},
			{1000, 4, 400000, &protocol.Lag{150}},
			{1000, 5, 500000, &protocol.Lag{200}},
		},
		brokerOffsets:           []int64{1200},
		currentLag:              200,
		timeNow:                 600,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   true,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusOK,
	},

	// 5 - status is now STALL because the lag is always non-zero
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{100}},
			{1000, 2, 200000, &protocol.Lag{150}},
			{1000, 3, 300000, &protocol.Lag{200}},
			{1000, 4, 400000, &protocol.Lag{250}},
			{1000, 5, 500000, &protocol.Lag{300}},
		},
		brokerOffsets:           []int64{1300},
		currentLag:              300,
		timeNow:                 600,
		isLagAlwaysNotZero:      true,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   true,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusStall,
	},

	// 6 - status is still STALL even when the lag stays the same
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{100}},
			{1000, 2, 200000, &protocol.Lag{100}},
			{1000, 3, 300000, &protocol.Lag{100}},
			{1000, 4, 400000, &protocol.Lag{100}},
			{1000, 5, 500000, &protocol.Lag{100}},
		},
		brokerOffsets:           []int64{1100},
		currentLag:              100,
		timeNow:                 600,
		isLagAlwaysNotZero:      true,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   true,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusStall,
	},

	// 7 - status is REWIND because the offsets go backwards, even though the lag does decrease (rewind is worse)
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{100}},
			{2000, 2, 200000, &protocol.Lag{150}},
			{3000, 3, 300000, &protocol.Lag{200}},
			{2000, 4, 400000, &protocol.Lag{1250}},
			{4000, 5, 500000, &protocol.Lag{300}},
		},
		brokerOffsets:           []int64{4300},
		currentLag:              300,
		timeNow:                 600,
		isLagAlwaysNotZero:      true,
		checkIfOffsetsRewind:    true,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: false,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusRewind,
	},

	// 8 - status is OK because the current lag is 0 (even though the offsets show lag), even though it would be considered stopped due to timestamps
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 1, 100000, &protocol.Lag{50}},
			{2000, 2, 200000, &protocol.Lag{100}},
			{3000, 3, 300000, &protocol.Lag{150}},
			{4000, 4, 400000, &protocol.Lag{200}},
			{5000, 5, 500000, &protocol.Lag{250}},
		},
		brokerOffsets:           []int64{5250},
		currentLag:              0,
		timeNow:                 1000,
		isLagAlwaysNotZero:      true,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   true,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusOK,
	},

	// 9 - status is STOP due to timestamps because the current lag is non-zero, even though lag is always zero in offsets, only because there is new data (#290)
	{
		offsets: []*protocol.ConsumerOffset{
			{792748079, 1, 1512224618356, &protocol.Lag{0}},
			{792748080, 2, 1512224619362, &protocol.Lag{0}},
			{792748081, 3, 1512224620366, &protocol.Lag{0}},
			{792748082, 4, 1512224621367, &protocol.Lag{0}},
			{792748083, 5, 1512224622370, &protocol.Lag{0}},
			{792748084, 6, 1512224623373, &protocol.Lag{0}},
			{792748085, 7, 1512224624378, &protocol.Lag{0}},
			{792748086, 8, 1512224625379, &protocol.Lag{0}},
			{792748087, 9, 1512224626383, &protocol.Lag{0}},
			{792748088, 10, 1512224627383, &protocol.Lag{0}},
			{792748089, 11, 1512224628383, &protocol.Lag{0}},
			{792748090, 12, 1512224629388, &protocol.Lag{0}},
			{792748091, 13, 1512224630391, &protocol.Lag{0}},
			{792748092, 14, 1512224631394, &protocol.Lag{0}},
			{792748093, 15, 1512224632397, &protocol.Lag{0}},
		},
		brokerOffsets:           []int64{792749024, 792749000, 792748800, 792748600, 792748500},
		currentLag:              931,
		timeNow:                 1512224650,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   true,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusStop,
	},

	// 10 - status is OK, even though it would be stop due to timestamps, as within the recent broker offset window the lag was zero (#303)
	{
		offsets: []*protocol.ConsumerOffset{
			{792748079, 1, 1512224618356, &protocol.Lag{0}},
			{792748080, 2, 1512224619362, &protocol.Lag{0}},
			{792748081, 3, 1512224620366, &protocol.Lag{0}},
			{792748082, 4, 1512224621367, &protocol.Lag{0}},
			{792748083, 5, 1512224622370, &protocol.Lag{0}},
			{792748084, 6, 1512224623373, &protocol.Lag{0}},
			{792748085, 7, 1512224624378, &protocol.Lag{0}},
			{792748086, 8, 1512224625379, &protocol.Lag{0}},
			{792748087, 9, 1512224626383, &protocol.Lag{0}},
			{792748088, 10, 1512224627383, &protocol.Lag{0}},
			{792748089, 11, 1512224628383, &protocol.Lag{0}},
			{792748090, 12, 1512224629388, &protocol.Lag{0}},
			{792748091, 13, 1512224630391, &protocol.Lag{0}},
			{792748092, 14, 1512224631394, &protocol.Lag{0}},
			{792748093, 15, 1512224632397, &protocol.Lag{0}},
		},
		brokerOffsets:           []int64{792748094, 792748093, 792748093, 792748093},
		currentLag:              1,
		timeNow:                 1512224650,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   true,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    true,
		status:                  protocol.StatusOK,
	},

	// 11 - same case as test 0 with additional `nil` lag entries which should not affect results
	{
		offsets: []*protocol.ConsumerOffset{
			{1000, 10, 100000, &protocol.Lag{0}},
			{1500, 15, 150000, nil},
			{2000, 20, 200000, &protocol.Lag{50}},
			{2500, 25, 250000, nil},
			{3000, 30, 300000, &protocol.Lag{100}},
			{3500, 35, 350000, nil},
			{4000, 40, 400000, &protocol.Lag{150}},
			{4500, 45, 450000, nil},
			{5000, 50, 500000, &protocol.Lag{200}},
			{5500, 55, 550000, nil},
		},
		brokerOffsets:           []int64{5700},
		currentLag:              200,
		timeNow:                 600,
		isLagAlwaysNotZero:      false,
		checkIfOffsetsRewind:    false,
		checkIfOffsetsStopped:   false,
		checkIfOffsetsStalled:   false,
		checkIfLagNotDecreasing: true,
		checkIfRecentLagZero:    false,
		status:                  protocol.StatusOK,
	},
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

		result = checkIfRecentLagZero(testSet.offsets, testSet.brokerOffsets)
		assert.Equalf(t, testSet.checkIfRecentLagZero, result, "TEST %v: Expected checkIfRecentLagZero to return %v, not %v", i, testSet.checkIfRecentLagZero, result)

		status := calculatePartitionStatus(testSet.offsets, testSet.brokerOffsets, testSet.currentLag, testSet.timeNow)
		assert.Equalf(t, testSet.status, status, "TEST %v: Expected calculatePartitionStatus to return %v, not %v", i, testSet.status.String(), status.String())
	}
}

// TODO this test should fail, ie a group should not exist if all its topics are deleted.
func TestCachingEvaluator_TopicDeleted(t *testing.T) {
	storageCoordinator, module := fixtureModule()
	module.Configure("test", "evaluator.test")
	module.Start()

	// Deleting a topic will not delete the consumer group even if the consumer group has no topics
	storageCoordinator.App.StorageChannel <- &protocol.StorageRequest{
		RequestType: protocol.StorageSetDeleteTopic,
		Cluster:     "testcluster",
		Topic:       "testtopic",
	}
	time.Sleep(100 * time.Millisecond)

	evalRequest := &protocol.EvaluatorRequest{
		Reply:   make(chan *protocol.ConsumerGroupStatus),
		Cluster: "testcluster",
		Group:   "testgroup",
		ShowAll: true,
	}

	module.GetCommunicationChannel() <- evalRequest
	evalResponse := <-evalRequest.Reply

	// The status is returned as 'OK' as the topic has previously existed and
	// belonged to a group therefore if the group has a recent timestamp in the
	// consumerMap the evaluator continues as normal but processes no
	// partitions.
	assert.Equalf(t, protocol.StatusOK, evalResponse.Status, "Expected status to be OK, not %v", evalResponse.Status.String())
	assert.Emptyf(t, evalResponse.Partitions, "Expected no partitions to be returned")
	assert.Equalf(t, float32(0.0), evalResponse.Complete, "Expected 'Complete' to be 0.0")
}
