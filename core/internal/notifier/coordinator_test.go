/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"errors"
	"regexp"
	"sync"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureCoordinator() *Coordinator {
	coordinator := Coordinator{
		Log: zap.NewNop(),
	}
	coordinator.App = &protocol.ApplicationContext{
		Logger:             zap.NewNop(),
		StorageChannel:     make(chan *protocol.StorageRequest),
		EvaluatorChannel:   make(chan *protocol.EvaluatorRequest),
		Zookeeper:          &helpers.MockZookeeperClient{},
		ZookeeperRoot:      "/burrow",
		ZookeeperConnected: true,
		ZookeeperExpired:   &sync.Cond{L: &sync.Mutex{}},
	}

	// Simple parser replacement that returns a blank template
	coordinator.templateParseFunc = func(filenames ...string) (*template.Template, error) {
		return template.New("test").Parse("")
	}

	viper.Reset()
	viper.Set("notifier.test.class-name", "null")
	viper.Set("notifier.test.group-whitelist", ".*")
	viper.Set("notifier.test.threshold", 1)
	viper.Set("notifier.test.timeout", 2)
	viper.Set("notifier.test.keepalive", 10)
	viper.Set("notifier.test.template-open", "template_open")
	viper.Set("notifier.test.template-close", "template_close")
	viper.Set("notifier.test.extras.foo", "bar")
	viper.Set("notifier.test.send-close", false)

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

	assert.Lenf(t, module.extras, 1, "Expected exactly one extra entry to be set, not %v", len(module.extras))
	val, ok := module.extras["foo"]
	assert.True(t, ok, "Expected extras key to be 'foo'")
	assert.Equalf(t, "bar", val, "Expected value of extras 'foo' to be 'bar', not %v", val)
}

func TestCoordinator_Configure_NoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Reset()
	coordinator.Configure()

	assert.Equalf(t, int64(310536000), coordinator.minInterval, "Expected coordinator minInterval to be 310536000, not %v", coordinator.minInterval)
}

func TestCoordinator_Configure_TwoModules(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("notifier.anothertest.class-name", "null")
	coordinator.Configure()

	assert.Lenf(t, coordinator.modules, 2, "Expected 2 modules configured, not %v", len(coordinator.modules))

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledConfigure, "Expected module 'test' Configure to be called")
	module = coordinator.modules["anothertest"].(*NullNotifier)
	assert.True(t, module.CalledConfigure, "Expected module 'anothertest' Configure to be called")
}

func TestCoordinator_Configure_BadRegexp(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("notifier.test.group-whitelist", "[")

	assert.Panics(t, func() { coordinator.Configure() }, "The code did not panic")
}

func TestCoordinator_Configure_BadTemplate(t *testing.T) {
	coordinator := fixtureCoordinator()

	// Simple parser replacement that returns an error
	coordinator.templateParseFunc = func(filenames ...string) (*template.Template, error) {
		return nil, errors.New("bad template")
	}

	assert.Panics(t, func() { coordinator.Configure() }, "The code did not panic")
}

func TestCoordinator_Configure_SendClose(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("notifier.test.send-close", true)

	// Simple parser replacement that returns an empty template using a string, or throws an error on unexpected use for this test
	coordinator.templateParseFunc = func(filenames ...string) (*template.Template, error) {
		if len(filenames) != 1 {
			return nil, errors.New("expected exactly 1 filename")
		}
		return template.New("test").Parse("")
	}
	coordinator.Configure()
	module := coordinator.modules["test"].(*NullNotifier)
	assert.NotNil(t, module.templateOpen, "Expected templateOpen to be set with a template")
	assert.NotNil(t, module.templateClose, "Expected templateClose to be set with a template")
}

func TestCoordinator_StartStop(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// Swap out the coordinator modules with a mock for testing
	mockModule := &helpers.MockModule{}
	mockModule.On("Start").Return(nil)
	mockModule.On("Stop").Return(nil)
	coordinator.modules["test"] = mockModule

	// Add mock calls for the Zookeeper client - We're not testing this part here, so just hang forever
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").After(1 * time.Minute).Return(errors.New("neverworks"))
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

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
	request := <-coordinator.App.StorageChannel
	assert.Equalf(t, protocol.StorageFetchClusters, request.RequestType, "Expected request type to be StorageFetchClusters, not %v", request.RequestType)

	// Send a response back with a cluster list, which will trigger another storage request
	request.Reply <- []string{"testcluster"}
	request = <-coordinator.App.StorageChannel
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
	_, ok = cluster.Groups["testgroup"]
	assert.True(t, ok, "Expected group to be testgroup")
}

// This tests the full set of calls to send evaluator requests
func TestCoordinator_sendEvaluatorRequests(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// A test cluster and group to send requests for
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Last: make(map[string]time.Time),
	}
	coordinator.clusters["testcluster2"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster2"].Groups["testgroup2"] = &ConsumerGroup{
		Last: make(map[string]time.Time),
	}

	go coordinator.sendEvaluatorRequests()

	// We expect to get 2 requests
	for i := 0; i < 2; i++ {
		request := <-coordinator.App.EvaluatorChannel
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
	case <-coordinator.App.EvaluatorChannel:
		assert.Fail(t, "Received extra request on the evaluator channel")
	default:
		// All is good - we didn't expect to find another request
	}
}

// Note, we do not check the calls to the module here, just that the response loop sets the event properly
func TestCoordinator_responseLoop_NotFound(t *testing.T) {
	coordinator := fixtureCoordinator()

	// For NotFound, we expect the notifier will not be called at all
	coordinator.notifyModuleFunc = func(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
		assert.Fail(t, "Expected notifyModule to not be called")
	}

	coordinator.Configure()

	// A test cluster and group to receive response for
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Last: make(map[string]time.Time),
	}

	// Test a NotFound response - we shouldn't do anything here
	responseNotFound := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusNotFound,
	}
	go func() {
		coordinator.evaluatorResponse <- responseNotFound

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equalf(t, "", group.Id, "Expected group incident ID to be empty, not %v", group.Id)
	assert.True(t, group.Start.IsZero(), "Expected group incident start time to be unset")
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be unset")
}

func TestCoordinator_responseLoop_NoIncidentOK(t *testing.T) {
	coordinator := fixtureCoordinator()

	// We expect notifyModule to be called for the Null module in all cases (except NotFound)
	responseOK := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusOK,
	}
	coordinator.notifyModuleFunc = func(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseOK, status, "Expected to be called with responseOK as the status")
		assert.True(t, startTime.IsZero(), "Expected to be called with zero value startTime")
		assert.Equal(t, "", eventId, "Expected to be called with empty eventId")
	}

	coordinator.Configure()

	// A test cluster and group to receive response for
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Last: make(map[string]time.Time),
	}

	// Test an OK response
	go func() {
		coordinator.evaluatorResponse <- responseOK

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equalf(t, "", group.Id, "Expected group incident ID to be empty, not %v", group.Id)
	assert.True(t, group.Start.IsZero(), "Expected group incident start time to be unset")
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be unset")

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledAcceptConsumerGroup, "Expected module 'test' AcceptConsumerGroup to be called")
}

func TestCoordinator_responseLoop_HaveIncidentOK(t *testing.T) {
	coordinator := fixtureCoordinator()

	// We expect notifyModule to be called for the Null module in all cases (except NotFound)
	mockStartTime, _ := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")
	responseOK := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusOK,
	}
	coordinator.notifyModuleFunc = func(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseOK, status, "Expected to be called with responseOK as the status")
		assert.Equal(t, mockStartTime, startTime, "Expected to be called with mockStartTime as the startTime")
		assert.Equalf(t, "testidstring", eventId, "Expected to be called with eventId as 'testidstring', not %v", eventId)
	}

	coordinator.Configure()

	// A test cluster and group to receive response for, with the incident active
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Start: mockStartTime,
		Id:    "testidstring",
		Last:  make(map[string]time.Time),
	}

	// Test an OK response
	go func() {
		coordinator.evaluatorResponse <- responseOK

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equalf(t, "", group.Id, "Expected group incident ID to be empty, not %v", group.Id)
	assert.True(t, group.Start.IsZero(), "Expected group incident start time to be cleared")
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be unset")

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledAcceptConsumerGroup, "Expected module 'test' AcceptConsumerGroup to be called")
}

func TestCoordinator_responseLoop_NoIncidentError(t *testing.T) {
	coordinator := fixtureCoordinator()

	// We expect notifyModule to be called for the Null module in all cases (except NotFound)
	responseError := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusError,
	}
	coordinator.notifyModuleFunc = func(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseError, status, "Expected to be called with responseError as the status")
		assert.False(t, startTime.IsZero(), "Expected to be called with a valid startTime, not zero")
		assert.NotEqual(t, "", eventId, "Expected to be called with a new eventId, not empty")
	}

	coordinator.Configure()

	// A test cluster and group to receive response for
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Last: make(map[string]time.Time),
	}

	// Test an Error response
	go func() {
		coordinator.evaluatorResponse <- responseError

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.NotEqual(t, "", group.Id, "Expected group incident ID to be set, not empty")
	assert.False(t, group.Start.IsZero(), "Expected group incident start time to be set")
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be unset (as real notifyFunc was not called)")

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledAcceptConsumerGroup, "Expected module 'test' AcceptConsumerGroup to be called")
}

func TestCoordinator_responseLoop_HaveIncidentError(t *testing.T) {
	coordinator := fixtureCoordinator()

	// We expect notifyModule to be called for the Null module in all cases (except NotFound)
	mockStartTime, _ := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")
	responseError := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusError,
	}
	coordinator.notifyModuleFunc = func(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseError, status, "Expected to be called with responseError as the status")
		assert.Equal(t, mockStartTime, startTime, "Expected to be called with mockStartTime as the startTime")
		assert.Equalf(t, "testidstring", eventId, "Expected to be called with eventId as 'testidstring', not %v", eventId)
	}

	coordinator.Configure()

	// A test cluster and group to receive response for, with the incident active
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Start: mockStartTime,
		Id:    "testidstring",
		Last:  make(map[string]time.Time),
	}

	// Test an Error response
	go func() {
		coordinator.evaluatorResponse <- responseError

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equal(t, mockStartTime, group.Start, "Expected group incident start time to be unchanged")
	assert.Equalf(t, "testidstring", group.Id, "Expected group incident ID to be 'testidstring', not %v", group.Id)
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be unset (as real notifyFunc was not called)")

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledAcceptConsumerGroup, "Expected module 'test' AcceptConsumerGroup to be called")
}

func TestCoordinator_notifyModule_NoIncidentOK(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// A test cluster and group to send notification for (so the func can check/set last time)
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Last: make(map[string]time.Time),
	}

	responseOK := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusOK,
	}
	mockModule := &helpers.MockModule{}
	mockModule.On("GetName").Return("test")

	coordinator.notifyModule(mockModule, responseOK, coordinator.clusters["testcluster"].Groups["testgroup"].Start, coordinator.clusters["testcluster"].Groups["testgroup"].Id)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to remain unset")
	mockModule.AssertExpectations(t)
}

func TestCoordinator_notifyModule_HaveIncidentOK(t *testing.T) {
	coordinator := fixtureCoordinator()
	coordinator.Configure()

	// A test cluster and group to send notification for (so the func can check/set last time)
	mockStartTime, _ := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Start: mockStartTime,
		Id:    "testidstring",
		Last:  make(map[string]time.Time),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"].Last["test"] = mockStartTime

	// SendClose is not set, so we still expect no notification to be sent
	responseOK := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusOK,
	}
	mockModule := &helpers.MockModule{}
	mockModule.On("GetName").Return("test")

	coordinator.notifyModule(mockModule, responseOK, coordinator.clusters["testcluster"].Groups["testgroup"].Start, coordinator.clusters["testcluster"].Groups["testgroup"].Id)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be set to the zero value")
	mockModule.AssertExpectations(t)
}

func TestCoordinator_notifyModule_HaveIncidentOK_SendClose(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("notifier.test.send-close", true)
	coordinator.Configure()

	// A test cluster and group to send notification for (so the func can check/set last time)
	mockStartTime, _ := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Start: mockStartTime,
		Id:    "testidstring",
		Last:  make(map[string]time.Time),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"].Last["test"] = mockStartTime

	// SendClose is set, so we expect a notification
	responseOK := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusOK,
	}
	mockModule := &helpers.MockModule{}
	mockModule.On("GetName").Return("test")
	mockModule.On("Notify", responseOK, coordinator.clusters["testcluster"].Groups["testgroup"].Id, coordinator.clusters["testcluster"].Groups["testgroup"].Start, true)

	coordinator.notifyModule(mockModule, responseOK, coordinator.clusters["testcluster"].Groups["testgroup"].Start, coordinator.clusters["testcluster"].Groups["testgroup"].Id)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.True(t, group.Last["test"].IsZero(), "Expected group last time to be set to the zero value")
	mockModule.AssertExpectations(t)
}

func TestCoordinator_notifyModule_IntervalTooShort(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("notifier.test.send-close", true)
	coordinator.Configure()

	// A test cluster and group to send notification for (so the func can check/set last time)
	mockStartTime := time.Now()
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Start: mockStartTime,
		Id:    "testidstring",
		Last:  make(map[string]time.Time),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"].Last["test"] = mockStartTime

	// Because our interval is 60 seconds, and we said we sent a notification "now", there should be no notification sent
	responseError := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusError,
	}
	mockModule := &helpers.MockModule{}
	mockModule.On("GetName").Return("test")

	coordinator.notifyModule(mockModule, responseError, coordinator.clusters["testcluster"].Groups["testgroup"].Start, coordinator.clusters["testcluster"].Groups["testgroup"].Id)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equal(t, mockStartTime, group.Last["test"], "Expected group last time to remain set to the start time")
	mockModule.AssertExpectations(t)
}

func TestCoordinator_notifyModule_Warning(t *testing.T) {
	coordinator := fixtureCoordinator()
	viper.Set("notifier.test.send-close", true)
	coordinator.Configure()

	// A test cluster and group to send notification for (so the func can check/set last time)
	mockStartTime := time.Now().Add(-100 * time.Second)
	coordinator.clusters["testcluster"] = &ClusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*ConsumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &ConsumerGroup{
		Start: mockStartTime,
		Id:    "testidstring",
		Last:  make(map[string]time.Time),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"].Last["test"] = mockStartTime

	// Last notification happened 100 seconds ago, so we expect a notification to be sent now
	responseError := &protocol.ConsumerGroupStatus{
		Cluster: "testcluster",
		Group:   "testgroup",
		Status:  protocol.StatusError,
	}
	mockModule := &helpers.MockModule{}
	mockModule.On("GetName").Return("test")
	mockModule.On("Notify", responseError, coordinator.clusters["testcluster"].Groups["testgroup"].Id, coordinator.clusters["testcluster"].Groups["testgroup"].Start, false)

	coordinator.notifyModule(mockModule, responseError, coordinator.clusters["testcluster"].Groups["testgroup"].Start, coordinator.clusters["testcluster"].Groups["testgroup"].Id)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.True(t, group.Last["test"].After(mockStartTime), "Expected group last time to be updated")
	mockModule.AssertExpectations(t)
}

func TestCoordinator_AcceptConsumerGroup(t *testing.T) {
	module := fixtureHttpNotifier()

	viper.Set("notifier.test.threshold", 2)
	viper.Set("notifier.test.group-whitelist", "test.*")
	module.groupWhitelist, _ = regexp.Compile("test.*")
	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status: protocol.StatusOK,
		Group:  "testgroup",
	}

	assert.False(t, moduleAcceptConsumerGroup(module, status), "Expected StatusOK,testgroup to return False")

	status.Status = protocol.StatusWarning
	assert.True(t, moduleAcceptConsumerGroup(module, status), "Expected StatusWarning,testgroup to return True")

	status.Status = protocol.StatusError
	assert.True(t, moduleAcceptConsumerGroup(module, status), "Expected StatusError,testgroup to return True")

	status.Group = "notagroup"
	assert.False(t, moduleAcceptConsumerGroup(module, status), "Expected StatusError,notagroup to return False")
}

func TestCoordinator_ExecuteTemplate(t *testing.T) {
	tmpl, _ := template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	extras := make(map[string]string)
	extras["foo"] = "bar"
	bytesToSend, err := ExecuteTemplate(tmpl, extras, status, "testidstring", time.Now())
	assert.Nil(t, err, "Expected no error to be returned")
	assert.Equalf(t, "testidstring testcluster testgroup OK", bytesToSend.String(), "Unexpected, got: %v", bytesToSend.String())
}

func TestCoordinator_manageEvalLoop_Start(t *testing.T) {
	coordinator := fixtureCoordinator()

	mockTicker := helpers.MockTicker{}
	mockTicker.On("Start")

	// Add mock calls for the Zookeeper client - Lock immediately returns with no error
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").Return(nil)
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

	coordinator.evalInterval = &mockTicker
	go coordinator.manageEvalLoop()
	time.Sleep(200 * time.Millisecond)

	mockTicker.AssertExpectations(t)
	mockLock.AssertExpectations(t)
	mockZk.AssertExpectations(t)
}

func TestCoordinator_manageEvalLoop_Expiration(t *testing.T) {
	coordinator := fixtureCoordinator()

	mockTicker := helpers.MockTicker{}
	mockTicker.On("Start").Once()
	mockTicker.On("Stop").Once()

	// Add mock calls for the Zookeeper client - Lock immediately returns with no error
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").Return(nil)
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

	coordinator.evalInterval = &mockTicker
	go coordinator.manageEvalLoop()
	time.Sleep(200 * time.Millisecond)

	// ZK gets disconnected and expired
	coordinator.App.ZookeeperConnected = false
	coordinator.App.ZookeeperExpired.Broadcast()
	time.Sleep(300 * time.Millisecond)

	mockTicker.AssertExpectations(t)
	mockLock.AssertExpectations(t)
	mockZk.AssertExpectations(t)
}

func TestCoordinator_manageEvalLoop_Reconnect(t *testing.T) {
	coordinator := fixtureCoordinator()

	mockTicker := helpers.MockTicker{}
	mockTicker.On("Start").Twice()
	mockTicker.On("Stop").Once()

	// Add mock calls for the Zookeeper client - Lock immediately returns with no error
	mockLock := &helpers.MockZookeeperLock{}
	mockLock.On("Lock").Return(nil)
	mockZk := coordinator.App.Zookeeper.(*helpers.MockZookeeperClient)
	mockZk.On("NewLock", "/burrow/notifier").Return(mockLock)

	coordinator.evalInterval = &mockTicker
	go coordinator.manageEvalLoop()
	time.Sleep(200 * time.Millisecond)

	// ZK gets disconnected and expired
	coordinator.App.ZookeeperConnected = false
	coordinator.App.ZookeeperExpired.Broadcast()
	time.Sleep(200 * time.Millisecond)

	// ZK gets reconnected
	coordinator.App.ZookeeperConnected = true
	time.Sleep(300 * time.Millisecond)

	mockTicker.AssertExpectations(t)
	mockLock.AssertExpectations(t)
	mockZk.AssertExpectations(t)
}
