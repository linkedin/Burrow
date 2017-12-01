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
	"fmt"
	"regexp"
	"sync"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	viper.Set("notifier.test.interval", 5)
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
	coordinator.clusters["deleteme"] = &clusterGroups{}

	// This goroutine will receive the storage request for a cluster list, and respond with an appropriate list
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchClusters, request.RequestType, "Expected request type to be StorageFetchClusters, not %v", request.RequestType)

		// Send a response back with a cluster list, which will trigger another storage request
		request.Reply <- []string{"testcluster"}

		// This goroutine will receive the storage request for a consumer list, and respond with an appropriate list
		// We don't start it until after the first request is received, because otherwise we'll have a race condition
		wg.Add(1)
		go func() {
			defer wg.Done()
			request := <-coordinator.App.StorageChannel
			assert.Equalf(t, protocol.StorageFetchConsumers, request.RequestType, "Expected request type to be StorageFetchConsumers, not %v", request.RequestType)
			assert.Equalf(t, "testcluster", request.Cluster, "Expected request cluster to be testcluster, not %v", request.RequestType)

			// Send back the group list response, and sleep for a moment after that (otherwise we'll be racing the goroutine that updates the cluster groups
			request.Reply <- []string{"testgroup"}
		}()
	}()

	coordinator.sendClusterRequest()
	coordinator.running.Wait()
	wg.Wait()

	assert.Lenf(t, coordinator.clusters, 1, "Expected 1 entry in the clusters map, not %v", len(coordinator.clusters))
	cluster, ok := coordinator.clusters["testcluster"]
	assert.True(t, ok, "Expected clusters map key to be testcluster")
	assert.Lenf(t, cluster.Groups, 1, "Expected 1 entry in the group list, not %v", len(cluster.Groups))
	_, ok = cluster.Groups["testgroup"]
	assert.True(t, ok, "Expected group to be testgroup")
}

// Note, we do not check the calls to the module here, just that the response loop sets the event properly
func TestCoordinator_responseLoop_NotFound(t *testing.T) {
	coordinator := fixtureCoordinator()

	// For NotFound, we expect the notifier will not be called at all
	coordinator.notifyModuleFunc = func(module Module, status *protocol.ConsumerGroupStatus, startTime time.Time, eventId string) {
		defer coordinator.running.Done()
		assert.Fail(t, "Expected notifyModule to not be called")
	}

	coordinator.Configure()

	// A test cluster and group to receive response for
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		LastNotify: make(map[string]time.Time),
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

	coordinator.running.Add(1)
	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equalf(t, "", group.ID, "Expected group incident ID to be empty, not %v", group.ID)
	assert.True(t, group.Start.IsZero(), "Expected group incident start time to be unset")
	assert.True(t, group.LastNotify["test"].IsZero(), "Expected group last time to be unset")
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
		defer coordinator.running.Done()
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseOK, status, "Expected to be called with responseOK as the status")
		assert.True(t, startTime.IsZero(), "Expected to be called with zero value startTime")
		assert.Equal(t, "", eventId, "Expected to be called with empty eventId")
	}

	coordinator.Configure()

	// A test cluster and group to receive response for
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		LastNotify: make(map[string]time.Time),
	}

	// Test an OK response
	go func() {
		coordinator.evaluatorResponse <- responseOK

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.running.Add(1)
	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equalf(t, "", group.ID, "Expected group incident ID to be empty, not %v", group.ID)
	assert.True(t, group.Start.IsZero(), "Expected group incident start time to be unset")
	assert.True(t, group.LastNotify["test"].IsZero(), "Expected group last time to be unset")

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
		defer coordinator.running.Done()
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseOK, status, "Expected to be called with responseOK as the status")
		assert.Equal(t, mockStartTime, startTime, "Expected to be called with mockStartTime as the startTime")
		assert.Equalf(t, "testidstring", eventId, "Expected to be called with eventId as 'testidstring', not %v", eventId)
	}

	coordinator.Configure()

	// A test cluster and group to receive response for, with the incident active
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		Start:      mockStartTime,
		ID:         "testidstring",
		LastNotify: make(map[string]time.Time),
	}

	// Test an OK response
	go func() {
		coordinator.evaluatorResponse <- responseOK

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.running.Add(1)
	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equalf(t, "", group.ID, "Expected group incident ID to be empty, not %v", group.ID)
	assert.True(t, group.Start.IsZero(), "Expected group incident start time to be cleared")
	assert.True(t, group.LastNotify["test"].IsZero(), "Expected group last time to be unset")

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
		defer coordinator.running.Done()
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseError, status, "Expected to be called with responseError as the status")
		assert.False(t, startTime.IsZero(), "Expected to be called with a valid startTime, not zero")
		assert.NotEqual(t, "", eventId, "Expected to be called with a new eventId, not empty")
	}

	coordinator.Configure()

	// A test cluster and group to receive response for
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		LastNotify: make(map[string]time.Time),
	}

	// Test an Error response
	go func() {
		coordinator.evaluatorResponse <- responseError

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.running.Add(1)
	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.NotEqual(t, "", group.ID, "Expected group incident ID to be set, not empty")
	assert.False(t, group.Start.IsZero(), "Expected group incident start time to be set")
	assert.True(t, group.LastNotify["test"].IsZero(), "Expected group last time to be unset (as real notifyFunc was not called)")

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
		defer coordinator.running.Done()
		assert.Equal(t, "test", module.GetName(), "Expected to be called with the null notifier module")
		assert.Equal(t, responseError, status, "Expected to be called with responseError as the status")
		assert.Equal(t, mockStartTime, startTime, "Expected to be called with mockStartTime as the startTime")
		assert.Equalf(t, "testidstring", eventId, "Expected to be called with eventId as 'testidstring', not %v", eventId)
	}

	coordinator.Configure()

	// A test cluster and group to receive response for, with the incident active
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		Start:      mockStartTime,
		ID:         "testidstring",
		LastNotify: make(map[string]time.Time),
	}

	// Test an Error response
	go func() {
		coordinator.evaluatorResponse <- responseError

		// After a short wait, close the quit channel to release the responseLoop
		time.Sleep(100 * time.Millisecond)
		close(coordinator.quitChannel)
	}()

	coordinator.running.Add(1)
	coordinator.responseLoop()
	coordinator.running.Wait()
	close(coordinator.evaluatorResponse)
	time.Sleep(100 * time.Millisecond)

	group := coordinator.clusters["testcluster"].Groups["testgroup"]
	assert.Equal(t, mockStartTime, group.Start, "Expected group incident start time to be unchanged")
	assert.Equalf(t, "testidstring", group.ID, "Expected group incident ID to be 'testidstring', not %v", group.ID)
	assert.True(t, group.LastNotify["test"].IsZero(), "Expected group last time to be unset (as real notifyFunc was not called)")

	module := coordinator.modules["test"].(*NullNotifier)
	assert.True(t, module.CalledAcceptConsumerGroup, "Expected module 'test' AcceptConsumerGroup to be called")
}

var notifyModuleTests = []struct {
	Threshold   int
	Status      protocol.StatusConstant
	Existing    bool
	SendClose   bool
	ExpectSend  bool
	ExpectClose bool
	ExpectID    bool
}{
	/*{1, 0, false, false, false, false, false},
	{2, 0, false, false, false, false, false},
	{1, 0, true, false, false, false, false},
	{1, 0, false, true, false, false, false},
	{1, 0, true, true, false, false, false}, */

	{1, 1, false, false, true, false, false},
	{1, 1, false, true, true, false, false},
	{1, 1, true, false, true, false, false},
	{1, 1, true, true, true, true, false},

	{1, 2, false, false, true, false, true},
	{1, 2, false, true, true, false, true},
	{1, 2, true, false, true, false, true},
	{1, 2, true, true, true, false, true},

	{3, 2, false, false, false, false, true},
	{3, 2, false, true, false, false, true},
	{3, 2, true, false, false, false, true},
	{3, 2, true, true, false, false, true},

	{2, 1, false, false, false, false, false},
	{2, 1, false, true, false, false, false},
	{2, 1, true, false, false, false, false},
	{2, 1, true, true, true, true, false},
}

func TestCoordinator_checkAndSendResponseToModules(t *testing.T) {
	// We don't need to configure the coordinator - just set up the data structures
	coordinator := fixtureCoordinator()
	coordinator.modules = make(map[string]protocol.Module)
	coordinator.clusters = make(map[string]*clusterGroups)
	coordinator.notifyModuleFunc = coordinator.notifyModule
	coordinator.clusterLock = &sync.RWMutex{}

	// A test cluster and group to send notification for (so the func can check/set last time)
	coordinator.clusters["testcluster"] = &clusterGroups{
		Lock:   &sync.RWMutex{},
		Groups: make(map[string]*consumerGroup),
	}
	coordinator.clusters["testcluster"].Groups["testgroup"] = &consumerGroup{
		LastNotify: make(map[string]time.Time),
	}

	mockStartTime, _ := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")

	for i, testSet := range notifyModuleTests {
		fmt.Printf("Running test %v - %v\n", i, testSet)

		// Set the module threshold and send-close configs
		viper.Reset()
		viper.Set("notifier.test.threshold", testSet.Threshold)
		viper.Set("notifier.test.send-close", testSet.SendClose)

		// Should there be an existing incident for this test?
		group := coordinator.clusters["testcluster"].Groups["testgroup"]
		delete(group.LastNotify, "test")
		if testSet.Existing {
			group.ID = "testidstring"
			group.Start = mockStartTime
		} else {
			group.ID = ""
			group.Start = time.Time{}
		}

		// Set up the response status to send
		// Sending i as the TotalPartitions is a hack to make mock errors a little easier to understand
		response := &protocol.ConsumerGroupStatus{
			Cluster:         "testcluster",
			Group:           "testgroup",
			Status:          testSet.Status,
			TotalPartitions: i,
		}

		// Set up the mock module and expected calls
		mockModule := &helpers.MockModule{}
		coordinator.modules["test"] = mockModule
		mockModule.On("GetName").Return("test")
		mockModule.On("GetGroupWhitelist").Return((*regexp.Regexp)(nil))
		mockModule.On("GetGroupBlacklist").Return((*regexp.Regexp)(nil))
		mockModule.On("AcceptConsumerGroup", response).Return(true)
		if testSet.ExpectSend {
			mockModule.On("Notify", response, mock.MatchedBy(func(s string) bool { return true }), mock.MatchedBy(func(t time.Time) bool { return true }), testSet.ExpectClose).Return()
		}

		// Call the func with a response that has the appropriate status
		coordinator.running.Add(1)
		coordinator.checkAndSendResponseToModules(response)

		mockModule.AssertExpectations(t)
		if testSet.ExpectSend && (!testSet.ExpectClose) {
			assert.Falsef(t, group.LastNotify["test"].IsZero(), "Test %v: Expected group last time to be set", i)
		} else {
			assert.Truef(t, group.LastNotify["test"].IsZero(), "Test %v: Expected group last time to remain unset", i)
		}

		// Check whether or not the incident is as expected afterwards
		if testSet.ExpectID {
			if testSet.Existing {
				assert.Equalf(t, "testidstring", group.ID, "Test %v: Expected group incident ID to be testidstring, not %v", i, group.ID)
				assert.Equalf(t, mockStartTime, group.Start, "Test %v: Expected group incident start time to be mock time, not %v", i, group.Start)
			} else {
				assert.NotEqualf(t, "", group.ID, "Test %v: Expected group incident ID to be not empty", i)
				assert.Falsef(t, group.Start.IsZero(), "Test %v: Expected group incident start time to be set", i)
			}
		} else {
			assert.Equalf(t, "", group.ID, "Test %v: Expected group incident ID to be empty, not %v", i, group.ID)
			assert.Truef(t, group.Start.IsZero(), "Test %v: Expected group incident start time to be unset", i)
		}
	}
}

func TestCoordinator_ExecuteTemplate(t *testing.T) {
	tmpl, _ := template.New("test").Parse("{{.ID}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	extras := make(map[string]string)
	extras["foo"] = "bar"
	bytesToSend, err := executeTemplate(tmpl, extras, status, "testidstring", time.Now())
	assert.Nil(t, err, "Expected no error to be returned")
	assert.Equalf(t, "testidstring testcluster testgroup OK", bytesToSend.String(), "Unexpected, got: %v", bytesToSend.String())
}
