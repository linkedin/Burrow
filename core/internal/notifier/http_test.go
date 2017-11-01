package notifier

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
)

func fixtureHttpNotifier() *HttpNotifier {
	module := HttpNotifier{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		Configuration:  &configuration.Configuration{},
	}

	module.App.Configuration.HttpNotifierProfile = make(map[string]*configuration.HttpNotifierProfile)
	module.App.Configuration.HttpNotifierProfile["test_http_profile"] = &configuration.HttpNotifierProfile{
		UrlOpen:     "url open",
		UrlClose:    "url close",
		MethodOpen:  "POST",
		MethodClose: "POST",
	}

	module.App.Configuration.Notifier = make(map[string]*configuration.NotifierConfig)
	module.App.Configuration.Notifier["test"] = &configuration.NotifierConfig{
		ClassName:      "http",
		Profile:        "test_http_profile",
		GroupWhitelist: ".*",
		Interval:       123,
		Threshold:      1,
		Timeout:        2,
		Keepalive:      10,
		TemplateOpen:   "template_open",
		TemplateClose:  "template_close",
		Extras:         []string{"foo=bar"},
		SendClose:      false,
	}

	// Simple parser replacement that returns an error
	module.templateParseFunc = func (filenames ...string) (*template.Template, error) {
		return template.New("test").Parse("")
	}

	return &module
}

func TestHttpNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(HttpNotifier))
	assert.Implements(t, (*NotifierModule)(nil), new(HttpNotifier))
}

func TestHttpNotifier_Configure(t *testing.T) {
	module := fixtureHttpNotifier()

	// Simple parser replacement that returns an empty template using a string, or throws an error on unexpected use for this test
	module.templateParseFunc = func (filenames ...string) (*template.Template, error) {
		if len(filenames) != 1 {
			return nil, errors.New("expected exactly 1 filename")
		}
		if filenames[0] == "template_close" {
			return nil, errors.New("close template should not be parsed")
		}
		return template.New("test").Parse("")
	}
	module.Configure("test")
	assert.NotNil(t, module.HttpClient, "Expected HttpClient to be set with a client object")
	assert.NotNil(t, module.groupWhitelist, "Expected groupWhitelist to be set with a regular expression")
	assert.NotNil(t, module.templateOpen, "Expected templateOpen to be set with a template")
	assert.Nil(t, module.templateClose, "Expected templateClose to not be set")
	assert.Equalf(t, int64(123), module.myConfiguration.Interval, "Expected Interval to get set to 123, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 1, module.myConfiguration.Threshold, "Expected Threshold to get set to 1, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 2, module.myConfiguration.Timeout, "Expected Timeout to get set to 2, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 10, module.myConfiguration.Keepalive, "Expected Keepalive to get set to 10, not %v", module.myConfiguration.Interval)
	assert.Lenf(t, module.extras, 1, "Expected exactly one extra entry to be set, not %v", len(module.extras))
	val, ok := module.extras["foo"]
	assert.True(t, ok, "Expected extras key to be 'foo'")
	assert.Equalf(t, "bar", val, "Expected value of extras 'foo' to be 'bar', not %v", val)
}

func TestHttpNotifier_Configure_Defaults(t *testing.T) {
	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].Interval = 0
	module.App.Configuration.Notifier["test"].Threshold = 0
	module.App.Configuration.Notifier["test"].Timeout = 0
	module.App.Configuration.Notifier["test"].Keepalive = 0
	module.App.Configuration.Notifier["test"].Extras = []string{}

	module.Configure("test")
	assert.Equalf(t, int64(60), module.myConfiguration.Interval, "Expected Interval to get set to 60, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 2, module.myConfiguration.Threshold, "Expected Threshold to get set to 2, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 5, module.myConfiguration.Timeout, "Expected Timeout to get set to 5, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 300, module.myConfiguration.Keepalive, "Expected Keepalive to get set to 300, not %v", module.myConfiguration.Interval)
	assert.Lenf(t, module.extras, 0, "Expected exactly zero extra entries to be set, not %v", len(module.extras))
}

func TestKafkaZkClient_Configure_BadRegexp(t *testing.T) {
	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].GroupWhitelist = "["

	assert.Panics(t, func() { module.Configure("test") }, "The code did not panic")
}

func TestKafkaZkClient_Configure_BadTemplate(t *testing.T) {
	module := fixtureHttpNotifier()

	// Simple parser replacement that returns an error
	module.templateParseFunc = func (filenames ...string) (*template.Template, error) {
		return nil, errors.New("bad template")
	}

	assert.Panics(t, func() { module.Configure("test") }, "The code did not panic")
}

func TestHttpNotifier_Configure_SendClose(t *testing.T) {
	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].SendClose = true

	// Simple parser replacement that returns an empty template using a string, or throws an error on unexpected use for this test
	module.templateParseFunc = func (filenames ...string) (*template.Template, error) {
		if len(filenames) != 1 {
			return nil, errors.New("expected exactly 1 filename")
		}
		return template.New("test").Parse("")
	}
	module.Configure("test")
	assert.NotNil(t, module.groupWhitelist, "Expected groupWhitelist to be set with a regular expression")
	assert.NotNil(t, module.templateOpen, "Expected templateOpen to be set with a template")
	assert.NotNil(t, module.templateClose, "Expected templateClose to be set with a template")
}

func TestHttpNotifier_StartStop(t *testing.T) {
	module := fixtureHttpNotifier()
	module.Configure("test")

	err := module.Start()
	assert.Nil(t, err, "Expected Start to return no error")
	err = module.Stop()
	assert.Nil(t, err, "Expected Stop to return no error")
}

func TestHttpNotifier_AcceptConsumerGroup(t *testing.T) {
	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].Threshold = 2
	module.App.Configuration.Notifier["test"].GroupWhitelist = "test.*"
	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status: protocol.StatusOK,
		Group:  "testgroup",
	}

	assert.False(t, module.AcceptConsumerGroup(status), "Expected StatusOK,testgroup to return False")

	status.Status = protocol.StatusWarning
	assert.True(t, module.AcceptConsumerGroup(status), "Expected StatusWarning,testgroup to return True")

	status.Status = protocol.StatusError
	assert.True(t, module.AcceptConsumerGroup(status), "Expected StatusError,testgroup to return True")

	status.Group = "notagroup"
	assert.False(t, module.AcceptConsumerGroup(status), "Expected StatusError,notagroup to return False")
}

// Struct that will be used for sending HTTP requests for testing
type HttpRequest struct {
	Template  string
	Id        string
	Cluster   string
	Group     string
}

func TestHttpNotifier_sendNotification_Open(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func( w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var req HttpRequest
		err := decoder.Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// If there's any error, we're going to panic to stop everything
		if req.Template != "template_open" {
			panic("bad template name")
		}
		if req.Id != "testidstring" {
			panic("bad id")
		}
		if req.Cluster != "testcluster" {
			panic("bad cluster")
		}
		if req.Group != "testgroup" {
			panic("bad group")
		}
		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureHttpNotifier()
	module.App.Configuration.HttpNotifierProfile["test_http_profile"].UrlOpen = ts.URL

	// Template sends the ID, cluster, and group
	module.templateParseFunc = func (filenames ...string) (*template.Template, error) {
		return template.New("test").Parse("{\"template\":\"" + filenames[0] + "\",\"id\":\"{{.Id}}\",\"cluster\":\"{{.Cluster}}\",\"group\":\"{{.Group}}\"}")
	}

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}
	event := &Event{
		Id:    "testidstring",
		Start: time.Now(),
		Last:  time.Now(),
	}

	module.sendNotification(status, event, false, module.Log)
}

func TestHttpNotifier_sendNotification_Close(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func( w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var req HttpRequest
		err := decoder.Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// If there's any error, we're going to panic to stop everything
		if req.Template != "template_close" {
			panic("bad template name")
		}
		if req.Id != "testidstring" {
			panic("bad id")
		}
		if req.Cluster != "testcluster" {
			panic("bad cluster")
		}
		if req.Group != "testgroup" {
			panic("bad group")
		}
		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].SendClose = true
	module.App.Configuration.HttpNotifierProfile["test_http_profile"].UrlClose = ts.URL

	// Template sends the ID, cluster, and group
	module.templateParseFunc = func (filenames ...string) (*template.Template, error) {
		return template.New("test").Parse("{\"template\":\"" + filenames[0] + "\",\"id\":\"{{.Id}}\",\"cluster\":\"{{.Cluster}}\",\"group\":\"{{.Group}}\"}")
	}

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}
	event := &Event{
		Id:    "testidstring",
		Start: time.Now(),
		Last:  time.Now(),
	}

	module.sendNotification(status, event, true, module.Log)
}

// Mock for sendNotification
type MockSendNotification struct {
	mock.Mock
}
func (m *MockSendNotification) sendNotification (status *protocol.ConsumerGroupStatus, event *Event, stateGood bool, logger *zap.Logger) {
	m.Called(status, event, stateGood, logger)
}

func TestHttpNotifier_Notify_GoodNotOpen(t *testing.T) {
	mockSend := &MockSendNotification{}

	module := fixtureHttpNotifier()
	module.sendNotificationFunc = mockSend.sendNotification
	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	// Notify should return without sending a notification
	module.Notify(status)
	mockSend.AssertExpectations(t)
}

func TestHttpNotifier_Notify_GoodOpen_NoClose(t *testing.T) {
	mockSend := &MockSendNotification{}

	module := fixtureHttpNotifier()
	module.sendNotificationFunc = mockSend.sendNotification
	module.Configure("test")

	module.groupIds["testcluster"] = make(map[string]*Event)
	module.groupIds["testcluster"]["testgroup"] = &Event{
		Id:    "testidstring",
		Start: time.Now(),
		Last:  time.Now(),
	}

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	// Notify should return without sending a notification
	module.Notify(status)
	mockSend.AssertExpectations(t)

	// map entry for the group should be removed
	clustermap, ok := module.groupIds["testcluster"]
	assert.True(t, ok, "Expected event map entry for the cluster to still exist")
	_, ok = clustermap["testgroup"]
	assert.False(t, ok, "Expected event map entry for the group to be removed")
}

func TestHttpNotifier_Notify_GoodOpen_SendClose(t *testing.T) {
	mockSend := &MockSendNotification{}

	module := fixtureHttpNotifier()
	module.sendNotificationFunc = mockSend.sendNotification
	module.App.Configuration.Notifier["test"].SendClose = true
	module.Configure("test")

	module.groupIds["testcluster"] = make(map[string]*Event)
	module.groupIds["testcluster"]["testgroup"] = &Event{
		Id:    "testidstring",
		Start: time.Now(),
		Last:  time.Now(),
	}

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}
	mockSend.On("sendNotification", status, module.groupIds["testcluster"]["testgroup"], true, mock.MatchedBy(func(logger *zap.Logger) bool { return logger != nil }))

	// Notify should send a notification
	module.Notify(status)
	mockSend.AssertExpectations(t)

	// map entry for the group should be removed
	clustermap, ok := module.groupIds["testcluster"]
	assert.True(t, ok, "Expected event map entry for the cluster to still exist")
	_, ok = clustermap["testgroup"]
	assert.False(t, ok, "Expected event map entry for the group to be removed")
}

func TestHttpNotifier_Notify_BadNotOpen(t *testing.T) {
	mockSend := &MockSendNotification{}

	module := fixtureHttpNotifier()
	module.sendNotificationFunc = mockSend.sendNotification
	module.App.Configuration.Notifier["test"].SendClose = true
	module.App.Configuration.Notifier["test"].Threshold = 2
	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusError,
		Cluster: "testcluster",
		Group:   "testgroup",
	}
	mockSend.On("sendNotification", status, mock.MatchedBy(func(event *Event) bool { return event != nil }), false, mock.MatchedBy(func(logger *zap.Logger) bool { return logger != nil }))

	// Notify should send a notification
	module.Notify(status)
	mockSend.AssertExpectations(t)

	// map entry for the group should exist
	clustermap, ok := module.groupIds["testcluster"]
	assert.True(t, ok, "Expected event map entry for the cluster to exist")
	_, ok = clustermap["testgroup"]
	assert.True(t, ok, "Expected event map entry for the group to exist")
}

func TestHttpNotifier_Notify_BadOpen_TooSoon(t *testing.T) {
	mockSend := &MockSendNotification{}

	module := fixtureHttpNotifier()
	module.sendNotificationFunc = mockSend.sendNotification
	module.App.Configuration.Notifier["test"].SendClose = true
	module.App.Configuration.Notifier["test"].Threshold = 2
	module.App.Configuration.Notifier["test"].Interval = 300
	module.Configure("test")

	ourTime := time.Now().Add(-100 * time.Millisecond)
	module.groupIds["testcluster"] = make(map[string]*Event)
	module.groupIds["testcluster"]["testgroup"] = &Event{
		Id:    "testidstring",
		Start: ourTime,
		Last:  ourTime,
	}

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusError,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	// Notify should not send a notification
	module.Notify(status)
	mockSend.AssertExpectations(t)

	// map entry for the group should exist
	clustermap, ok := module.groupIds["testcluster"]
	assert.True(t, ok, "Expected event map entry for the cluster to exist")
	_, ok = clustermap["testgroup"]
	assert.True(t, ok, "Expected event map entry for the group to exist")
}

func TestHttpNotifier_Notify_BadOpen(t *testing.T) {
	mockSend := &MockSendNotification{}

	module := fixtureHttpNotifier()
	module.sendNotificationFunc = mockSend.sendNotification
	module.App.Configuration.Notifier["test"].SendClose = true
	module.App.Configuration.Notifier["test"].Threshold = 2
	module.App.Configuration.Notifier["test"].Interval = 300
	module.Configure("test")

	ourTime := time.Now().Add(-(time.Duration(module.App.Configuration.Notifier["test"].Interval) * 2) * time.Second)
	module.groupIds["testcluster"] = make(map[string]*Event)
	module.groupIds["testcluster"]["testgroup"] = &Event{
		Id:    "testidstring",
		Start: ourTime,
		Last:  ourTime,
	}

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusError,
		Cluster: "testcluster",
		Group:   "testgroup",
	}
	mockSend.On("sendNotification", status, module.groupIds["testcluster"]["testgroup"], false, mock.MatchedBy(func(logger *zap.Logger) bool { return logger != nil }))

	// Notify should send a notification
	module.Notify(status)
	mockSend.AssertExpectations(t)

	// map entry for the group should exist
	clustermap, ok := module.groupIds["testcluster"]
	assert.True(t, ok, "Expected event map entry for the cluster to exist")
	_, ok = clustermap["testgroup"]
	assert.True(t, ok, "Expected event map entry for the group to exist")

	// event last time should be updated, start time should be the same
	assert.True(t, ourTime.Before(module.groupIds["testcluster"]["testgroup"].Last), "Expected event Last time to be updated")
	assert.Equal(t, ourTime, module.groupIds["testcluster"]["testgroup"].Start, "Expected event Start time to remain unchanged")
}
