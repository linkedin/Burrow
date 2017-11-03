package notifier

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/http"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
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
		Timeout:        2,
		Keepalive:      10,
		TemplateOpen:   "template_open",
		TemplateClose:  "template_close",
		SendClose:      false,
	}

	return &module
}

func TestHttpNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(HttpNotifier))
	assert.Implements(t, (*Module)(nil), new(HttpNotifier))
}

func TestHttpNotifier_Configure(t *testing.T) {
	module := fixtureHttpNotifier()

	module.Configure("test")
	assert.NotNil(t, module.HttpClient, "Expected HttpClient to be set with a client object")
	assert.Equalf(t, 2, module.myConfiguration.Timeout, "Expected Timeout to get set to 2, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 10, module.myConfiguration.Keepalive, "Expected Keepalive to get set to 10, not %v", module.myConfiguration.Interval)
}

func TestHttpNotifier_Configure_Defaults(t *testing.T) {
	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].Timeout = 0
	module.App.Configuration.Notifier["test"].Keepalive = 0

	module.Configure("test")
	assert.Equalf(t, 5, module.myConfiguration.Timeout, "Expected Timeout to get set to 5, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 300, module.myConfiguration.Keepalive, "Expected Keepalive to get set to 300, not %v", module.myConfiguration.Interval)
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
	module.Configure("test")

	// Should always return true
	assert.True(t, module.AcceptConsumerGroup(&protocol.ConsumerGroupStatus{}), "Expected any status to return True")
}

// Struct that will be used for sending HTTP requests for testing
type HttpRequest struct {
	Template  string
	Id        string
	Cluster   string
	Group     string
}

func TestHttpNotifier_Notify_Open(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func( w http.ResponseWriter, r *http.Request) {
		// Must get an appropriate Content-Type header
		headers, ok := r.Header["Content-Type"]
		assert.True(t, ok, "Expected to receive Content-Type header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Content-Type header")
		assert.Equalf(t, "application/json", headers[0], "Expected Content-Type header to be 'application/json', not '%v'", headers[0])

		decoder := json.NewDecoder(r.Body)
		var req HttpRequest
		err := decoder.Decode(&req)
		if err != nil {
			assert.Failf(t, "Failed to decode message body", "Failed to decode message body: %v", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		assert.Equalf(t, "template_open", req.Template, "Expected Template to be template_open, not %v", req.Template)
		assert.Equalf(t, "testidstring", req.Id, "Expected Id to be testidstring, not %v", req.Id)
		assert.Equalf(t, "testcluster", req.Cluster, "Expected Cluster to be testcluster, not %v", req.Cluster)
		assert.Equalf(t, "testgroup", req.Group, "Expected Group to be testgroup, not %v", req.Group)

		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureHttpNotifier()
	module.App.Configuration.HttpNotifierProfile["test_http_profile"].UrlOpen = ts.URL

	// Template sends the ID, cluster, and group
	module.templateOpen, _ = template.New("test").Parse("{\"template\":\"template_open\",\"id\":\"{{.Id}}\",\"cluster\":\"{{.Cluster}}\",\"group\":\"{{.Group}}\"}")

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), false)
}

func TestHttpNotifier_Notify_Close(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func( w http.ResponseWriter, r *http.Request) {
		// Must get an appropriate Content-Type header
		headers, ok := r.Header["Content-Type"]
		assert.True(t, ok, "Expected to receive Content-Type header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Content-Type header")
		assert.Equalf(t, "application/json", headers[0], "Expected Content-Type header to be 'application/json', not '%v'", headers[0])

		decoder := json.NewDecoder(r.Body)
		var req HttpRequest
		err := decoder.Decode(&req)
		if err != nil {
			assert.Failf(t, "Failed to decode message body", "Failed to decode message body: %v", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		assert.Equalf(t, "template_close", req.Template, "Expected Template to be template_close, not %v", req.Template)
		assert.Equalf(t, "testidstring", req.Id, "Expected Id to be testidstring, not %v", req.Id)
		assert.Equalf(t, "testcluster", req.Cluster, "Expected Cluster to be testcluster, not %v", req.Cluster)
		assert.Equalf(t, "testgroup", req.Group, "Expected Group to be testgroup, not %v", req.Group)

		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureHttpNotifier()
	module.App.Configuration.Notifier["test"].SendClose = true
	module.App.Configuration.HttpNotifierProfile["test_http_profile"].UrlClose = ts.URL

	// Template sends the ID, cluster, and group
	module.templateClose, _ = template.New("test").Parse("{\"template\":\"template_close\",\"id\":\"{{.Id}}\",\"cluster\":\"{{.Cluster}}\",\"group\":\"{{.Group}}\"}")

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), true)
}
