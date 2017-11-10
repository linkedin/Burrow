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
	"encoding/json"
	"fmt"
	"net/http"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureHttpNotifier() *HttpNotifier {
	module := HttpNotifier{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{}

	viper.Reset()
	viper.Set("notifier.test.class-name", "http")
	viper.Set("notifier.test.url-open", "url_open")
	viper.Set("notifier.test.url-close", "url_close")
	viper.Set("notifier.test.template-open", "template_open")
	viper.Set("notifier.test.template-close", "template_close")
	viper.Set("notifier.test.send-close", false)

	return &module
}

func TestHttpNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(HttpNotifier))
	assert.Implements(t, (*Module)(nil), new(HttpNotifier))
}

func TestHttpNotifier_Configure(t *testing.T) {
	module := fixtureHttpNotifier()

	module.Configure("test", "notifier.test")
	assert.NotNil(t, module.HttpClient, "Expected HttpClient to be set with a client object")
}

func TestHttpNotifier_StartStop(t *testing.T) {
	module := fixtureHttpNotifier()
	module.Configure("test", "notifier.test")

	err := module.Start()
	assert.Nil(t, err, "Expected Start to return no error")
	err = module.Stop()
	assert.Nil(t, err, "Expected Stop to return no error")
}

func TestHttpNotifier_AcceptConsumerGroup(t *testing.T) {
	module := fixtureHttpNotifier()
	module.Configure("test", "notifier.test")

	// Should always return true
	assert.True(t, module.AcceptConsumerGroup(&protocol.ConsumerGroupStatus{}), "Expected any status to return True")
}

// Struct that will be used for sending HTTP requests for testing
type HttpRequest struct {
	Template string
	Id       string
	Cluster  string
	Group    string
}

func TestHttpNotifier_Notify_Open(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func(w http.ResponseWriter, r *http.Request) {
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
	viper.Set("notifier.test.url-open", ts.URL)

	// Template sends the ID, cluster, and group
	module.templateOpen, _ = template.New("test").Parse("{\"template\":\"template_open\",\"id\":\"{{.Id}}\",\"cluster\":\"{{.Cluster}}\",\"group\":\"{{.Group}}\"}")

	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), false)
}

func TestHttpNotifier_Notify_Close(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func(w http.ResponseWriter, r *http.Request) {
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
	viper.Set("notifier.test.send-close", true)
	viper.Set("notifier.test.url-close", ts.URL)

	// Template sends the ID, cluster, and group
	module.templateClose, _ = template.New("test").Parse("{\"template\":\"template_close\",\"id\":\"{{.Id}}\",\"cluster\":\"{{.Cluster}}\",\"group\":\"{{.Group}}\"}")

	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), true)
}
