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
	"net/http/httptest"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureSlackNotifier() *SlackNotifier {
	module := SlackNotifier{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		Configuration: &configuration.Configuration{},
	}

	module.App.Configuration.SlackNotifierProfile = make(map[string]*configuration.SlackNotifierProfile)
	module.App.Configuration.SlackNotifierProfile["test_slack_profile"] = &configuration.SlackNotifierProfile{
		Token:     "testtoken",
		Channel:   "#testchannel",
		Username:  "testuser",
		IconEmoji: ":shrug:",
	}

	module.App.Configuration.Notifier = make(map[string]*configuration.NotifierConfig)
	module.App.Configuration.Notifier["test"] = &configuration.NotifierConfig{
		ClassName:     "slack",
		Profile:       "test_slack_profile",
		Timeout:       2,
		Keepalive:     10,
		TemplateOpen:  "template_open",
		TemplateClose: "template_close",
		SendClose:     false,
	}

	return &module
}

func TestSlackNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(SlackNotifier))
	assert.Implements(t, (*Module)(nil), new(SlackNotifier))
}

func TestSlackNotifier_Configure(t *testing.T) {
	module := fixtureSlackNotifier()

	module.Configure("test")
	assert.NotNil(t, module.HttpClient, "Expected HttpClient to be set with a client object")
	assert.Equalf(t, 2, module.myConfiguration.Timeout, "Expected Timeout to get set to 2, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 10, module.myConfiguration.Keepalive, "Expected Keepalive to get set to 10, not %v", module.myConfiguration.Interval)
}

func TestSlackNotifier_Configure_Defaults(t *testing.T) {
	module := fixtureSlackNotifier()
	module.App.Configuration.Notifier["test"].Timeout = 0
	module.App.Configuration.Notifier["test"].Keepalive = 0

	module.Configure("test")
	assert.Equalf(t, 5, module.myConfiguration.Timeout, "Expected Timeout to get set to 5, not %v", module.myConfiguration.Interval)
	assert.Equalf(t, 300, module.myConfiguration.Keepalive, "Expected Keepalive to get set to 300, not %v", module.myConfiguration.Interval)
}

func TestSlackNotifier_Configure_NoToken(t *testing.T) {
	module := fixtureSlackNotifier()
	module.App.Configuration.SlackNotifierProfile["test_slack_profile"].Token = ""

	assert.Panics(t, func() { module.Configure("test") }, "The code did not panic")
}

func TestSlackNotifier_Configure_NoChannel(t *testing.T) {
	module := fixtureSlackNotifier()
	module.App.Configuration.SlackNotifierProfile["test_slack_profile"].Channel = ""

	assert.Panics(t, func() { module.Configure("test") }, "The code did not panic")
}

func TestSlackNotifier_StartStop(t *testing.T) {
	module := fixtureSlackNotifier()
	module.Configure("test")

	err := module.Start()
	assert.Nil(t, err, "Expected Start to return no error")
	err = module.Stop()
	assert.Nil(t, err, "Expected Stop to return no error")
}

func TestSlackNotifier_AcceptConsumerGroup(t *testing.T) {
	module := fixtureSlackNotifier()
	module.Configure("test")

	// Should always return true
	assert.True(t, module.AcceptConsumerGroup(&protocol.ConsumerGroupStatus{}), "Expected any status to return True")
}

func TestSlackNotifier_Notify_Open(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func(w http.ResponseWriter, r *http.Request) {
		// Must get an appropriate Authorization header
		headers, ok := r.Header["Authorization"]
		assert.True(t, ok, "Expected to receive Authorization header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Authorization header")
		assert.Equalf(t, "Bearer testtoken", headers[0], "Expected Authorization header to be 'Bearer testtoken', not '%v'", headers[0])

		// Must get an appropriate Content-Type header
		headers, ok = r.Header["Content-Type"]
		assert.True(t, ok, "Expected to receive Content-Type header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Content-Type header")
		assert.Equalf(t, "application/json", headers[0], "Expected Content-Type header to be 'application/json', not '%v'", headers[0])

		decoder := json.NewDecoder(r.Body)
		var req SlackMessage
		err := decoder.Decode(&req)
		if err != nil {
			assert.Failf(t, "Failed to decode message body", "Failed to decode message body: %v", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Validate each field we expect to receive
		assert.Equalf(t, "#testchannel", req.Channel, "Expected Channel to be #testchannel, not %v", req.Channel)
		assert.Equalf(t, "testuser", req.Username, "Expected Username to be testuser, not %v", req.Username)
		assert.Equalf(t, ":shrug:", req.IconEmoji, "Expected IconEmoji to be :shrug:, not %v", req.IconEmoji)
		assert.Equalf(t, "testidstring testcluster testgroup WARN", req.Text, "Expected Text to be 'testidstring testcluster testgroup WARN', not '%v'", req.Text)

		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureSlackNotifier()
	module.postURL = ts.URL

	// Template sends the ID, cluster, and group
	module.templateOpen, _ = template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), false)
}

func TestSlackNotifier_Notify_Close(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func(w http.ResponseWriter, r *http.Request) {
		// Must get an appropriate Authorization header
		headers, ok := r.Header["Authorization"]
		assert.True(t, ok, "Expected to receive Authorization header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Authorization header")
		assert.Equalf(t, "Bearer testtoken", headers[0], "Expected Authorization header to be 'Bearer testtoken', not '%v'", headers[0])

		// Must get an appropriate Content-Type header
		headers, ok = r.Header["Content-Type"]
		assert.True(t, ok, "Expected to receive Content-Type header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Content-Type header")
		assert.Equalf(t, "application/json", headers[0], "Expected Content-Type header to be 'application/json', not '%v'", headers[0])

		decoder := json.NewDecoder(r.Body)
		var req SlackMessage
		err := decoder.Decode(&req)
		if err != nil {
			assert.Failf(t, "Failed to decode message body", "Failed to decode message body: %v", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Validate each field we expect to receive
		assert.Equalf(t, "#testchannel", req.Channel, "Expected Channel to be #testchannel, not %v", req.Channel)
		assert.Equalf(t, "testuser", req.Username, "Expected Username to be testuser, not %v", req.Username)
		assert.Equalf(t, ":shrug:", req.IconEmoji, "Expected IconEmoji to be :shrug:, not %v", req.IconEmoji)
		assert.Equalf(t, "testidstring testcluster testgroup WARN", req.Text, "Expected Text to be 'testidstring testcluster testgroup WARN', not '%v'", req.Text)

		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureSlackNotifier()
	module.App.Configuration.Notifier["test"].SendClose = true
	module.postURL = ts.URL

	// Template sends the ID, cluster, and group
	module.templateClose, _ = template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), true)
}
