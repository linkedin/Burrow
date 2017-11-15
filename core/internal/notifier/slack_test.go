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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"text/template"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureSlackNotifier() *SlackNotifier {
	module := SlackNotifier{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{}

	viper.Reset()
	viper.Set("notifier.test.class-name", "slack")
	viper.Set("notifier.test.template-open", "template_open")
	viper.Set("notifier.test.template-close", "template_close")
	viper.Set("notifier.test.send-close", false)
	viper.Set("notifier.test.url", "http://example.co")

	return &module
}

func TestSlackNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(SlackNotifier))
	assert.Implements(t, (*Module)(nil), new(SlackNotifier))
}

func TestSlackNotifier_Configure(t *testing.T) {
	module := fixtureSlackNotifier()

	module.Configure("test", "notifier.test")
	assert.NotNil(t, module.HttpClient, "Expected HttpClient to be set with a client object")
}

func TestSlackNotifier_Configure_NoURL(t *testing.T) {
	module := fixtureSlackNotifier()
	viper.Set("notifier.test.url", "")

	assert.Panics(t, func() { module.Configure("test", "notifier.test") }, "The code did not panic")
}

func TestSlackNotifier_StartStop(t *testing.T) {
	module := fixtureSlackNotifier()
	module.Configure("test", "notifier.test")

	err := module.Start()
	assert.Nil(t, err, "Expected Start to return no error")
	err = module.Stop()
	assert.Nil(t, err, "Expected Stop to return no error")
}

func TestSlackNotifier_AcceptConsumerGroup(t *testing.T) {
	module := fixtureSlackNotifier()
	module.Configure("test", "notifier.test")

	// Should always return true
	assert.True(t, module.AcceptConsumerGroup(&protocol.ConsumerGroupStatus{}), "Expected any status to return True")
}

func TestSlackNotifier_Notify_Open(t *testing.T) {
	// handler that validates that we get the right values
	requestHandler := func(w http.ResponseWriter, r *http.Request) {
		// Must get an appropriate Content-Type header
		headers, ok := r.Header["Content-Type"]
		assert.True(t, ok, "Expected to receive Content-Type header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Content-Type header")
		assert.Equalf(t, "application/json", headers[0], "Expected Content-Type header to be 'application/json', not '%v'", headers[0])

		body, _ := ioutil.ReadAll(r.Body)
		assert.Equalf(t, "testidstring testcluster testgroup WARN", string(body), "Expected Text to be 'testidstring testcluster testgroup WARN', not '%v'", string(body))

		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureSlackNotifier()
	viper.Set("notifier.test.url", ts.URL)

	// Template sends the ID, cluster, and group
	module.templateOpen, _ = template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	module.Configure("test", "notifier.test")

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
		// Must get an appropriate Content-Type header
		headers, ok := r.Header["Content-Type"]
		assert.True(t, ok, "Expected to receive Content-Type header")
		assert.Len(t, headers, 1, "Expected to receive exactly one Content-Type header")
		assert.Equalf(t, "application/json", headers[0], "Expected Content-Type header to be 'application/json', not '%v'", headers[0])

		body, _ := ioutil.ReadAll(r.Body)

		assert.Equalf(t, "testidstring testcluster testgroup WARN", string(body), "Expected Text to be 'testidstring testcluster testgroup WARN', not '%v'", string(body))

		fmt.Fprint(w, "ok")
	}

	// create test server with handler
	ts := httptest.NewServer(http.HandlerFunc(requestHandler))
	defer ts.Close()

	module := fixtureSlackNotifier()
	viper.Set("notifier.test.url", ts.URL)
	viper.Set("notifier.test.send-close", true)

	// Template sends the ID, cluster, and group
	module.templateClose, _ = template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), true)
}
