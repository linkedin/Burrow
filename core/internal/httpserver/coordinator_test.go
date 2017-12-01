/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package httpserver

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureConfiguredCoordinator() *Coordinator {
	logLevel := zap.NewAtomicLevelAt(zap.InfoLevel)

	coordinator := Coordinator{
		Log: zap.NewNop(),
		App: &protocol.ApplicationContext{
			Logger:           zap.NewNop(),
			LogLevel:         &logLevel,
			StorageChannel:   make(chan *protocol.StorageRequest),
			EvaluatorChannel: make(chan *protocol.EvaluatorRequest),
		},
	}

	viper.Reset()
	coordinator.Configure()
	return &coordinator
}

func TestHttpServer_handleAdmin(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Set up a request
	req, err := http.NewRequest("GET", "/burrow/admin", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)
	assert.Equalf(t, "GOOD", rr.Body.String(), "Expected response body to be 'GOOD', not '%v'", rr.Body.String())
}

func TestHttpServer_getClusterList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchClusters, request.RequestType, "Expected request of type StorageFetchClusters, not %v", request.RequestType)
		request.Reply <- []string{"testcluster"}
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/admin/loglevel", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseLogLevel
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "info", resp.Level, "Expected Level to be info, not %v", resp.Level)
}

func TestHttpServer_setLogLevel(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Set up a request
	req, err := http.NewRequest("POST", "/v3/admin/loglevel", strings.NewReader("{\"level\": \"debug\"}"))
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseError
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")

	assert.False(t, resp.Error, "Expected response Error to be false")

	// The log level is changed async to the HTTP call, so sleep to make sure it got processed
	time.Sleep(100 * time.Millisecond)
	assert.Equalf(t, zap.DebugLevel, coordinator.App.LogLevel.Level(), "Expected log level to be set to Debug, not %v", coordinator.App.LogLevel.Level().String())
}

func TestHttpServer_DefaultHandler(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/no/such/uri", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseError
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")

	assert.True(t, resp.Error, "Expected response Error to be true")
}
