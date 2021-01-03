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
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/protocol"
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
			AppReady:         false,
		},
	}

	viper.Reset()
	coordinator.Configure()
	return &coordinator
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
