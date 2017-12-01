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

	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
)

func setupConfiguration() {
	viper.Reset()
	viper.Set("client-profile.test.client-id", "testid")
	viper.Set("storage.teststorage.class-name", "inmemory")
	viper.Set("consumer.testconsumer.class-name", "kafka_zk")
	viper.Set("consumer.testconsumer.client-profile", "test")
	viper.Set("cluster.testcluster.class-name", "kafka")
	viper.Set("cluster.testcluster.client-profile", "test")
	viper.Set("evaluator.testevaluator.class-name", "caching")
	viper.Set("notifier.testnotifier.class-name", "null")
}

func TestHttpServer_configMain(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body - just test that it decodes
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConfigMain
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
}

func TestHttpServer_configStorageList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/storage", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConfigModuleList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "storage", resp.Coordinator, "Expected Coordinator to be storage, not %v", resp.Coordinator)
	assert.Equalf(t, []string{"teststorage"}, resp.Modules, "Expected Modules to be [teststorage], not %v", resp.Modules)
}

func TestHttpServer_configConsumerList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/consumer", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConfigModuleList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "consumer", resp.Coordinator, "Expected Coordinator to be consumer, not %v", resp.Coordinator)
	assert.Equalf(t, []string{"testconsumer"}, resp.Modules, "Expected Modules to be [testconsumer], not %v", resp.Modules)
}

func TestHttpServer_configClusterList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/cluster", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConfigModuleList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "cluster", resp.Coordinator, "Expected Coordinator to be cluster, not %v", resp.Coordinator)
	assert.Equalf(t, []string{"testcluster"}, resp.Modules, "Expected Modules to be [testcluster], not %v", resp.Modules)
}

func TestHttpServer_configEvaluatorList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/evaluator", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConfigModuleList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "evaluator", resp.Coordinator, "Expected Coordinator to be evaluator, not %v", resp.Coordinator)
	assert.Equalf(t, []string{"testevaluator"}, resp.Modules, "Expected Modules to be [testevaluator], not %v", resp.Modules)
}

func TestHttpServer_configNotifierList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/notifier", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConfigModuleList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "notifier", resp.Coordinator, "Expected Coordinator to be notifier, not %v", resp.Coordinator)
	assert.Equalf(t, []string{"testnotifier"}, resp.Modules, "Expected Modules to be [testnotifier], not %v", resp.Modules)
}

func TestHttpServer_configStorageDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/storage/teststorage", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Need a custom type for the test, due to variations in the response for different module types
	type ResponseType struct {
		Error   bool                            `json:"error"`
		Message string                          `json:"message"`
		Module  httpResponseConfigModuleStorage `json:"module"`
		Request httpResponseRequestInfo         `json:"request"`
	}

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp ResponseType
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "inmemory", resp.Module.ClassName, "Expected ClassName to be immemory, not %v", resp.Module.ClassName)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/config/storage/nomodule", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_configConsumerDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/consumer/testconsumer", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Need a custom type for the test, due to variations in the response for different module types
	type ResponseType struct {
		Error   bool                             `json:"error"`
		Message string                           `json:"message"`
		Module  httpResponseConfigModuleConsumer `json:"module"`
		Request httpResponseRequestInfo          `json:"request"`
	}

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp ResponseType
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "kafka_zk", resp.Module.ClassName, "Expected ClassName to be kafka_zk, not %v", resp.Module.ClassName)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/config/consumer/nomodule", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_configEvaluatorDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/evaluator/testevaluator", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Need a custom type for the test, due to variations in the response for different module types
	type ResponseType struct {
		Error   bool                              `json:"error"`
		Message string                            `json:"message"`
		Module  httpResponseConfigModuleEvaluator `json:"module"`
		Request httpResponseRequestInfo           `json:"request"`
	}

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp ResponseType
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "caching", resp.Module.ClassName, "Expected ClassName to be caching, not %v", resp.Module.ClassName)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/config/evaluator/nomodule", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_configNotifierDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	setupConfiguration()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/config/notifier/testnotifier", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Need a custom type for the test, due to variations in the response for different module types
	type ResponseType struct {
		Error   bool                                 `json:"error"`
		Message string                               `json:"message"`
		Module  httpResponseConfigModuleNotifierNull `json:"module"`
		Request httpResponseRequestInfo              `json:"request"`
	}

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp ResponseType
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "null", resp.Module.ClassName, "Expected ClassName to be null, not %v", resp.Module.ClassName)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/config/notifier/nomodule", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}
