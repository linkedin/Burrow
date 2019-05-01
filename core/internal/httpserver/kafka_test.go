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
	"time"

	"github.com/spf13/viper"

	"github.com/stretchr/testify/assert"
	"net/http/httptest"
	"testing"

	"github.com/linkedin/Burrow/core/protocol"
)

func TestHttpServer_handleClusterList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchClusters, request.RequestType, "Expected request of type StorageFetchClusters, not %v", request.RequestType)
		request.Reply <- []string{"testcluster"}
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseClusterList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, []string{"testcluster"}, resp.Clusters, "Expected Clusters list to contain just testcluster, not %v", resp.Clusters)
}

func TestHttpServer_handleClusterDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()
	viper.Set("client-profile.test.client-id", "testid")
	viper.Set("cluster.testcluster.class-name", "kafka")
	viper.Set("cluster.testcluster.client-profile", "test")

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka/testcluster", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Need a specialized version of this for decoding
	type ResponseType struct {
		Error   bool                            `json:"error"`
		Message string                          `json:"message"`
		Module  httpResponseConfigModuleCluster `json:"module"`
		Request httpResponseRequestInfo         `json:"request"`
	}

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp ResponseType
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, "kafka", resp.Module.ClassName, "Expected response to contain a module with type kafka, not %v", resp.Module.ClassName)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_handleTopicList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopics, request.RequestType, "Expected request of type StorageFetchTopics, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		request.Reply <- []string{"testtopic"}
		close(request.Reply)

		// Second request is a 404
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopics, request.RequestType, "Expected request of type StorageFetchTopics, not %v", request.RequestType)
		assert.Equalf(t, "nocluster", request.Cluster, "Expected request Cluster to be nocluster, not %v", request.Cluster)
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka/testcluster/topic", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseTopicList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, []string{"testtopic"}, resp.Topics, "Expected Topics list to contain just testtopic, not %v", resp.Topics)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster/topic", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_handleConsumerList(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchConsumers, request.RequestType, "Expected request of type StorageFetchConsumers, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		request.Reply <- []string{"testgroup"}
		close(request.Reply)

		// Second request is a 404
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchConsumers, request.RequestType, "Expected request of type StorageFetchConsumers, not %v", request.RequestType)
		assert.Equalf(t, "nocluster", request.Cluster, "Expected request Cluster to be nocluster, not %v", request.Cluster)
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka/testcluster/consumer", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConsumerList
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, []string{"testgroup"}, resp.Consumers, "Expected Consumers list to contain just testgroup, not %v", resp.Consumers)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster/consumer", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_handleTopicDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopic, request.RequestType, "Expected request of type StorageFetchTopic, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testtopic", request.Topic, "Expected request Topic to be testtopic, not %v", request.Topic)
		request.Reply <- []int64{345, 921}
		close(request.Reply)

		// Second request is a 404
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopic, request.RequestType, "Expected request of type StorageFetchTopic, not %v", request.RequestType)
		assert.Equalf(t, "nocluster", request.Cluster, "Expected request Cluster to be nocluster, not %v", request.Cluster)
		assert.Equalf(t, "testtopic", request.Topic, "Expected request Topic to be testtopic, not %v", request.Topic)
		close(request.Reply)

		// Third request is a 404
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopic, request.RequestType, "Expected request of type StorageFetchTopic, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "notopic", request.Topic, "Expected request Topic to be notopic, not %v", request.Topic)
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka/testcluster/topic/testtopic", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseTopicDetail
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Equalf(t, []int64{345, 921}, resp.Offsets, "Expected Offsets list to contain [345, 921], not %v", resp.Offsets)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster/topic/testtopic", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)

	// Call a third time for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/testcluster/topic/notopic", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_handleConsumerDetail(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchConsumer, request.RequestType, "Expected request of type StorageFetchConsumer, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		response := make(protocol.ConsumerTopics)
		response["testtopic"] = make([]*protocol.ConsumerPartition, 1)
		response["testtopic"][0] = &protocol.ConsumerPartition{
			Offsets:    make([]*protocol.ConsumerOffset, 1),
			Owner:      "somehost",
			CurrentLag: 2345,
		}
		response["testtopic"][0].Offsets[0] = &protocol.ConsumerOffset{
			Offset:    9837458,
			Timestamp: 12837487,
			Lag:       &protocol.Lag{2355},
		}
		request.Reply <- response
		close(request.Reply)

		// Second request is a 404
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchConsumer, request.RequestType, "Expected request of type StorageFetchConsumer, not %v", request.RequestType)
		assert.Equalf(t, "nocluster", request.Cluster, "Expected request Cluster to be nocluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		close(request.Reply)

		// Third request is a 404
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchConsumer, request.RequestType, "Expected request of type StorageFetchConsumer, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "nogroup", request.Group, "Expected request Group to be nogroup, not %v", request.Group)
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka/testcluster/consumer/testgroup", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseConsumerDetail
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.Lenf(t, resp.Topics, 1, "Expected response to contain exactly one topic, not %v", len(resp.Topics))
	topic, ok := resp.Topics["testtopic"]
	assert.True(t, ok, "Expected topic name to be testtopic")
	assert.Lenf(t, topic, 1, "Expected topic to contain exactly one partition, not %v", len(topic))
	assert.Equalf(t, "somehost", topic[0].Owner, "Expected partition Owner to be somehost, not %v", topic[0].Owner)
	assert.Equalf(t, uint64(2345), topic[0].CurrentLag, "Expected partition CurrentLag to be 2345, not %v", topic[0].CurrentLag)
	assert.Lenf(t, topic[0].Offsets, 1, "Expected partition to have exactly one offset, not %v", topic[0].Offsets)
	assert.Equalf(t, int64(9837458), topic[0].Offsets[0].Offset, "Expected Offset to be 9837458, not %v", topic[0].Offsets[0].Offset)
	assert.Equalf(t, int64(12837487), topic[0].Offsets[0].Timestamp, "Expected Timestamp to be 12837487, not %v", topic[0].Offsets[0].Timestamp)
	assert.Equalf(t, &protocol.Lag{uint64(2355)}, topic[0].Offsets[0].Lag, "Expected Lag to be 2355, not %v", topic[0].Offsets[0].Lag)

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster/consumer/testgroup", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)

	// Call a third time for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/testcluster/consumer/nogroup", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

// Custom response types for consumer status, as the status field will be a string
type ResponsePartition struct {
	Topic      string                   `json:"topic"`
	Partition  int32                    `json:"partition"`
	Status     string                   `json:"status"`
	Start      *protocol.ConsumerOffset `json:"start"`
	End        *protocol.ConsumerOffset `json:"end"`
	CurrentLag int64                    `json:"current_lag"`
	Complete   float32                  `json:"complete"`
}
type ResponseStatus struct {
	Cluster         string               `json:"cluster"`
	Group           string               `json:"group"`
	Status          string               `json:"status"`
	Complete        float32              `json:"complete"`
	Partitions      []*ResponsePartition `json:"partitions"`
	TotalPartitions int                  `json:"partition_count"`
	Maxlag          *ResponsePartition   `json:"maxlag"`
	TotalLag        uint64               `json:"totallag"`
}
type ResponseType struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Status  ResponseStatus          `json:"status"`
	Request httpResponseRequestInfo `json:"request"`
}

func TestHttpServer_handleConsumerStatus(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected evaluator request
	go func() {
		request := <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		assert.False(t, request.ShowAll, "Expected request ShowAll to be False")
		response := &protocol.ConsumerGroupStatus{
			Cluster:         request.Cluster,
			Group:           request.Group,
			Status:          protocol.StatusOK,
			Complete:        1.0,
			Partitions:      make([]*protocol.PartitionStatus, 0),
			TotalPartitions: 2134,
			Maxlag: &protocol.PartitionStatus{
				Topic:     "testtopic",
				Partition: 0,
				Status:    protocol.StatusOK,
				Start: &protocol.ConsumerOffset{
					Offset:    9836458,
					Timestamp: 12836487,
					Lag:       &protocol.Lag{3254},
				},
				End: &protocol.ConsumerOffset{
					Offset:    9837458,
					Timestamp: 12837487,
					Lag:       &protocol.Lag{2355},
				},
			},
			TotalLag: 2345,
		}
		request.Reply <- response
		close(request.Reply)

		// Second request is a 404
		request = <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "nocluster", request.Cluster, "Expected request Cluster to be nocluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		assert.False(t, request.ShowAll, "Expected request ShowAll to be False")
		request.Reply <- &protocol.ConsumerGroupStatus{
			Cluster:    request.Cluster,
			Group:      request.Group,
			Status:     protocol.StatusNotFound,
			Complete:   1.0,
			Partitions: make([]*protocol.PartitionStatus, 0),
			Maxlag:     nil,
			TotalLag:   0,
		}
		close(request.Reply)

		// Third request is a 404
		request = <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "nogroup", request.Group, "Expected request Group to be nogroup, not %v", request.Group)
		assert.False(t, request.ShowAll, "Expected request ShowAll to be False")
		request.Reply <- &protocol.ConsumerGroupStatus{
			Cluster:    request.Cluster,
			Group:      request.Group,
			Status:     protocol.StatusNotFound,
			Complete:   1.0,
			Partitions: make([]*protocol.PartitionStatus, 0),
			Maxlag:     nil,
			TotalLag:   0,
		}
		close(request.Reply)

		// Now we switch to expecting /lag requests
		request = <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		assert.True(t, request.ShowAll, "Expected request ShowAll to be True")
		response.Partitions = make([]*protocol.PartitionStatus, 1)
		response.Partitions[0] = response.Maxlag
		request.Reply <- response
		close(request.Reply)

		// Fifth request is a 404
		request = <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "nocluster", request.Cluster, "Expected request Cluster to be nocluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		assert.True(t, request.ShowAll, "Expected request ShowAll to be True")
		request.Reply <- &protocol.ConsumerGroupStatus{
			Cluster:    request.Cluster,
			Group:      request.Group,
			Status:     protocol.StatusNotFound,
			Complete:   1.0,
			Partitions: make([]*protocol.PartitionStatus, 0),
			Maxlag:     nil,
			TotalLag:   0,
		}
		close(request.Reply)

		// Sixth request is a 404
		request = <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "nogroup", request.Group, "Expected request Group to be nogroup, not %v", request.Group)
		assert.True(t, request.ShowAll, "Expected request ShowAll to be True")
		request.Reply <- &protocol.ConsumerGroupStatus{
			Cluster:    request.Cluster,
			Group:      request.Group,
			Status:     protocol.StatusNotFound,
			Complete:   1.0,
			Partitions: make([]*protocol.PartitionStatus, 0),
			Maxlag:     nil,
			TotalLag:   0,
		}
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/v3/kafka/testcluster/consumer/testgroup/status", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp ResponseType
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.NotNil(t, resp.Status, "Expected Status to not be nil")
	assert.Equalf(t, float32(1.0), resp.Status.Complete, "Expected Complete to be 1.0, not %v", resp.Status.Complete)
	assert.Lenf(t, resp.Status.Partitions, 0, "Expected Status to contain exactly zero partitions, not %v", len(resp.Status.Partitions))
	assert.Equalf(t, 2134, resp.Status.TotalPartitions, "Expected TotalPartitions to be 2134, not %v", resp.Status.TotalPartitions)
	assert.NotNil(t, resp.Status.Maxlag, "Expected Maxlag to not be nil")

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster/consumer/testgroup/status", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)

	// Call a third time for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/testcluster/consumer/nogroup/status", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)

	// Call for complete status (/lag endpoint)
	req, err = http.NewRequest("GET", "/v3/kafka/testcluster/consumer/testgroup/lag", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Parse response body
	decoder = json.NewDecoder(rr.Body)
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
	assert.NotNil(t, resp.Status, "Expected Status to not be nil")
	assert.Equalf(t, float32(1.0), resp.Status.Complete, "Expected Complete to be 1.0, not %v", resp.Status.Complete)
	assert.Lenf(t, resp.Status.Partitions, 1, "Expected Status to contain exactly one partition, not %v", len(resp.Status.Partitions))
	assert.Equalf(t, 2134, resp.Status.TotalPartitions, "Expected TotalPartitions to be 2134, not %v", resp.Status.TotalPartitions)
	assert.NotNil(t, resp.Status.Maxlag, "Expected Maxlag to not be nil")

	// Call again for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/nocluster/consumer/testgroup/lag", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)

	// Call a sixth time for a 404
	req, err = http.NewRequest("GET", "/v3/kafka/testcluster/consumer/nogroup/lag", nil)
	assert.NoError(t, err, "Expected request setup to return no error")
	rr = httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)
	assert.Equalf(t, http.StatusNotFound, rr.Code, "Expected response code to be 404, not %v", rr.Code)
}

func TestHttpServer_handleConsumerDelete(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage request
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageSetDeleteGroup, request.RequestType, "Expected request of type StorageFetchConsumer, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		// No response expected
	}()

	// Set up a request
	req, err := http.NewRequest("DELETE", "/v3/kafka/testcluster/consumer/testgroup", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	// Sleep briefly just to catch the goroutine above throwing a failure
	time.Sleep(100 * time.Millisecond)

	// Parse response body
	decoder := json.NewDecoder(rr.Body)
	var resp httpResponseError
	err = decoder.Decode(&resp)
	assert.NoError(t, err, "Expected body decode to return no error")
	assert.False(t, resp.Error, "Expected response Error to be false")
}
