package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/linkedin/Burrow/core/protocol"
)

func TestHttpServer_handlePrometheusMetrics(t *testing.T) {
	coordinator := fixtureConfiguredCoordinator()

	// Respond to the expected storage requests
	go func() {
		request := <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchClusters, request.RequestType, "Expected request of type StorageFetchClusters, not %v", request.RequestType)
		request.Reply <- []string{"testcluster"}
		close(request.Reply)

		// List of consumers
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchConsumers, request.RequestType, "Expected request of type StorageFetchConsumers, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		request.Reply <- []string{"testgroup", "testgroup2"}
		close(request.Reply)

		// List of topics
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopics, request.RequestType, "Expected request of type StorageFetchTopics, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		request.Reply <- []string{"testtopic", "testtopic1"}
		close(request.Reply)

		// Topic details
		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopic, request.RequestType, "Expected request of type StorageFetchTopic, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testtopic", request.Topic, "Expected request Topic to be testtopic, not %v", request.Topic)
		request.Reply <- []int64{6556, 5566}
		close(request.Reply)

		request = <-coordinator.App.StorageChannel
		assert.Equalf(t, protocol.StorageFetchTopic, request.RequestType, "Expected request of type StorageFetchTopic, not %v", request.RequestType)
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testtopic1", request.Topic, "Expected request Topic to be testtopic, not %v", request.Topic)
		request.Reply <- []int64{54}
		close(request.Reply)
	}()

	// Respond to the expected evaluator requests
	go func() {
		// testgroup happy paths
		request := <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		assert.True(t, request.ShowAll, "Expected request ShowAll to be True")
		response := &protocol.ConsumerGroupStatus{
			Cluster:  request.Cluster,
			Group:    request.Group,
			Status:   protocol.StatusOK,
			Complete: 1.0,
			Partitions: []*protocol.PartitionStatus{
				{
					Topic:      "testtopic",
					Partition:  0,
					Status:     protocol.StatusOK,
					CurrentLag: 100,
					Complete:   1.0,
					End: &protocol.ConsumerOffset{
						Offset: 22663,
					},
					Owner: "/1.1.1.1",
				},
				{
					Topic:      "testtopic",
					Partition:  1,
					Status:     protocol.StatusOK,
					CurrentLag: 10,
					Complete:   1.0,
					End: &protocol.ConsumerOffset{
						Offset: 2488,
					},
					Owner: "/1.1.1.1",
				},
				{
					Topic:      "testtopic1",
					Partition:  0,
					Status:     protocol.StatusOK,
					CurrentLag: 50,
					Complete:   1.0,
					End: &protocol.ConsumerOffset{
						Offset: 99888,
					},
					Owner: "/1.1.1.1",
				},
				{
					Topic:      "incomplete",
					Partition:  0,
					Status:     protocol.StatusOK,
					CurrentLag: 0,
					Complete:   0.2,
					End: &protocol.ConsumerOffset{
						Offset: 5335,
					},
					Owner: "/1.1.1.1",
				},
			},
			TotalPartitions: 2134,
			Maxlag:          &protocol.PartitionStatus{},
			TotalLag:        2345,
		}
		request.Reply <- response
		close(request.Reply)

		// testgroup2 not found
		request = <-coordinator.App.EvaluatorChannel
		assert.Equalf(t, "testcluster", request.Cluster, "Expected request Cluster to be testcluster, not %v", request.Cluster)
		assert.Equalf(t, "testgroup2", request.Group, "Expected request Group to be testgroup, not %v", request.Group)
		assert.True(t, request.ShowAll, "Expected request ShowAll to be True")
		response = &protocol.ConsumerGroupStatus{
			Cluster: request.Cluster,
			Group:   request.Group,
			Status:  protocol.StatusNotFound,
		}
		request.Reply <- response
		close(request.Reply)
	}()

	// Set up a request
	req, err := http.NewRequest("GET", "/metrics", nil)
	assert.NoError(t, err, "Expected request setup to return no error")

	// Call the handler via httprouter
	rr := httptest.NewRecorder()
	coordinator.router.ServeHTTP(rr, req)

	assert.Equalf(t, http.StatusOK, rr.Code, "Expected response code to be 200, not %v", rr.Code)

	promExp := rr.Body.String()
	assert.Contains(t, promExp, `burrow_kafka_consumer_status{cluster="testcluster",consumer_group="testgroup"} 1`)
	assert.Contains(t, promExp, `burrow_kafka_consumer_lag_total{cluster="testcluster",consumer_group="testgroup"} 2345`)

	assert.Contains(t, promExp, `burrow_kafka_consumer_partition_lag{cluster="testcluster",consumer_group="testgroup",owner="1.1.1.1",partition="0",topic="testtopic"} 100`)
	assert.Contains(t, promExp, `burrow_kafka_consumer_partition_lag{cluster="testcluster",consumer_group="testgroup",owner="1.1.1.1",partition="1",topic="testtopic"} 10`)
	assert.Contains(t, promExp, `burrow_kafka_consumer_partition_lag{cluster="testcluster",consumer_group="testgroup",owner="1.1.1.1",partition="0",topic="testtopic1"} 50`)

	assert.Contains(t, promExp, `burrow_kafka_consumer_current_offset{cluster="testcluster",consumer_group="testgroup",owner="1.1.1.1",partition="0",topic="testtopic"} 22663`)
	assert.Contains(t, promExp, `burrow_kafka_consumer_current_offset{cluster="testcluster",consumer_group="testgroup",owner="1.1.1.1",partition="1",topic="testtopic"} 2488`)
	assert.Contains(t, promExp, `burrow_kafka_consumer_current_offset{cluster="testcluster",consumer_group="testgroup",owner="1.1.1.1",partition="0",topic="testtopic1"} 99888`)

	assert.Contains(t, promExp, `burrow_kafka_topic_partition_offset{cluster="testcluster",partition="0",topic="testtopic"} 6556`)
	assert.Contains(t, promExp, `burrow_kafka_topic_partition_offset{cluster="testcluster",partition="1",topic="testtopic"} 5566`)
	assert.Contains(t, promExp, `burrow_kafka_topic_partition_offset{cluster="testcluster",partition="0",topic="testtopic1"} 54`)

	assert.NotContains(t, promExp, `burrow_kafka_consumer_partition_lag{cluster="testcluster",consumer_group="testgroup",partition="0",topic="incomplete"} 0`)
	assert.NotContains(t, promExp, "testgroup2")
}
