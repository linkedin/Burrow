/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package protocol

import "encoding/json"

// StorageRequestConstant is used in StorageRequest to indicate the type of request. Numeric ordering is not important
type StorageRequestConstant int

const (
	// StorageSetBrokerOffset is the request type to store a broker offset. Requires Cluster, Topic, Partition,
	// TopicPartitionCount, and Offset fields
	StorageSetBrokerOffset StorageRequestConstant = 0

	// StorageSetConsumerOffset is the request type to store a consumer offset. Requires Cluster, Group, Topic,
	// Partition, Offset, and Timestamp fields
	StorageSetConsumerOffset StorageRequestConstant = 1

	// StorageSetConsumerOwner is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	StorageSetConsumerOwner StorageRequestConstant = 2

	// StorageSetDeleteTopic is the request type to remove a topic from the broker and all consumers. Requires Cluster,
	// Group, and Topic fields
	StorageSetDeleteTopic StorageRequestConstant = 3

	// StorageSetDeleteGroup is the request type to remove a consumer group. Requires Cluster and Group fields
	StorageSetDeleteGroup StorageRequestConstant = 4

	// StorageFetchClusters is the request type to retrieve a list of clusters. Requires Reply. Returns a []string
	StorageFetchClusters StorageRequestConstant = 5

	// StorageFetchConsumers is the request type to retrieve a list of consumer groups in a cluster. Requires Reply and
	// Cluster fields. Returns a []string
	StorageFetchConsumers StorageRequestConstant = 6

	// StorageFetchTopics is the request type to retrieve a list of topics in a cluster. Requires Reply and Cluster
	// fields. Returns a []string
	StorageFetchTopics StorageRequestConstant = 7

	// StorageFetchConsumer is the request type to retrieve all stored information for a single consumer group. Requires
	// Reply, Cluster, and Group fields. Returns a ConsumerTopics object
	StorageFetchConsumer StorageRequestConstant = 8

	// StorageFetchTopic is the request type to retrieve the current broker offsets (one per partition) for a topic.
	// Requires Reply, Cluster, and Topic fields.
	// Returns a []int64
	StorageFetchTopic StorageRequestConstant = 9

	// StorageClearConsumerOwners is the request type to remove all partition owner information for a single group.
	// Requires Cluster and Group fields
	StorageClearConsumerOwners StorageRequestConstant = 10

	// StorageFetchConsumersForTopic is the request type to obtain a list of all consumer groups consuming from a topic.
	// Returns a []string
	StorageFetchConsumersForTopic StorageRequestConstant = 11
)

var storageRequestStrings = [...]string{
	"StorageSetBrokerOffset",
	"StorageSetConsumerOffset",
	"StorageSetConsumerOwner",
	"StorageSetDeleteTopic",
	"StorageSetDeleteGroup",
	"StorageFetchClusters",
	"StorageFetchConsumers",
	"StorageFetchTopics",
	"StorageFetchConsumer",
	"StorageFetchTopic",
	"StorageClearConsumerOwners",
	"StorageFetchConsumersForTopic",
}

// String returns a string representation of a StorageRequestConstant for logging
func (c StorageRequestConstant) String() string {
	if (c >= 0) && (c < StorageRequestConstant(len(storageRequestStrings))) {
		return storageRequestStrings[c]
	}
	return "UNKNOWN"
}

// MarshalText implements the encoding.TextMarshaler interface. The status is the string representation of
// StorageRequestConstant
func (c StorageRequestConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// MarshalJSON implements the json.Marshaler interface. The status is the string representation of
// StorageRequestConstant
func (c StorageRequestConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// StorageRequest is sent over the StorageChannel that is stored in the application context. It is a query to either
// send information to the storage subsystem, or retrieve information from it . The RequestType indiciates the
// particular type of request. "Set" and "Clear" requests do not get a response. "Fetch" requests will send a response
// over the Reply channel supplied in the request
type StorageRequest struct {
	// The type of request that this struct encapsulates
	RequestType StorageRequestConstant

	// If the RequestType is a "Fetch" request, Reply must contain a channel to receive the response on
	Reply chan interface{}

	// The name of the cluster to which the request applies. Required for all request types except StorageFetchClusters
	Cluster string

	// The name of the consumer group to which the request applies
	Group string

	// The name of the topic to which the request applies
	Topic string

	// The ID of the partition to which the request applies
	Partition int32

	// For StorageSetBrokerOffset requests, TopicPartitionCount indiciates the total number of partitions for the topic
	TopicPartitionCount int32

	// For StorageSetBrokerOffset and StorageSetConsumerOffset requests, the offset to store
	Offset int64

	// For StorageSetConsumerOffset requests, the offset of the offset commit itself (i.e. the __consumer_offsets offset)
	Order int64

	// For StorageSetConsumerOffset requests, the timestamp of the offset being stored
	Timestamp int64

	// For StorageSetConsumerOwner requests, a string describing the consumer host that owns the partition
	Owner string

	// For StorageSetConsumerOwner requests, a string containing the client_id set by the consumer
	ClientID string
}

// ConsumerPartition represents the information stored for a group for a single partition. It is used as part of the
// response to a StorageFetchConsumer request
type ConsumerPartition struct {
	// A slice containing a ConsumerOffset object for each offset Burrow has stored for this partition. This can be any
	// length up to the number of intervals Burrow has been configured to store, depending on how many offset commits
	// have been seen for this partition
	Offsets []*ConsumerOffset `json:"offsets"`

	// A slice containing the history of broker offsets stored for this partition. This is used for evaluation only,
	// and as such it is not provided when encoding to JSON (for HTTP responses)
	BrokerOffsets []int64 `json:"-"`

	// A string that describes the consumer host that currently owns this partition, if the information is available
	// (for active new consumers)
	Owner string `json:"owner"`

	// A string containing the client_id set by the consumer (for active new consumers)
	ClientID string `json:"client_id"`

	// The current number of messages that the consumer is behind for this partition. This is calculated using the
	// last committed offset and the current broker end offset
	CurrentLag uint64 `json:"current-lag"`
}

// Lag is just a wrapper for a uint64, but it can be `nil`
type Lag struct {
	Value uint64
}

// MarshalJSON should just treat lag as a nullable number, not a nested struct
func (lag Lag) MarshalJSON() ([]byte, error) {
	return json.Marshal(lag.Value)
}

// UnmarshalJSON reads lag from a JSON number
func (lag *Lag) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &lag.Value)
}

// ConsumerOffset represents a single offset stored. It is used as part of the response to a StorageFetchConsumer
// request
type ConsumerOffset struct {
	// The offset that is stored
	Offset int64 `json:"offset"`

	// The offst of this __consumer_offsets commit
	Order int64 `json:"-"`

	// The timestamp at which the offset was committed
	Timestamp int64 `json:"timestamp"`

	// The number of messages that the consumer was behind at the time that the offset was committed. This number is
	// not updated after the offset was committed, so it does not represent the current lag of the consumer.
	Lag *Lag `json:"lag"`
}

// ConsumerTopics is the response that is sent for a StorageFetchConsumer request. It is a map of topic names to
// ConsumerPartitions objects that describe that topic
type ConsumerTopics map[string]ConsumerPartitions

// ConsumerPartitions describes all partitions for a single topic. The index indicates the partition ID, and the value
// is a pointer to a ConsumerPartition object with the offset information for that partition.
type ConsumerPartitions []*ConsumerPartition
