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

// EvaluatorRequest is sent over the EvaluatorChannel that is stored in the application context. It is a query for the
// status of a group in a cluster. The response to this query is sent over the reply channel. This request is typically
// used in the HTTP server and notifier subsystems.
type EvaluatorRequest struct {
	// Reply is the channel over which the evaluator will send the status response. The sender should expect to receive
	// only one message over this channel for each request, and the channel will not be closed after the response is
	// sent (to facilitate the notifier, which uses a single channel for all responses)
	Reply chan *ConsumerGroupStatus

	// The name of the cluster in which the group is found
	Cluster string

	// The name of the group to get the status for
	Group string

	// If ShowAll is true, the returned status object contains a partition entry for every partition the group consumes,
	// regardless of the state of that partition. If false (the default), only partitions that have a status of WARN
	// or above are returned in the status object.
	ShowAll bool
}

// PartitionStatus represents the state of a single consumed partition
type PartitionStatus struct {
	// The topic name for this partition
	Topic string `json:"topic"`

	// The partition ID
	Partition int32 `json:"partition"`

	// If available (for active new consumers), the consumer host that currently owns this partiton
	Owner string `json:"owner"`

	// If available (for active new consumers), the client_id of the consumer that currently owns this partition
	ClientID string `json:"client_id"`

	// The status of the partition
	Status StatusConstant `json:"status"`

	// A ConsumerOffset object that describes the first (oldest) offset that Burrow is storing for this partition
	Start *ConsumerOffset `json:"start"`

	// A ConsumerOffset object that describes the last (latest) offset that Burrow is storing for this partition
	End *ConsumerOffset `json:"end"`

	// The current number of messages that the consumer is behind for this partition. This is calculated using the
	// last committed offset and the current broker end offset
	CurrentLag uint64 `json:"current_lag"`

	// A number between 0.0 and 1.0 that describes the percentage complete the offset information is for this partition.
	// For example, if Burrow has been configured to store 10 offsets, and Burrow has only stored 7 commits for this
	// partition, Complete will be 0.7
	Complete float32 `json:"complete"`
}

// ConsumerGroupStatus is the response object that is sent in reply to an EvaluatorRequest. It describes the current
// status of a single consumer group.
type ConsumerGroupStatus struct {
	// The name of the cluster in which the group exists
	Cluster string `json:"cluster"`

	// The name of the consumer group
	Group string `json:"group"`

	// The status of the consumer group. This is either NOTFOUND, OK, WARN, or ERR. It is calculated from the highest
	// Status for the individual partitions
	Status StatusConstant `json:"status"`

	// A number between 0.0 and 1.0 that describes the percentage complete the partition information is for this group.
	// A partition that has a Complete value of less than 1.0 will be treated as zero.
	Complete float32 `json:"complete"`

	// A slice of PartitionStatus objects showing individual partition status. If the request ShowAll field was true,
	// this slice will contain every partition consumed by the group. If ShowAll was false, this slice will only
	// contain the partitions that have a status of WARN or above.
	Partitions []*PartitionStatus `json:"partitions"`

	// A count of the total number of partitions that the group has committed offsets for. Note, this may not be the
	// same as the total number of partitions consumed by the group, if Burrow has not seen commits for all partitions
	// yet.
	TotalPartitions int `json:"partition_count"`

	// A PartitionStatus object for the partition with the highest CurrentLag value
	Maxlag *PartitionStatus `json:"maxlag"`

	// The sum of all partition CurrentLag values for the group
	TotalLag uint64 `json:"totallag"`
}

// StatusConstant describes the state of a partition or group as a single value. These values are ordered from least
// to most "bad", with zero being reserved to indicate that a group is not found.
type StatusConstant int

const (
	// StatusNotFound indicates that the consumer group does not exist. It is not used for partition status.
	StatusNotFound StatusConstant = 0

	// StatusOK indicates that a partition is in a good state. For a group, it indicates that all partitions are in a
	// good state.
	StatusOK StatusConstant = 1

	// StatusWarning indicates that a partition is lagging - it is making progress, but falling further behind. For a
	// group, it indicates that one or more partitions are lagging.
	StatusWarning StatusConstant = 2

	// StatusError indicates that a group has one or more partitions that are in the Stop, Stall, or Rewind states. It
	// is not used for partition status.
	StatusError StatusConstant = 3

	// StatusStop indicates that the consumer has not committed an offset for that partition in some time, and the lag
	// is non-zero. It is not used for group status.
	StatusStop StatusConstant = 4

	// StatusStall indicates that the consumer is committing offsets for the partition, but they are not increasing and
	// the lag is non-zero. It is not used for group status.
	StatusStall StatusConstant = 5

	// StatusRewind indicates that the consumer has committed an offset for the partition that is less than the
	// previous offset. It is not used for group status.
	StatusRewind StatusConstant = 6
)

var statusStrings = [...]string{"NOTFOUND", "OK", "WARN", "ERR", "STOP", "STALL", "REWIND"}

// String returns a string representation of a StatusConstant
func (c StatusConstant) String() string {
	if (c >= 0) && (c < StatusConstant(len(statusStrings))) {
		return statusStrings[c]
	}
	return "UNKNOWN"
}

// MarshalText implements the encoding.TextMarshaler interface. The status is the string representation of
// StatusConstant
func (c StatusConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// MarshalJSON implements the json.Marshaler interface. The status is the string representation of StatusConstant
func (c StatusConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}
