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

type StorageRequestConstant int

const (
	StorageSetBrokerOffset     StorageRequestConstant = 0
	StorageSetConsumerOffset   StorageRequestConstant = 1
	StorageSetConsumerOwner    StorageRequestConstant = 2
	StorageSetDeleteTopic      StorageRequestConstant = 3
	StorageSetDeleteGroup      StorageRequestConstant = 4
	StorageFetchClusters       StorageRequestConstant = 5
	StorageFetchConsumers      StorageRequestConstant = 6
	StorageFetchTopics         StorageRequestConstant = 7
	StorageFetchConsumer       StorageRequestConstant = 8
	StorageFetchTopic          StorageRequestConstant = 9
	StorageClearConsumerOwners StorageRequestConstant = 10
)

// Strings are used for logging
var StorageRequestStrings = [...]string{
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
}

func (c StorageRequestConstant) String() string {
	if (c >= 0) && (c < StorageRequestConstant(len(StorageRequestStrings))) {
		return StorageRequestStrings[c]
	} else {
		return "UNKNOWN"
	}
}
func (c StorageRequestConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}
func (c StorageRequestConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

type StorageRequest struct {
	RequestType         StorageRequestConstant
	Reply               chan interface{}
	Cluster             string
	Group               string
	Topic               string
	Partition           int32
	TopicPartitionCount int32
	Offset              int64
	Timestamp           int64
	Owner               string
}

type ConsumerPartition struct {
	Offsets    []*ConsumerOffset `json:"offsets"`
	Owner      string            `json:"owner"`
	CurrentLag int64             `json:"current-lag"`
}

type ConsumerOffset struct {
	Offset    int64 `json:"offset"`
	Timestamp int64 `json:"timestamp"`
	Lag       int64 `json:"lag"`
}

type ConsumerTopics map[string]ConsumerPartitions
type ConsumerPartitions []*ConsumerPartition
