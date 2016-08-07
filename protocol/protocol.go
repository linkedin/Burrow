/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package protocol

import (
	"encoding/json"
)

type PartitionOffset struct {
	Cluster             string
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	Group               string
	TopicPartitionCount int
}

type ConsumerOffset struct {
	Offset     int64 `json:"offset"`
	Timestamp  int64 `json:"timestamp"`
	Lag        int64 `json:"lag"`
	Artificial bool  `json:"-"`
}

type StatusConstant int

const (
	StatusNotFound StatusConstant = 0
	StatusOK       StatusConstant = 1
	StatusWarning  StatusConstant = 2
	StatusError    StatusConstant = 3
	StatusStop     StatusConstant = 4
	StatusStall    StatusConstant = 5
	StatusRewind   StatusConstant = 6
)

var StatusStrings = [...]string{"NOTFOUND", "OK", "WARN", "ERR", "STOP", "STALL", "REWIND"}

func (c StatusConstant) String() string {
	if (c >= 0) && (c < StatusConstant(len(StatusStrings))) {
		return StatusStrings[c]
	} else {
		return "UNKNOWN"
	}
}
func (c StatusConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}
func (c StatusConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

type PartitionStatus struct {
	Topic     string         `json:"topic"`
	Partition int32          `json:"partition"`
	Status    StatusConstant `json:"status"`
	Start     ConsumerOffset `json:"start"`
	End       ConsumerOffset `json:"end"`
}

type ConsumerGroupStatus struct {
	Cluster         string             `json:"cluster"`
	Group           string             `json:"group"`
	Status          StatusConstant     `json:"status"`
	Complete        bool               `json:"complete"`
	Partitions      []*PartitionStatus `json:"partitions"`
	TotalPartitions int                `json:"partition_count"`
	Maxlag          *PartitionStatus   `json:"maxlag"`
	TotalLag        uint64             `json:"totallag"`
}
