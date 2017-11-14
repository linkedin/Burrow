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

type EvaluatorRequest struct {
	Reply   chan *ConsumerGroupStatus
	Cluster string
	Group   string
	ShowAll bool
}

type PartitionStatus struct {
	Topic      string          `json:"topic"`
	Partition  int32           `json:"partition"`
	Owner      string          `json:"owner"`
	Status     StatusConstant  `json:"status"`
	Start      *ConsumerOffset `json:"start"`
	End        *ConsumerOffset `json:"end"`
	CurrentLag uint64          `json:"current_lag"`
	Complete   float32         `json:"complete"`
}

type ConsumerGroupStatus struct {
	Cluster         string             `json:"cluster"`
	Group           string             `json:"group"`
	Status          StatusConstant     `json:"status"`
	Complete        float32            `json:"complete"`
	Partitions      []*PartitionStatus `json:"partitions"`
	TotalPartitions int                `json:"partition_count"`
	Maxlag          *PartitionStatus   `json:"maxlag"`
	TotalLag        uint64             `json:"totallag"`
}

var StatusStrings = [...]string{"NOTFOUND", "OK", "WARN", "ERR", "STOP", "STALL", "REWIND"}

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
