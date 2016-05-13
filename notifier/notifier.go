/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"encoding/json"
)

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

type ConsumerOffset struct {
	Offset     int64
	Timestamp  int64
	Lag        int64
	artificial bool
}

type PartitionStatus struct {
	Topic     string
	Partition int32
	Status    StatusConstant
	Start     ConsumerOffset
	End       ConsumerOffset
}

type Message struct {
	Cluster         string
	Group           string
	Status          StatusConstant
	Complete        bool
	Partitions      []*PartitionStatus
	TotalPartitions int
	Maxlag          *PartitionStatus
	TotalLag        uint64
}

type Notifier interface {
	Notify(msg Message) error
	NotifierName() string
	Ignore(msg Message) bool
}
