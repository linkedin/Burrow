/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"encoding/json"
)

// Helper function for the templates to encode an object into a JSON string
func templateJsonEncoder(encodeMe interface{}) string {
	jsonStr, _ := json.Marshal(encodeMe)
	return string(jsonStr)
}

// Helper - recategorize partitions as a map of lists
// map[string][]string => status short name -> list of topics
func classifyTopicsByStatus(partitions []*PartitionStatus) map[string][]string {
	tmp_map := make(map[string]map[string]bool)
	for _, partition := range partitions {
		if _, ok := tmp_map[partition.Status.String()]; !ok {
			tmp_map[partition.Status.String()] = make(map[string]bool)
		}
		tmp_map[partition.Status.String()][partition.Topic] = true
	}

	rv := make(map[string][]string)
	for status, topicMap := range tmp_map {
		rv[status] = make([]string, 0, len(topicMap))
		for topic, _ := range topicMap {
			rv[status] = append(rv[status], topic)
		}
	}

	return rv
}

// Template Helper - Return a map of partition counts
// keys are warn, stop, stall, rewind, unknown
func templateCountPartitions(partitions []*PartitionStatus) map[string]int {
	rv := map[string]int{
		"warn":    0,
		"stop":    0,
		"stall":   0,
		"rewind":  0,
		"unknown": 0,
	}

	for _, partition := range partitions {
		switch partition.Status {
		case StatusOK:
			break
		case StatusWarning:
			rv["warn"]++
		case StatusStop:
			rv["stop"]++
		case StatusStall:
			rv["stall"]++
		case StatusRewind:
			rv["rewind"]++
		default:
			rv["unknown"]++
		}
	}

	return rv
}

// Template Helper - do maths
func templateAdd(a, b int) int {
	return a + b
}
func templateMinus(a, b int) int {
	return a - b
}
func templateMultiply(a, b int) int {
	return a * b
}
func templateDivide(a, b int) int {
	return a / b
}

func maxLagHelper(a *PartitionStatus) int64 {
	if a == nil {
		return 0
	} else {
		return a.End.Lag
	}
}
