/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
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
	"text/template"
	"time"

	"github.com/linkedin/Burrow/core/protocol"
)

// Helper functions for templates
var helperFunctionMap = template.FuncMap{
	"jsonencoder":     templateJsonEncoder,
	"topicsbystatus":  classifyTopicsByStatus,
	"partitioncounts": templateCountPartitions,
	"add":             templateAdd,
	"minus":           templateMinus,
	"multiply":        templateMultiply,
	"divide":          templateDivide,
	"maxlag":          maxLagHelper,
	"formattimestamp": formatTimestamp,
}

// Helper function for the templates to encode an object into a JSON string
func templateJsonEncoder(encodeMe interface{}) string {
	jsonStr, _ := json.Marshal(encodeMe)
	return string(jsonStr)
}

// Helper - recategorize partitions as a map of lists
// map[string][]string => status short name -> list of topics
func classifyTopicsByStatus(partitions []*protocol.PartitionStatus) map[string][]string {
	tmpMap := make(map[string]map[string]bool)
	for _, partition := range partitions {
		if _, ok := tmpMap[partition.Status.String()]; !ok {
			tmpMap[partition.Status.String()] = make(map[string]bool)
		}
		tmpMap[partition.Status.String()][partition.Topic] = true
	}

	rv := make(map[string][]string)
	for status, topicMap := range tmpMap {
		rv[status] = make([]string, 0, len(topicMap))
		for topic := range topicMap {
			rv[status] = append(rv[status], topic)
		}
	}

	return rv
}

// Template Helper - Return a map of partition counts
// keys are warn, stop, stall, rewind, unknown
func templateCountPartitions(partitions []*protocol.PartitionStatus) map[string]int {
	rv := map[string]int{
		"warn":    0,
		"stop":    0,
		"stall":   0,
		"rewind":  0,
		"unknown": 0,
	}

	for _, partition := range partitions {
		switch partition.Status {
		case protocol.StatusOK:
			break
		case protocol.StatusWarning:
			rv["warn"]++
		case protocol.StatusStop:
			rv["stop"]++
		case protocol.StatusStall:
			rv["stall"]++
		case protocol.StatusRewind:
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

func maxLagHelper(a *protocol.PartitionStatus) uint64 {
	if a == nil {
		return 0
	} else {
		return a.CurrentLag
	}
}

func formatTimestamp(timestamp int64, formatString string) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format(formatString)
}
