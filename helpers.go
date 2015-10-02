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
