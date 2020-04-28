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
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"log"
	"text/template"
	"time"

	"bytes"

	"github.com/linkedin/Burrow/core/protocol"
)

// executeTemplate provides a common interface for notifier modules to call to process a text/template in the context
// of a protocol.ConsumerGroupStatus and create a message to use in a notification.
func executeTemplate(tmpl *template.Template, extras map[string]string, status *protocol.ConsumerGroupStatus, eventID string, startTime time.Time) (*bytes.Buffer, error) {
	bytesToSend := new(bytes.Buffer)
	err := tmpl.Execute(bytesToSend, struct {
		Cluster string
		Group   string
		ID      string
		Start   time.Time
		Extras  map[string]string
		Result  protocol.ConsumerGroupStatus
	}{
		Cluster: status.Cluster,
		Group:   status.Group,
		ID:      eventID,
		Start:   startTime,
		Extras:  extras,
		Result:  *status,
	})
	if err != nil {
		return nil, err
	}
	return bytesToSend, nil
}

// Helper functions for templates
var helperFunctionMap = template.FuncMap{
	"jsonencoder":     templateJSONEncoder,
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
func templateJSONEncoder(encodeMe interface{}) string {
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

// Appends supplied certificates to trusted certificate chain
func buildRootCAs(extraCaFile string, noVerify bool) *x509.CertPool {
	rootCAs := x509.NewCertPool()

	if extraCaFile != "" && !noVerify {
		certs, err := ioutil.ReadFile(extraCaFile)

		if err != nil {
			log.Panicf("Failed to append %q to RootCAs: %v", extraCaFile, err)
		}

		if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
			log.Println("No certs appended, using system certs only")
		}
	}

	return rootCAs
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
	}
	return a.CurrentLag
}

func formatTimestamp(timestamp int64, formatString string) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format(formatString)
}
