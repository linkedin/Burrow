/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package httpserver

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/linkedin/Burrow/core/protocol"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
)

type httpPrometheusMetric struct {
	Name   string
	Value  float64
	Labels *map[string]string

	Help string
	Type string // counter, gauge, histogram, summary, untyped

	Replacer *strings.Replacer
}

func (pm *httpPrometheusMetric) escape(s string) string {
	if pm.Replacer == nil {
		pm.Replacer = strings.NewReplacer("\\", `\\`, "\n", `\n`, "\"", `\"`)
	}
	return pm.Replacer.Replace(s)
}

func (pm *httpPrometheusMetric) String() string {
	metric := pm.escape(pm.Name) + "{"
	if pm.Labels != nil && len(*pm.Labels) > 0 {
		labelNameList := make([]string, len(*pm.Labels))
		labelNumber := 0

		for labelName := range *pm.Labels {
			labelNameList[labelNumber] = labelName
			labelNumber++
		}

		sort.Strings(labelNameList)

		for _, labelName := range labelNameList {
			labelValue := (*pm.Labels)[labelName]
			metric += fmt.Sprintf("%s=\"%s\",", pm.escape(labelName), pm.escape(labelValue))
		}
	}
	metric = strings.TrimRight(metric, ",")
	metric += fmt.Sprintf("} %f\n", pm.Value)
	return metric
}

func (pm *httpPrometheusMetric) Comment() string {
	var comment = ""
	if len(pm.Help) > 0 {
		comment += fmt.Sprintf("# HELP %s %s\n", pm.escape(pm.Name), pm.escape(pm.Help))
	}
	if len(pm.Type) > 0 {
		comment += fmt.Sprintf("# TYPE %s %s\n", pm.escape(pm.Name), pm.escape(pm.Type))
	}
	return comment
}

func (pm *httpPrometheusMetric) MarshalText() ([]byte, error) {
	return []byte(pm.String()), nil
}

func (hc *Coordinator) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch cluster list from the storage module
	requestFetchClusters := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- requestFetchClusters
	responseFetchClusters := <-requestFetchClusters.Reply

	replacer := strings.NewReplacer("\\", `\\`, "\n", `\n`, "\"", `\"`)
	host, _ := os.Hostname()
	hostLabel := replacer.Replace(host)

	var metric httpPrometheusMetric
	metricList := make([]httpPrometheusMetric, 0)

	for _, cluster := range responseFetchClusters.([]string) {
		clusterLabel := replacer.Replace(cluster)

		// Consumer Group List

		requestFetchConsumers := &protocol.StorageRequest{
			RequestType: protocol.StorageFetchConsumers,
			Cluster:     cluster,
			Reply:       make(chan interface{}),
		}
		hc.App.StorageChannel <- requestFetchConsumers
		responseFetchConsumers := <-requestFetchConsumers.Reply

		// Consumer Group Status

		for _, consumerGroup := range responseFetchConsumers.([]string) {

			// request the consumer group status structure
			requestConsumerGroupStatus := &protocol.EvaluatorRequest{
				Cluster: cluster,
				Group:   consumerGroup,
				ShowAll: true,
				Reply:   make(chan *protocol.ConsumerGroupStatus),
			}
			hc.App.EvaluatorChannel <- requestConsumerGroupStatus
			responseConsumerGroupStatus := <-requestConsumerGroupStatus.Reply

			// skip the consumer group if the data have not been fully gathered
			if responseConsumerGroupStatus.Complete < 1.0 {
				continue
			}

			consumerGroupLabels := map[string]string{
				"host":                  hostLabel,
				"cluster":               clusterLabel,
				"consumer_group":        replacer.Replace(consumerGroup),
				"consumer_group_status": replacer.Replace(responseConsumerGroupStatus.Status.String()),
			}

			metric = httpPrometheusMetric{
				Name:   "burrow_consumer_group_total_lag",
				Value:  float64(responseConsumerGroupStatus.TotalLag),
				Help:   "The sum of all consumed partition current lag values for the group.",
				Type:   "gauge",
				Labels: &consumerGroupLabels,
			}
			metricList = append(metricList, metric)

			metric = httpPrometheusMetric{
				Name:   "burrow_consumer_group_total_partitions",
				Value:  float64(responseConsumerGroupStatus.TotalPartitions),
				Help:   "The total count of all partitions that this groups has committed offsets for across all consumed topics. It may not be the same as the total number of partitions consumed by the group, if Burrow has not seen commits for all partitions yet.",
				Type:   "gauge",
				Labels: &consumerGroupLabels,
			}
			metricList = append(metricList, metric)

			metric = httpPrometheusMetric{
				Name:   "burrow_consumer_group_status_code",
				Value:  float64(responseConsumerGroupStatus.Status),
				Help:   "The status code of the consumer group. It is calculated from the highest status for the individual partitions. (Codes are index of this list starting from zero: NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND).",
				Type:   "gauge",
				Labels: &consumerGroupLabels,
			}
			metricList = append(metricList, metric)

			if responseConsumerGroupStatus.Maxlag != nil {
				metric = httpPrometheusMetric{
					Name:   "burrow_consumer_group_max_lag",
					Value:  float64(responseConsumerGroupStatus.Maxlag.CurrentLag),
					Help:   "The current lag value of a partition consumed by this group which has the highest lag.",
					Type:   "gauge",
					Labels: &consumerGroupLabels,
				}
				metricList = append(metricList, metric)
			}

			// Consumer Group Partition Status

			for _, partitionStatus := range responseConsumerGroupStatus.Partitions {

				// skip the consumer group partition if the data have not been fully gathered
				if partitionStatus.Complete < 1.0 {
					continue
				}

				consumerGroupPartitionLabels := map[string]string{
					"host":             hostLabel,
					"cluster":          clusterLabel,
					"consumer_group":   replacer.Replace(consumerGroup),
					"topic":            replacer.Replace(partitionStatus.Topic),
					"partition":        strconv.Itoa(int(partitionStatus.Partition)),
					"owner":            replacer.Replace(partitionStatus.Owner),
					"client_id":        replacer.Replace(partitionStatus.ClientID),
					"partition_status": replacer.Replace(partitionStatus.Status.String()),
				}

				metric = httpPrometheusMetric{
					Name:   "burrow_consumer_group_partition_current_lag",
					Value:  float64(partitionStatus.CurrentLag),
					Help:   "The current number of messages that the consumer is behind for this partition. This is calculated using the last committed offset and the current broker end offset.",
					Type:   "gauge",
					Labels: &consumerGroupPartitionLabels,
				}
				metricList = append(metricList, metric)

				metric = httpPrometheusMetric{
					Name:   "burrow_consumer_group_partition_status_code",
					Value:  float64(partitionStatus.Status),
					Help:   "The status code of the consumer group partition. (Codes are index of this list starting from zero: NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND).",
					Type:   "gauge",
					Labels: &consumerGroupPartitionLabels,
				}
				metricList = append(metricList, metric)

				metric = httpPrometheusMetric{
					Name:   "burrow_consumer_group_partition_latest_offset",
					Value:  float64(partitionStatus.End.Offset),
					Help:   "The latest offset value that Burrow is storing for this partition.",
					Type:   "counter",
					Labels: &consumerGroupPartitionLabels,
				}
				metricList = append(metricList, metric)

			}
		}

		// Topic List

		requestFetchTopics := &protocol.StorageRequest{
			RequestType: protocol.StorageFetchTopics,
			Cluster:     cluster,
			Reply:       make(chan interface{}),
		}
		hc.App.StorageChannel <- requestFetchTopics
		responseFetchTopics := <-requestFetchTopics.Reply

		// Topic Status

		for _, topic := range responseFetchTopics.([]string) {

			requestTopicDetail := &protocol.StorageRequest{
				RequestType: protocol.StorageFetchTopic,
				Cluster:     cluster,
				Topic:       topic,
				Reply:       make(chan interface{}),
			}
			hc.App.StorageChannel <- requestTopicDetail
			responseTopicDetail := <-requestTopicDetail.Reply

			for partitionNumber, partitionOffset := range responseTopicDetail.([]int64) {

				topicDetailLabels := map[string]string{
					"host":      hostLabel,
					"cluster":   clusterLabel,
					"topic":     replacer.Replace(topic),
					"partition": strconv.Itoa(partitionNumber),
				}

				metric = httpPrometheusMetric{
					Name:   "burrow_topic_partition_offset",
					Value:  float64(partitionOffset),
					Help:   "The offset of the partition in this topic.",
					Type:   "counter",
					Labels: &topicDetailLabels,
				}
				metricList = append(metricList, metric)

			}

		}

	}

	// Output

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)

	sort.Slice(metricList, func(a, b int) bool {
		return metricList[a].Name < metricList[b].Name
	})

	previousMetricName := ""
	for _, metric := range metricList {
		if metric.Name != previousMetricName {
			_, _ = w.Write([]byte("\n" + metric.Comment()))
		}
		previousMetricName = metric.Name
		_, _ = w.Write([]byte(metric.String()))
	}

}
