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
	"container/ring"
	"encoding/json"
	log "github.com/cihub/seelog"
	"regexp"
	"sync"
	"time"
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

type BrokerOffset struct {
	Offset    int64
	Timestamp int64
}

type ConsumerOffset struct {
	Offset    int64 `json:"offset"`
	Timestamp int64 `json:"timestamp"`
	Lag       int64 `json:"lag"`
}

type ClusterOffsets struct {
	broker       map[string][]*BrokerOffset
	consumer     map[string]map[string][]*ring.Ring
	brokerLock   *sync.RWMutex
	consumerLock *sync.RWMutex
}
type OffsetStorage struct {
	app            *ApplicationContext
	quit           chan struct{}
	offsetChannel  chan *PartitionOffset
	requestChannel chan interface{}
	offsets        map[string]*ClusterOffsets
	groupBlacklist *regexp.Regexp
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
	if (c >= 0) && (c <= 5) {
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
	Cluster    string             `json:"cluster"`
	Group      string             `json:"group"`
	Status     StatusConstant     `json:"status"`
	Complete   bool               `json:"complete"`
	Partitions []*PartitionStatus `json:"partitions"`
}

type ResponseTopicList struct {
	TopicList []string
	Error     bool
}
type ResponseOffsets struct {
	OffsetList []int64
	ErrorGroup bool
	ErrorTopic bool
}
type RequestClusterList struct {
	Result chan []string
}
type RequestConsumerList struct {
	Result  chan []string
	Cluster string
}
type RequestTopicList struct {
	Result  chan *ResponseTopicList
	Cluster string
	Group   string
}
type RequestOffsets struct {
	Result  chan *ResponseOffsets
	Cluster string
	Topic   string
	Group   string
}
type RequestConsumerStatus struct {
	Result  chan *ConsumerGroupStatus
	Cluster string
	Group   string
}
type RequestConsumerDrop struct {
	Result  chan StatusConstant
	Cluster string
	Group   string
}

func NewOffsetStorage(app *ApplicationContext) (*OffsetStorage, error) {
	storage := &OffsetStorage{
		app:            app,
		quit:           make(chan struct{}),
		offsetChannel:  make(chan *PartitionOffset, 10000),
		requestChannel: make(chan interface{}),
		offsets:        make(map[string]*ClusterOffsets),
	}

	if app.Config.General.GroupBlacklist != "" {
		re, err := regexp.Compile(app.Config.General.GroupBlacklist)
		if err != nil {
			return nil, err
		}
		storage.groupBlacklist = re
	}

	for cluster, _ := range app.Config.Kafka {
		storage.offsets[cluster] = &ClusterOffsets{
			broker:       make(map[string][]*BrokerOffset),
			consumer:     make(map[string]map[string][]*ring.Ring),
			brokerLock:   &sync.RWMutex{},
			consumerLock: &sync.RWMutex{},
		}
	}

	go func() {
		for {
			select {
			case o := <-storage.offsetChannel:
				if o.Group == "" {
					go storage.addBrokerOffset(o)
				} else {
					go storage.addConsumerOffset(o)
				}
			case r := <-storage.requestChannel:
				switch r.(type) {
				case *RequestConsumerList:
					request, _ := r.(*RequestConsumerList)
					go storage.requestConsumerList(request)
				case *RequestTopicList:
					request, _ := r.(*RequestTopicList)
					go storage.requestTopicList(request)
				case *RequestOffsets:
					request, _ := r.(*RequestOffsets)
					go storage.requestOffsets(request)
				case *RequestConsumerStatus:
					request, _ := r.(*RequestConsumerStatus)
					go storage.evaluateGroup(request.Cluster, request.Group, request.Result)
				case *RequestConsumerDrop:
					request, _ := r.(*RequestConsumerDrop)
					go storage.dropGroup(request.Cluster, request.Group, request.Result)
				default:
					// Silently drop unknown requests
				}
			case <-storage.quit:
				return
			}
		}
	}()

	return storage, nil
}

func (storage *OffsetStorage) addBrokerOffset(offset *PartitionOffset) {
	if _, ok := storage.offsets[offset.Cluster]; !ok {
		// Ignore offsets for clusters that we don't know about - should never happen anyways
		return
	}

	storage.offsets[offset.Cluster].brokerLock.Lock()
	if _, ok := storage.offsets[offset.Cluster].broker[offset.Topic]; !ok {
		storage.offsets[offset.Cluster].broker[offset.Topic] = make([]*BrokerOffset, offset.TopicPartitionCount)
	}
	if offset.TopicPartitionCount >= len(storage.offsets[offset.Cluster].broker[offset.Topic]) {
		// The partition count has increased. Append enough extra partitions to our slice
		for i := len(storage.offsets[offset.Cluster].broker[offset.Topic]); i < offset.TopicPartitionCount; i++ {
			storage.offsets[offset.Cluster].broker[offset.Topic] = append(storage.offsets[offset.Cluster].broker[offset.Topic], nil)
		}
	}

	if storage.offsets[offset.Cluster].broker[offset.Topic][offset.Partition] == nil {
		storage.offsets[offset.Cluster].broker[offset.Topic][offset.Partition] = &BrokerOffset{
			Offset:    offset.Offset,
			Timestamp: offset.Timestamp,
		}
	} else {
		storage.offsets[offset.Cluster].broker[offset.Topic][offset.Partition].Offset = offset.Offset
		storage.offsets[offset.Cluster].broker[offset.Topic][offset.Partition].Timestamp = offset.Timestamp
	}

	storage.offsets[offset.Cluster].brokerLock.Unlock()
}

func (storage *OffsetStorage) addConsumerOffset(offset *PartitionOffset) {
	// Ignore offsets for clusters that we don't know about - should never happen anyways
	if _, ok := storage.offsets[offset.Cluster]; !ok {
		return
	}

	// Ignore groups that match our blacklist
	if (storage.groupBlacklist != nil) && storage.groupBlacklist.MatchString(offset.Group) {
		return
	}

	// Get broker partition count and offset for this topic and partition first
	storage.offsets[offset.Cluster].brokerLock.RLock()
	if topic, ok := storage.offsets[offset.Cluster].broker[offset.Topic]; !ok || (ok && ((int32(len(topic)) <= offset.Partition) || (topic[offset.Partition] == nil))) {
		// If we don't have the partition or offset from the broker side yet, ignore the consumer offset for now
		storage.offsets[offset.Cluster].brokerLock.RUnlock()
		return
	}
	brokerOffset := storage.offsets[offset.Cluster].broker[offset.Topic][offset.Partition].Offset
	partitionCount := len(storage.offsets[offset.Cluster].broker[offset.Topic])
	storage.offsets[offset.Cluster].brokerLock.RUnlock()

	storage.offsets[offset.Cluster].consumerLock.Lock()
	if _, ok := storage.offsets[offset.Cluster].consumer[offset.Group]; !ok {
		storage.offsets[offset.Cluster].consumer[offset.Group] = make(map[string][]*ring.Ring)
	}
	if _, ok := storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic]; !ok {
		storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic] = make([]*ring.Ring, partitionCount)
	}
	if int(offset.Partition) >= len(storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic]) {
		// The partition count must have increased. Append enough extra partitions to our slice
		for i := len(storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic]); i < partitionCount; i++ {
			storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic] = append(storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic], nil)
		}
	}

	if storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition] == nil {
		storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition] = ring.New(storage.app.Config.Lagcheck.Intervals)
	}

	// Calculate the lag against the brokerOffset
	partitionLag := brokerOffset - offset.Offset
	if partitionLag < 0 {
		// Little bit of a hack - because we only get broker offsets periodically, it's possible the consumer offset could be ahead of where we think the broker
		// is. In this case, just mark it as zero lag.
		partitionLag = 0
	}

	// Update or create the ring value at the current pointer
	if storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition].Value == nil {
		storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition].Value = &ConsumerOffset{
			Offset:    offset.Offset,
			Timestamp: offset.Timestamp,
			Lag:       partitionLag,
		}
	} else {
		ringval, _ := storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition].Value.(*ConsumerOffset)
		ringval.Offset = offset.Offset
		ringval.Timestamp = offset.Timestamp
		ringval.Lag = partitionLag
	}

	// Advance the ring pointer
	storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition] = storage.offsets[offset.Cluster].consumer[offset.Group][offset.Topic][offset.Partition].Next()
	storage.offsets[offset.Cluster].consumerLock.Unlock()
}

func (storage *OffsetStorage) Stop() {
	close(storage.quit)
}

func (storage *OffsetStorage) dropGroup(cluster string, group string, resultChannel chan StatusConstant) {
	storage.offsets[cluster].consumerLock.Lock()

	if _, ok := storage.offsets[cluster].consumer[group]; ok {
		log.Infof("Removing group %s from cluster %s by request", group, cluster)
		delete(storage.offsets[cluster].consumer, group)
		resultChannel <- StatusOK
	} else {
		resultChannel <- StatusNotFound
	}

	storage.offsets[cluster].consumerLock.Unlock()
}

// Evaluate a consumer group based on specific rules about lag
// Rule 1: If over the stored period, the lag is ever zero for the partition, the period is OK
// Rule 2: If the consumer offset does not change, and the lag is non-zero, it's an error (partition is stalled)
// Rule 3: If the consumer offsets are moving, but the lag is consistently increasing, it's a warning (consumer is slow)
// Rule 4: If the difference between now and the last offset timestamp is greater than the difference between the last and first offset timestamps, the
//         consumer has stopped committing offsets for that partition (error)
// Rule 5: If the lag is -1, this is a special value that means there is no broker offset yet. Consider it good (will get caught in the next refresh of topics)
// Rule 6: If the consumer offset decreases from one interval to the next the partition is marked as a rewind (error)
func (storage *OffsetStorage) evaluateGroup(cluster string, group string, resultChannel chan *ConsumerGroupStatus) {
	status := &ConsumerGroupStatus{
		Cluster:    cluster,
		Group:      group,
		Status:     StatusNotFound,
		Complete:   true,
		Partitions: make([]*PartitionStatus, 0),
	}

	// Make sure the group even exists
	storage.offsets[cluster].consumerLock.RLock()
	if _, ok := storage.offsets[cluster].consumer[group]; !ok {
		storage.offsets[cluster].consumerLock.RUnlock()
		resultChannel <- status
		return
	}

	// Scan the offsets table once and store all the offsets for the group locally
	status.Status = StatusOK
	offsetList := make(map[string][][]ConsumerOffset, len(storage.offsets[cluster].consumer[group]))
	var youngestOffset int64
	for topic, partitions := range storage.offsets[cluster].consumer[group] {
		offsetList[topic] = make([][]ConsumerOffset, len(partitions))
		for partition, offsetRing := range partitions {
			// If we don't have our ring full yet, make sure we let the caller know
			if (offsetRing == nil) || (offsetRing.Prev().Value == nil) || (offsetRing.Value == nil) {
				status.Complete = false
				continue
			}

			// Pull out the offsets once so we can unlock the map
			offsetList[topic][partition] = make([]ConsumerOffset, storage.app.Config.Lagcheck.Intervals)
			idx := -1
			offsetRing.Do(func(val interface{}) {
				idx += 1
				ptr, _ := val.(*ConsumerOffset)
				offsetList[topic][partition][idx] = *ptr

				// Track the youngest offset we have found to check expiration
				if offsetList[topic][partition][idx].Timestamp > youngestOffset {
					youngestOffset = offsetList[topic][partition][idx].Timestamp
				}
			})
		}
	}
	storage.offsets[cluster].consumerLock.RUnlock()

	// If the youngest offset is older than our expiration window, flush the group
	if (youngestOffset > 0) && (youngestOffset < ((time.Now().Unix() - storage.app.Config.Lagcheck.ExpireGroup) * 1000)) {
		storage.offsets[cluster].consumerLock.Lock()
		log.Infof("Removing expired group %s from cluster %s", group, cluster)
		delete(storage.offsets[cluster].consumer, group)
		storage.offsets[cluster].consumerLock.Unlock()

		// Return the group as a 404
		status.Status = StatusNotFound
		resultChannel <- status
		return
	}

	for topic, partitions := range offsetList {
		for partition, offsets := range partitions {
			// Skip partitions we're missing offsets for
			if len(offsets) == 0 {
				continue
			}
			maxidx := len(offsets) - 1

			// Rule 5 - we're missing broker offsets so we're not complete yet
			if offsets[0].Lag == -1 {
				status.Complete = false
				continue
			}

			// Rule 4 - Offsets haven't been committed in a while
			if ((time.Now().Unix() * 1000) - offsets[maxidx].Timestamp) > (offsets[maxidx].Timestamp - offsets[0].Timestamp) {
				status.Status = StatusError
				status.Partitions = append(status.Partitions, &PartitionStatus{
					Topic:     topic,
					Partition: int32(partition),
					Status:    StatusStop,
					Start:     offsets[0],
					End:       offsets[maxidx],
				})
				continue
			}

			// Rule 6 - Did the consumer offsets rewind at any point?
			// We check this first because we always want to know about a rewind - it's bad behavior
			for i := 1; i <= maxidx; i++ {
				if offsets[i].Offset < offsets[i-1].Offset {
					status.Status = StatusError
					status.Partitions = append(status.Partitions, &PartitionStatus{
						Topic:     topic,
						Partition: int32(partition),
						Status:    StatusRewind,
						Start:     offsets[0],
						End:       offsets[maxidx],
					})
					continue
				}
			}

			// Rule 1
			if offsets[maxidx].Lag == 0 {
				continue
			}
			if offsets[maxidx].Offset == offsets[0].Offset {
				// Rule 1
				if offsets[0].Lag == 0 {
					continue
				}

				// Rule 2
				status.Status = StatusError
				status.Partitions = append(status.Partitions, &PartitionStatus{
					Topic:     topic,
					Partition: int32(partition),
					Status:    StatusStall,
					Start:     offsets[0],
					End:       offsets[maxidx],
				})
			} else {
				// Rule 1 passes, or shortcut a full check on Rule 3 if we can
				if (offsets[0].Lag == 0) || (offsets[maxidx].Lag <= offsets[0].Lag) {
					continue
				}

				lagDropped := false
				for i := 0; i <= maxidx; i++ {
					// Rule 1 passes or Rule 3 is shortcut (lag dropped somewhere in the period)
					if (offsets[i].Lag == 0) || ((i > 0) && (offsets[i].Lag < offsets[i-1].Lag)) {
						lagDropped = true
						break
					}
				}

				if !lagDropped {
					// Rule 3
					if status.Status == StatusOK {
						status.Status = StatusWarning
					}
					status.Partitions = append(status.Partitions, &PartitionStatus{
						Topic:     topic,
						Partition: int32(partition),
						Status:    StatusWarning,
						Start:     offsets[0],
						End:       offsets[maxidx],
					})
				}
			}
		}
	}
	resultChannel <- status
}

func (storage *OffsetStorage) Evaluate(resultChannel chan *ConsumerGroupStatus) {
	for cluster, _ := range storage.offsets {
		for group, _ := range storage.offsets[cluster].consumer {
			go storage.evaluateGroup(cluster, group, resultChannel)
		}
	}
}

func (storage *OffsetStorage) requestClusterList(request *RequestClusterList) {
	clusterList := make([]string, len(storage.offsets))
	i := 0
	for group, _ := range storage.offsets {
		clusterList[i] = group
		i += 1
	}

	request.Result <- clusterList
}

func (storage *OffsetStorage) requestConsumerList(request *RequestConsumerList) {
	if _, ok := storage.offsets[request.Cluster]; !ok {
		request.Result <- make([]string, 0)
		return
	}

	storage.offsets[request.Cluster].consumerLock.RLock()
	consumerList := make([]string, len(storage.offsets[request.Cluster].consumer))
	i := 0
	for group := range storage.offsets[request.Cluster].consumer {
		consumerList[i] = group
		i += 1
	}
	storage.offsets[request.Cluster].consumerLock.RUnlock()

	request.Result <- consumerList
}

func (storage *OffsetStorage) requestTopicList(request *RequestTopicList) {
	if _, ok := storage.offsets[request.Cluster]; !ok {
		request.Result <- &ResponseTopicList{Error: true}
		return
	}

	response := &ResponseTopicList{Error: false}
	if request.Group == "" {
		storage.offsets[request.Cluster].brokerLock.RLock()
		response.TopicList = make([]string, len(storage.offsets[request.Cluster].broker))
		i := 0
		for topic := range storage.offsets[request.Cluster].broker {
			response.TopicList[i] = topic
			i += 1
		}
		storage.offsets[request.Cluster].brokerLock.RUnlock()
	} else {
		storage.offsets[request.Cluster].consumerLock.RLock()
		if _, ok := storage.offsets[request.Cluster].consumer[request.Group]; ok {
			response.TopicList = make([]string, len(storage.offsets[request.Cluster].consumer[request.Group]))
			i := 0
			for topic := range storage.offsets[request.Cluster].consumer[request.Group] {
				response.TopicList[i] = topic
				i += 1
			}
		} else {
			response.Error = true
		}
		storage.offsets[request.Cluster].consumerLock.RUnlock()
	}
	request.Result <- response
}

func (storage *OffsetStorage) requestOffsets(request *RequestOffsets) {
	if _, ok := storage.offsets[request.Cluster]; !ok {
		request.Result <- &ResponseOffsets{ErrorTopic: true, ErrorGroup: true}
		return
	}

	response := &ResponseOffsets{ErrorGroup: false, ErrorTopic: false}
	if request.Group == "" {
		storage.offsets[request.Cluster].brokerLock.RLock()
		if _, ok := storage.offsets[request.Cluster].broker[request.Topic]; ok {
			response.OffsetList = make([]int64, len(storage.offsets[request.Cluster].broker[request.Topic]))
			for partition, offset := range storage.offsets[request.Cluster].broker[request.Topic] {
				if offset == nil {
					response.OffsetList[partition] = -1
				} else {
					response.OffsetList[partition] = offset.Offset
				}
			}
		} else {
			response.ErrorTopic = true
		}
		storage.offsets[request.Cluster].brokerLock.RUnlock()
	} else {
		storage.offsets[request.Cluster].consumerLock.RLock()
		if _, ok := storage.offsets[request.Cluster].consumer[request.Group]; ok {
			if _, ok := storage.offsets[request.Cluster].consumer[request.Group][request.Topic]; ok {
				response.OffsetList = make([]int64, len(storage.offsets[request.Cluster].consumer[request.Group][request.Topic]))
				for partition, oring := range storage.offsets[request.Cluster].consumer[request.Group][request.Topic] {
					if oring == nil {
						response.OffsetList[partition] = -1
					} else {
						offset, _ := oring.Prev().Value.(*ConsumerOffset)
						if offset == nil {
							response.OffsetList[partition] = -1
						} else {
							response.OffsetList[partition] = offset.Offset
						}
					}
				}
			} else {
				response.ErrorTopic = true
			}
		} else {
			response.ErrorGroup = true
		}
		storage.offsets[request.Cluster].consumerLock.RUnlock()
	}
	request.Result <- response
}
