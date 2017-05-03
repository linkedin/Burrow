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
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/linkedin/Burrow/protocol"
	"regexp"
	"sync"
	"time"
)

type OffsetStorage struct {
	app            *ApplicationContext
	quit           chan struct{}
	offsetChannel  chan *protocol.PartitionOffset
	requestChannel chan interface{}
	offsets        map[string]*ClusterOffsets
	groupBlacklist *regexp.Regexp
	groupWhitelist *regexp.Regexp
}

type BrokerOffset struct {
	Offset    int64
	Timestamp int64
}

type ClusterOffsets struct {
	broker       map[string][]*BrokerOffset
	consumer     map[string]map[string][]*ring.Ring
	brokerLock   *sync.RWMutex
	consumerLock *sync.RWMutex
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
	Result  chan *protocol.ConsumerGroupStatus
	Cluster string
	Group   string
	Showall bool
}
type RequestConsumerDrop struct {
	Result  chan protocol.StatusConstant
	Cluster string
	Group   string
}

func NewOffsetStorage(app *ApplicationContext) (*OffsetStorage, error) {
	storage := &OffsetStorage{
		app:            app,
		quit:           make(chan struct{}),
		offsetChannel:  make(chan *protocol.PartitionOffset, 10000),
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

	if app.Config.General.GroupWhitelist != "" {
		re, err := regexp.Compile(app.Config.General.GroupWhitelist)
		if err != nil {
			return nil, err
		}
		storage.groupWhitelist = re
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
					go storage.evaluateGroup(request.Cluster, request.Group, request.Result, request.Showall)
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

func (storage *OffsetStorage) addBrokerOffset(offset *protocol.PartitionOffset) {
	clusterMap, ok := storage.offsets[offset.Cluster]
	if !ok {
		// Ignore offsets for clusters that we don't know about - should never happen anyways
		return
	}

	clusterMap.brokerLock.Lock()
	topicList, ok := clusterMap.broker[offset.Topic]
	if !ok {
		clusterMap.broker[offset.Topic] = make([]*BrokerOffset, offset.TopicPartitionCount)
		topicList = clusterMap.broker[offset.Topic]
	}
	if offset.TopicPartitionCount >= len(topicList) {
		// The partition count has increased. Append enough extra partitions to our slice
		for i := len(topicList); i < offset.TopicPartitionCount; i++ {
			topicList = append(topicList, nil)
		}
	}

	partitionEntry := topicList[offset.Partition]
	if partitionEntry == nil {
		topicList[offset.Partition] = &BrokerOffset{
			Offset:    offset.Offset,
			Timestamp: offset.Timestamp,
		}
		partitionEntry = topicList[offset.Partition]
	} else {
		partitionEntry.Offset = offset.Offset
		partitionEntry.Timestamp = offset.Timestamp
	}

	clusterMap.brokerLock.Unlock()
}

func (storage *OffsetStorage) addConsumerOffset(offset *protocol.PartitionOffset) {
	// Ignore offsets for clusters that we don't know about - should never happen anyways
	clusterOffsets, ok := storage.offsets[offset.Cluster]
	if !ok {
		return
	}

	// Ignore groups that are out of filter bounds
	if !storage.AcceptConsumerGroup(offset.Group) {
		log.Debugf("Dropped offset (black/white list): cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v",
			offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset)
		return
	}

	// Get broker partition count and offset for this topic and partition first
	clusterOffsets.brokerLock.RLock()
	topicPartitionList, ok := clusterOffsets.broker[offset.Topic]
	if !ok {
		// We don't know about this topic from the brokers yet - skip consumer offsets for now
		clusterOffsets.brokerLock.RUnlock()
		log.Debugf("Dropped offset (no topic): cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v",
			offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset)
		return
	}
	if offset.Partition < 0 {
		// This should never happen, but if it does, log an warning with the offset information for review
		log.Warnf("Got a negative partition ID: cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v",
			offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset)
		clusterOffsets.brokerLock.RUnlock()
		return
	}
	if offset.Partition >= int32(len(topicPartitionList)) {
		// We know about the topic, but partitions have been expanded and we haven't seen that from the broker yet
		clusterOffsets.brokerLock.RUnlock()
		log.Debugf("Dropped offset (expanded): cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v",
			offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset)
		return
	}
	if topicPartitionList[offset.Partition] == nil {
		// We know about the topic and partition, but we haven't actually gotten the broker offset yet
		clusterOffsets.brokerLock.RUnlock()
		log.Debugf("Dropped offset (broker offset): cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v",
			offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset)
		return
	}
	brokerOffset := topicPartitionList[offset.Partition].Offset
	partitionCount := len(topicPartitionList)
	clusterOffsets.brokerLock.RUnlock()

	clusterOffsets.consumerLock.Lock()
	consumerMap, ok := clusterOffsets.consumer[offset.Group]
	if !ok {
		clusterOffsets.consumer[offset.Group] = make(map[string][]*ring.Ring)
		consumerMap = clusterOffsets.consumer[offset.Group]
	}
	consumerTopicMap, ok := consumerMap[offset.Topic]
	if !ok {
		consumerMap[offset.Topic] = make([]*ring.Ring, partitionCount)
		consumerTopicMap = consumerMap[offset.Topic]
	}
	if int(offset.Partition) >= len(consumerTopicMap) {
		// The partition count must have increased. Append enough extra partitions to our slice
		for i := len(consumerTopicMap); i < partitionCount; i++ {
			consumerTopicMap = append(consumerTopicMap, nil)
		}
	}

	consumerPartitionRing := consumerTopicMap[offset.Partition]
	if consumerPartitionRing == nil {
		consumerTopicMap[offset.Partition] = ring.New(storage.app.Config.Lagcheck.Intervals)
		consumerPartitionRing = consumerTopicMap[offset.Partition]
	} else {
		lastOffset := consumerPartitionRing.Prev().Value.(*protocol.ConsumerOffset)
		timestampDifference := offset.Timestamp - lastOffset.Timestamp

		// Prevent old offset commits, but only if the offsets don't advance (because of artifical commits below)
		if (timestampDifference <= 0) && (offset.Offset <= lastOffset.Offset) {
			clusterOffsets.consumerLock.Unlock()
			log.Debugf("Dropped offset (noadvance): cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v tsdiff=%v lag=%v",
				offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset,
				timestampDifference, brokerOffset-offset.Offset)
			return
		}

		// Prevent new commits that are too fast (less than the min-distance config) if the last offset was not artificial
		if (!lastOffset.Artificial) && (timestampDifference >= 0) && (timestampDifference < (storage.app.Config.Lagcheck.MinDistance * 1000)) {
			// Store the current offset before dropping the ConsumerOffset.
			// This might be the last offset we receive for a while, and it can help us decide if the consumer is STOPPED because
			// it has processed all messages.
			lastOffset.MaxOffset = offset.Offset
			clusterOffsets.consumerLock.Unlock()
			log.Debugf("Dropped offset (mindistance): cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v tsdiff=%v lag=%v",
				offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset,
				timestampDifference, brokerOffset-offset.Offset)
			return
		}
	}

	// Calculate the lag against the brokerOffset
	partitionLag := brokerOffset - offset.Offset
	if partitionLag < 0 {
		// Little bit of a hack - because we only get broker offsets periodically, it's possible the consumer offset could be ahead of where we think the broker
		// is. In this case, just mark it as zero lag.
		partitionLag = 0
	}

	// Update or create the ring value at the current pointer
	if consumerPartitionRing.Value == nil {
		consumerPartitionRing.Value = &protocol.ConsumerOffset{
			Offset:     offset.Offset,
			MaxOffset:  offset.Offset,
			Timestamp:  offset.Timestamp,
			Lag:        partitionLag,
			Artificial: false,
		}
	} else {
		ringval, _ := consumerPartitionRing.Value.(*protocol.ConsumerOffset)
		ringval.Offset = offset.Offset
		ringval.MaxOffset = offset.Offset
		ringval.Timestamp = offset.Timestamp
		ringval.Lag = partitionLag
		ringval.Artificial = false
	}

	log.Tracef("Commit offset: cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v lag=%v",
		offset.Cluster, offset.Topic, offset.Partition, offset.Group, offset.Timestamp, offset.Offset,
		partitionLag)


	// Advance the ring pointer
	consumerTopicMap[offset.Partition] = consumerTopicMap[offset.Partition].Next()
	clusterOffsets.consumerLock.Unlock()
}

func (storage *OffsetStorage) Stop() {
	close(storage.quit)
}

func (storage *OffsetStorage) dropGroup(cluster string, group string, resultChannel chan protocol.StatusConstant) {
	storage.offsets[cluster].consumerLock.Lock()

	if _, ok := storage.offsets[cluster].consumer[group]; ok {
		log.Infof("Removing group %s from cluster %s by request", group, cluster)
		delete(storage.offsets[cluster].consumer, group)
		resultChannel <- protocol.StatusOK
	} else {
		resultChannel <- protocol.StatusNotFound
	}

	storage.offsets[cluster].consumerLock.Unlock()
}

// Evaluate a consumer group based on specific rules about lag
// Rule 1:  If over the stored period, the lag is ever zero for the partition, the period is OK
// Rule 2:  If the consumer offset does not change, and the lag is non-zero, it's an error (partition is stalled)
// Rule 3:  If the consumer offsets are moving, but the lag is consistently increasing, it's a warning (consumer is slow)
// Rule 4:  If the difference between now and the last offset timestamp is greater than the difference between the last and first offset timestamps, the
//          consumer has stopped committing offsets for that partition (error), unless
// Rule 5:  If the lag is -1, this is a special value that means there is no broker offset yet. Consider it good (will get caught in the next refresh of topics)
// Rule 6:  If the consumer offset decreases from one interval to the next the partition is marked as a rewind (error)
func (storage *OffsetStorage) evaluateGroup(cluster string, group string, resultChannel chan *protocol.ConsumerGroupStatus, showall bool) {
	status := &protocol.ConsumerGroupStatus{
		Cluster:    cluster,
		Group:      group,
		Status:     protocol.StatusNotFound,
		Complete:   true,
		Partitions: make([]*protocol.PartitionStatus, 0),
		Maxlag:     nil,
		TotalLag:   0,
	}

	// Make sure the cluster exists
	clusterMap, ok := storage.offsets[cluster]
	if !ok {
		resultChannel <- status
		return
	}

	// Make sure the group even exists
	clusterMap.consumerLock.Lock()
	consumerMap, ok := clusterMap.consumer[group]
	if !ok {
		clusterMap.consumerLock.Unlock()
		resultChannel <- status
		return
	}

	// Scan the offsets table once and store all the offsets for the group locally
	status.Status = protocol.StatusOK
	offsetList := make(map[string][][]protocol.ConsumerOffset, len(consumerMap))
	var youngestOffset int64
	for topic, partitions := range consumerMap {
		offsetList[topic] = make([][]protocol.ConsumerOffset, len(partitions))
		for partition, offsetRing := range partitions {
			status.TotalPartitions += 1

			// If we don't have our ring full yet, make sure we let the caller know
			if (offsetRing == nil) || (offsetRing.Value == nil) {
				status.Complete = false
				continue
			}

			// Add an artificial offset commit if the consumer has no lag against the current broker offset	AND if the distance away from the
			// last offset timestamp is outside the MinDistance threshold
			lastOffset := offsetRing.Prev().Value.(*protocol.ConsumerOffset)
			timestampDifference := (time.Now().Unix() * 1000) - lastOffset.Timestamp
			if lastOffset.MaxOffset >= clusterMap.broker[topic][partition].Offset && (timestampDifference >= (storage.app.Config.Lagcheck.MinDistance * 1000)) {
				ringval, _ := offsetRing.Value.(*protocol.ConsumerOffset)
				ringval.Offset = lastOffset.Offset
				ringval.MaxOffset = lastOffset.MaxOffset
				ringval.Timestamp = time.Now().Unix() * 1000
				ringval.Lag = 0
				ringval.Artificial = true
				partitions[partition] = partitions[partition].Next()

				log.Tracef("Artificial offset: cluster=%s topic=%s partition=%v group=%s timestamp=%v offset=%v max_offset=%v lag=0",
					cluster, topic, partition, group, ringval.Timestamp, lastOffset.Offset, lastOffset.MaxOffset)
			}

			// Pull out the offsets once so we can unlock the map
			offsetList[topic][partition] = make([]protocol.ConsumerOffset, storage.app.Config.Lagcheck.Intervals)
			partitionMap := offsetList[topic][partition]
			idx := -1
			partitions[partition].Do(func(val interface{}) {
				idx += 1
				ptr, _ := val.(*protocol.ConsumerOffset)
				partitionMap[idx] = *ptr

				// Track the youngest offset we have found to check expiration
				if partitionMap[idx].Timestamp > youngestOffset {
					youngestOffset = partitionMap[idx].Timestamp
				}
			})
		}
	}

	// If the youngest offset is earlier than our expiration window, flush the group
	if (youngestOffset > 0) && (youngestOffset < ((time.Now().Unix() - storage.app.Config.Lagcheck.ExpireGroup) * 1000)) {
		log.Infof("Removing expired group %s from cluster %s", group, cluster)
		delete(clusterMap.consumer, group)
		clusterMap.consumerLock.Unlock()

		// Return the group as a 404
		status.Status = protocol.StatusNotFound
		resultChannel <- status
		return
	}
	clusterMap.consumerLock.Unlock()

	var maxlag int64
	for topic, partitions := range offsetList {
		for partition, offsets := range partitions {
			// Skip partitions we're missing offsets for
			if len(offsets) == 0 {
				continue
			}
			maxidx := len(offsets) - 1
			firstOffset := offsets[0]
			lastOffset := offsets[maxidx]

			// Rule 5 - we're missing broker offsets so we're not complete yet
			if firstOffset.Lag == -1 {
				status.Complete = false
				continue
			}

			// We may always add this partition, so create it once
			thispart := &protocol.PartitionStatus{
				Topic:     topic,
				Partition: int32(partition),
				Status:    protocol.StatusOK,
				Start:     firstOffset,
				End:       lastOffset,
			}

			// Check if this partition is the one with the most lag currently
			if lastOffset.Lag > maxlag {
				status.Maxlag = thispart
				maxlag = lastOffset.Lag
			}
			status.TotalLag += uint64(lastOffset.Lag)

			// Rule 4 - Offsets haven't been committed in a while
			if ((time.Now().Unix() * 1000) - lastOffset.Timestamp) > (lastOffset.Timestamp - firstOffset.Timestamp) {
				status.Status = protocol.StatusError
				thispart.Status = protocol.StatusStop
				status.Partitions = append(status.Partitions, thispart)
				continue
			}

			// Rule 6 - Did the consumer offsets rewind at any point?
			// We check this first because we always want to know about a rewind - it's bad behavior
			for i := 1; i <= maxidx; i++ {
				if offsets[i].Offset < offsets[i-1].Offset {
					status.Status = protocol.StatusError
					thispart.Status = protocol.StatusRewind
					status.Partitions = append(status.Partitions, thispart)
					continue
				}
			}

			// Rule 1
			if lastOffset.Lag == 0 {
				if showall {
					status.Partitions = append(status.Partitions, thispart)
				}
				continue
			}
			if lastOffset.Offset == firstOffset.Offset {
				// Rule 1
				if firstOffset.Lag == 0 {
					if showall {
						status.Partitions = append(status.Partitions, thispart)
					}
					continue
				}

				// Rule 2
				status.Status = protocol.StatusError
				thispart.Status = protocol.StatusStall
			} else {
				// Rule 1 passes, or shortcut a full check on Rule 3 if we can
				if (firstOffset.Lag == 0) || (lastOffset.Lag <= firstOffset.Lag) {
					if showall {
						status.Partitions = append(status.Partitions, thispart)
					}
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
					if status.Status == protocol.StatusOK {
						status.Status = protocol.StatusWarning
					}
					thispart.Status = protocol.StatusWarning
				}
			}

			// Always add the partition if it's not OK
			if (thispart.Status != protocol.StatusOK) || showall {
				status.Partitions = append(status.Partitions, thispart)
			}
		}
	}
	resultChannel <- status
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
						offset, _ := oring.Prev().Value.(*protocol.ConsumerOffset)
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

func (storage *OffsetStorage) debugPrintGroup(cluster string, group string) {
	// Make sure the cluster exists
	clusterMap, ok := storage.offsets[cluster]
	if !ok {
		log.Debugf("Detail cluster=%s,group=%s: No Cluster", cluster, group)
		return
	}

	// Make sure the group even exists
	clusterMap.consumerLock.RLock()
	consumerMap, ok := clusterMap.consumer[group]
	if !ok {
		clusterMap.consumerLock.RUnlock()
		log.Debugf("Detail cluster=%s,group=%s: No Group", cluster, group)
		return
	}

	// Scan the offsets table and print all partitions (full ring) for the group
	for topic, partitions := range consumerMap {
		for partition, offsetRing := range partitions {
			if (offsetRing == nil) || (offsetRing.Value == nil) {
				log.Debugf("Detail cluster=%s,group=%s,topic=%s,partition=%v: No Ring", cluster, group, topic, partition)
				continue
			}

			// Pull out the offsets once so we can unlock the map
			ringStr := ""
			offsetRing.Do(func(val interface{}) {
				if val == nil {
					ringStr += "(),"
				} else {
					ptr, _ := val.(*protocol.ConsumerOffset)
					ringStr += fmt.Sprintf("(%v,%v,%v,%v)", ptr.Timestamp, ptr.Offset, ptr.Lag, ptr.Artificial)
				}
			})
			log.Debugf("Detail cluster=%s,group=%s,topic=%s,partition=%v: %s", cluster, group, topic, partition, ringStr)
		}
	}
	clusterMap.consumerLock.RUnlock()
}

func (storage *OffsetStorage) AcceptConsumerGroup(group string) bool {
	// First check to make sure group is in the whitelist
	if (storage.groupWhitelist != nil) && !storage.groupWhitelist.MatchString(group) {
		return false;
	}

	// The group is in the whitelist (or there is not whitelist).  Now check the blacklist
	if (storage.groupBlacklist != nil) && storage.groupBlacklist.MatchString(group) {
		return false;
	}

	// good to go
	return true;
}
