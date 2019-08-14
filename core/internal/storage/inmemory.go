/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package storage

import (
	"container/ring"
	"math/rand"
	"regexp"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

// InMemoryStorage is a storage module that maintains the entire data set in memory in a series of maps. It has a
// configurable number of worker goroutines to service requests, and for requests that are group-specific, the group
// and cluster name are used to hash the request to a consistent worker. This assures that requests for a group are
// processed in order.
type InMemoryStorage struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name        string
	intervals   int
	numWorkers  int
	expireGroup int64
	minDistance int64
	queueDepth  int

	requestChannel chan *protocol.StorageRequest
	workersRunning sync.WaitGroup
	mainRunning    sync.WaitGroup
	offsets        map[string]clusterOffsets
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	workers        []chan *protocol.StorageRequest
}

type brokerOffset struct {
	Offset    int64
	Timestamp int64
}

type consumerPartition struct {
	offsets  *ring.Ring
	owner    string
	clientID string
}

type consumerGroup struct {
	// This lock is held when using the individual group, either for read or write
	lock       *sync.RWMutex
	topics     map[string][]*consumerPartition
	lastCommit int64
}

type clusterOffsets struct {
	broker   map[string][]*ring.Ring
	consumer map[string]*consumerGroup

	// This lock is used when modifying broker topics or offsets
	brokerLock *sync.RWMutex

	// This lock is used when modifying the overall consumer list
	// It does not need to be held for modifying an individual group
	consumerLock *sync.RWMutex
}

// Represents the destination of adding an offset into
// the consumer offsets ring buffer.
// `insertDest`: the destination for an insert
// `extendDest`: the next slot in the ring, which will be overridden
//
// If only `extendDest` is set, we're appending to the ring.
// If only `insertDest` is set, we're replacing an existing slot.
// If both are set, we're inserting before some existing items,
// i.e. shifting all items past `insertDest` forward by one,
type offsetRingDestination struct {
	insertDest *ring.Ring
	extendDest *ring.Ring
}

func (destination *offsetRingDestination) destinationSlot() *ring.Ring {
	if destination.insertDest != nil {
		return destination.insertDest
	}
	return destination.extendDest
}

func (destination *offsetRingDestination) isAppend() bool {
	return destination.insertDest == nil
}

func (destination *offsetRingDestination) isShift() bool {
	return destination.insertDest != nil && destination.extendDest != nil
}

// Configure validates the configuration for the module, creates a channel to receive requests on, and sets up the
// storage map. If no expiration time for groups is set, a default value of 7 days is used. If no interval count is
// set, a default of 10 intervals is used. If no worker count is set, a default of 20 workers is used.
func (module *InMemoryStorage) Configure(name string, configRoot string) {
	module.Log.Info("configuring")

	module.name = name

	// Set defaults for configs if needed
	viper.SetDefault(configRoot+".intervals", 10)
	viper.SetDefault(configRoot+".expire-group", 604800)
	viper.SetDefault(configRoot+".workers", 20)
	viper.SetDefault(configRoot+".queue-depth", 1)
	module.intervals = viper.GetInt(configRoot + ".intervals")
	module.expireGroup = viper.GetInt64(configRoot + ".expire-group")
	module.numWorkers = viper.GetInt(configRoot + ".workers")
	module.minDistance = viper.GetInt64(configRoot + ".min-distance")
	module.queueDepth = viper.GetInt(configRoot + ".queue-depth")

	module.requestChannel = make(chan *protocol.StorageRequest, module.queueDepth)
	module.workersRunning = sync.WaitGroup{}
	module.mainRunning = sync.WaitGroup{}
	module.offsets = make(map[string]clusterOffsets)

	whitelist := viper.GetString(configRoot + ".group-whitelist")
	if whitelist != "" {
		re, err := regexp.Compile(whitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}

	blacklist := viper.GetString(configRoot + ".group-blacklist")
	if blacklist != "" {
		re, err := regexp.Compile(blacklist)
		if err != nil {
			module.Log.Panic("Failed to compile group blacklist")
			panic(err)
		}
		module.groupBlacklist = re
	}
}

// GetCommunicationChannel returns the RequestChannel that has been setup for this module.
func (module *InMemoryStorage) GetCommunicationChannel() chan *protocol.StorageRequest {
	return module.requestChannel
}

// Start sets up the rest of the storage map for each configured cluster. It then starts the configured number of
// worker routines to handle requests. Finally, it starts a main loop which will receive requests and hash them to the
// correct worker.
func (module *InMemoryStorage) Start() error {
	module.Log.Info("starting")

	for cluster := range viper.GetStringMap("cluster") {
		module.
			offsets[cluster] = clusterOffsets{
			broker:       make(map[string][]*ring.Ring),
			consumer:     make(map[string]*consumerGroup),
			brokerLock:   &sync.RWMutex{},
			consumerLock: &sync.RWMutex{},
		}
	}

	// Start the appropriate number of workers, with a channel for each
	module.workers = make([]chan *protocol.StorageRequest, module.numWorkers)
	for i := 0; i < module.numWorkers; i++ {
		module.workers[i] = make(chan *protocol.StorageRequest, module.queueDepth)
		module.workersRunning.Add(1)
		go module.requestWorker(i, module.workers[i])
	}

	module.mainRunning.Add(1)
	go module.mainLoop()
	return nil
}

// Stop closes the incoming request channel, which will close the main loop. It then closes each of the worker
// channels, to close the workers, and waits for all goroutines to exit before returning.
func (module *InMemoryStorage) Stop() error {
	module.Log.Info("stopping")

	close(module.requestChannel)
	module.mainRunning.Wait()

	for i := 0; i < module.numWorkers; i++ {
		close(module.workers[i])
	}
	module.workersRunning.Wait()

	return nil
}

func (module *InMemoryStorage) requestWorker(workerNum int, requestChannel chan *protocol.StorageRequest) {
	defer module.workersRunning.Done()

	// Using a map for the request types avoids a bit of complexity below
	var requestTypeMap = map[protocol.StorageRequestConstant]func(*protocol.StorageRequest, *zap.Logger){
		protocol.StorageSetBrokerOffset:        module.addBrokerOffset,
		protocol.StorageSetConsumerOffset:      module.addConsumerOffset,
		protocol.StorageSetConsumerOwner:       module.addConsumerOwner,
		protocol.StorageSetDeleteTopic:         module.deleteTopic,
		protocol.StorageSetDeleteGroup:         module.deleteGroup,
		protocol.StorageFetchClusters:          module.fetchClusterList,
		protocol.StorageFetchConsumers:         module.fetchConsumerList,
		protocol.StorageFetchTopics:            module.fetchTopicList,
		protocol.StorageFetchConsumer:          module.fetchConsumer,
		protocol.StorageFetchTopic:             module.fetchTopic,
		protocol.StorageClearConsumerOwners:    module.clearConsumerOwners,
		protocol.StorageFetchConsumersForTopic: module.fetchConsumersForTopicList,
	}

	workerLogger := module.Log.With(zap.Int("worker", workerNum))
	for r := range requestChannel {
		if requestFunc, ok := requestTypeMap[r.RequestType]; ok {
			requestFunc(r, workerLogger.With(
				zap.String("cluster", r.Cluster),
				zap.String("consumer", r.Group),
				zap.String("topic", r.Topic),
				zap.Int32("partition", r.Partition),
				zap.Int32("topic_partition_count", r.TopicPartitionCount),
				zap.Int64("offset", r.Offset),
				zap.Int64("timestamp", r.Timestamp),
				zap.String("owner", r.Owner),
				zap.String("client_id", r.ClientID),
				zap.String("request", r.RequestType.String())))
		}
	}
}

func (module *InMemoryStorage) mainLoop() {
	defer module.mainRunning.Done()

	for r := range module.requestChannel {
		switch r.RequestType {
		case protocol.StorageSetBrokerOffset, protocol.StorageSetDeleteTopic, protocol.StorageFetchClusters, protocol.StorageFetchConsumers, protocol.StorageFetchTopics, protocol.StorageFetchTopic, protocol.StorageFetchConsumersForTopic:
			// Send to any worker
			module.workers[int(rand.Int31n(int32(module.numWorkers)))] <- r
		case protocol.StorageSetConsumerOffset, protocol.StorageSetConsumerOwner, protocol.StorageSetDeleteGroup, protocol.StorageClearConsumerOwners, protocol.StorageFetchConsumer:
			// Hash to a consistent worker
			module.workers[int(xxhash.ChecksumString64(r.Cluster+r.Group)%uint64(module.numWorkers))] <- r
		default:
			module.Log.Error("unknown storage request type",
				zap.Int("request_type", int(r.RequestType)),
			)
			if r.Reply != nil {
				close(r.Reply)
			}
		}
	}
}

func (module *InMemoryStorage) addBrokerOffset(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		// Ignore offsets for clusters that we don't know about - should never happen anyways
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.brokerLock.Lock()
	defer clusterMap.brokerLock.Unlock()

	topicList, ok := clusterMap.broker[request.Topic]
	if !ok {
		clusterMap.broker[request.Topic] = make([]*ring.Ring, 0, request.TopicPartitionCount)
		topicList = clusterMap.broker[request.Topic]
	}
	if request.TopicPartitionCount >= int32(len(topicList)) {
		// The partition count has increased. Append enough extra partitions, with offset rings, to our slice
		for i := int32(len(topicList)); i < request.TopicPartitionCount; i++ {
			topicList = append(topicList, ring.New(module.intervals))
		}
	}

	// Advance to the next ring entry (this means the pointer is always at the most recent entry, rather than the
	// oldest entry)
	topicList[request.Partition] = topicList[request.Partition].Next()
	partitionEntry := topicList[request.Partition]

	if partitionEntry.Value == nil {
		partitionEntry.Value = &brokerOffset{
			Offset:    request.Offset,
			Timestamp: request.Timestamp,
		}
	} else {
		ringval, _ := partitionEntry.Value.(*brokerOffset)
		ringval.Offset = request.Offset
		ringval.Timestamp = request.Timestamp
	}

	requestLogger.Debug("ok")
	clusterMap.broker[request.Topic] = topicList
}

func (module *InMemoryStorage) getBrokerOffset(clusterMap *clusterOffsets, topic string, partition int32, requestLogger *zap.Logger) (int64, int32) {
	clusterMap.brokerLock.RLock()
	defer clusterMap.brokerLock.RUnlock()

	topicPartitionList, ok := clusterMap.broker[topic]
	if !ok {
		// We don't know about this topic from the brokers yet - skip consumer offsets for now
		requestLogger.Debug("dropped", zap.String("reason", "no topic"))
		return 0, 0
	}
	if partition < 0 {
		// This should never happen, but if it does, log an warning with the offset information for review
		requestLogger.Warn("negative partition")
		return 0, 0
	}
	if partition >= int32(len(topicPartitionList)) {
		// We know about the topic, but partitions have been expanded and we haven't seen that from the broker yet
		requestLogger.Debug("dropped", zap.String("reason", "no broker partition"))
		return 0, 0
	}
	if topicPartitionList[partition].Value == nil {
		// We know about the topic and partition, but we haven't actually gotten the broker offset yet
		requestLogger.Debug("dropped", zap.String("reason", "no broker offset"))
		return 0, 0
	}
	return topicPartitionList[partition].Value.(*brokerOffset).Offset, int32(len(topicPartitionList))
}

func (module *InMemoryStorage) getConsumerPartition(consumerMap *consumerGroup, topic string, partition int32, partitionCount int32, requestLogger *zap.Logger) *consumerPartition {
	// Get or create the topic for the consumer
	consumerTopicMap, ok := consumerMap.topics[topic]
	if !ok {
		consumerMap.topics[topic] = make([]*consumerPartition, 0, partitionCount)
		consumerTopicMap = consumerMap.topics[topic]
	}

	// Get the partition specified
	if int(partition) >= len(consumerTopicMap) {
		// The partition count must have increased. Append enough extra partitions to our slice
		for i := int32(len(consumerTopicMap)); i < partitionCount; i++ {
			consumerTopicMap = append(consumerTopicMap, &consumerPartition{})
		}
		consumerMap.topics[topic] = consumerTopicMap
	}

	// Get or create the offsets ring for this partition
	if consumerTopicMap[partition].offsets == nil {
		consumerTopicMap[partition].offsets = ring.New(module.intervals)
	}

	return consumerTopicMap[partition]
}

func (module *InMemoryStorage) acceptConsumerGroup(group string) bool {
	if (module.groupWhitelist != nil) && (!module.groupWhitelist.MatchString(group)) {
		return false
	}
	if (module.groupBlacklist != nil) && module.groupBlacklist.MatchString(group) {
		return false
	}
	return true
}

func (module *InMemoryStorage) addConsumerOffset(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		// Ignore offsets for clusters that we don't know about - should never happen anyways
		requestLogger.Warn("unknown cluster")
		return
	}

	if request.Timestamp < ((time.Now().Unix() - module.expireGroup) * 1000) {
		requestLogger.Debug("dropped", zap.String("reason", "old offset"))
		return
	}

	if !module.acceptConsumerGroup(request.Group) {
		requestLogger.Debug("dropped", zap.String("reason", "group not whitelisted"))
		return
	}

	// Get the broker offset for this partition, as well as the partition count
	brokerOffset, partitionCount := module.getBrokerOffset(&clusterMap, request.Topic, request.Partition, requestLogger)
	if partitionCount == 0 {
		// If the returned partitionCount is zero, there was an error that was already logged. Just stop processing
		return
	}

	// Make the consumer group if it does not yet exist
	clusterMap.consumerLock.Lock()
	consumerMap, ok := clusterMap.consumer[request.Group]
	if !ok {
		clusterMap.consumer[request.Group] = &consumerGroup{
			lock:   &sync.RWMutex{},
			topics: make(map[string][]*consumerPartition),
		}
		consumerMap = clusterMap.consumer[request.Group]
	}
	clusterMap.consumerLock.Unlock()

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	defer consumerMap.lock.Unlock()

	// Get the offset ring for this partition - it always points to the earliest offset (or where to insert a new value)
	consumerPartition := module.getConsumerPartition(consumerMap, request.Topic, request.Partition, partitionCount, requestLogger)
	consumerPartitionRing := consumerPartition.offsets

	destination := findConsumerOffsetDestination(consumerPartitionRing, request, requestLogger)
	if destination == nil {
		return
	}

	var partitionLag *protocol.Lag
	if destination.isAppend() {
		// Calculate the lag against the brokerOffset
		partitionLag = &protocol.Lag{0}
		if brokerOffset > request.Offset {
			// Little bit of a hack - because we only get broker offsets periodically, it's possible the consumer offset could be ahead of where we think the broker
			// is. If the broker appears behind, just treat it as zero lag.
			partitionLag.Value = uint64(brokerOffset - request.Offset)
		}
		requestLogger.Debug("ok", zap.Uint64("lag", partitionLag.Value))
		consumerMap.lastCommit = request.Timestamp
	}

	destination = module.mergeFrequentCommitIntoPrevious(destination, request, requestLogger)
	module.storeConsumerOffset(consumerPartition, destination, request, partitionLag)
}

// Given a consumer offset ring and a storage request, find the destination
// based on the order of commits.
func findConsumerOffsetDestination(offsetRing *ring.Ring, request *protocol.StorageRequest, requestLogger *zap.Logger) *offsetRingDestination {
	destSlot := offsetRing
	prevSlot := offsetRing.Prev()

	if prevSlot.Value != nil {
		// nonempty ring, prevSlot is the latest offset
		if offsetRing.Value != nil {
			// full ring, offsetRing is the oldest
			if offsetRing.Value.(*protocol.ConsumerOffset).Order >= request.Order {
				requestLogger.Debug("dropping backfill commit, already full")
				return nil
			}
		}

		if prevSlot.Value.(*protocol.ConsumerOffset).Order >= request.Order {
			// the request is not the newest, so we need to insert it somewhere
			for {
				prevSlot = destSlot.Prev()
				if prevSlot.Value == nil || prevSlot == offsetRing {
					// Reached a blank slot or the oldest slot in the ring, just replace it
					return &offsetRingDestination{
						insertDest: prevSlot,
						extendDest: nil,
					}
				}

				// Lookback one previous commit
				prevOrder := prevSlot.Value.(*protocol.ConsumerOffset).Order
				if prevOrder < request.Order {
					// Insert here
					return &offsetRingDestination{
						insertDest: destSlot,
						extendDest: offsetRing,
					}
				} else if prevOrder == request.Order {
					return nil
				}

				// else Request is older than prevSlot; keep searching
				destSlot = prevSlot
			}
		}
	}

	// If we haven't returned by now we are just appending
	return &offsetRingDestination{
		insertDest: nil,
		extendDest: offsetRing,
	}
}

// If the offset commit is faster than we are allowing (less than the min-distance config), replace the previous
// commit with this one. This lets us store the new offset commit without dropping an old one
func (module *InMemoryStorage) mergeFrequentCommitIntoPrevious(destination *offsetRingDestination, request *protocol.StorageRequest, requestLogger *zap.Logger) *offsetRingDestination {
	prevSlot := destination.destinationSlot().Prev()
	if prevSlot.Value != nil {
		prevItem, _ := prevSlot.Value.(*protocol.ConsumerOffset)
		if prevItem.Order < request.Order && (request.Timestamp-prevItem.Timestamp) < (module.minDistance*1000) {
			// We also set the timestamp for the request to the previous timestamp. The reason for this is that if we
			// update the timestamp to the new timestamp, we may never create a new offset in the ring (consider the
			// case where someone is committing with a frequency lower than min-distance)
			request.Timestamp = prevItem.Timestamp
			return &offsetRingDestination{
				insertDest: prevSlot,
				extendDest: nil,
			}
		}
	}
	return destination
}

func (module *InMemoryStorage) storeConsumerOffset(consumerPartition *consumerPartition, destination *offsetRingDestination, request *protocol.StorageRequest, partitionLag *protocol.Lag) {
	if destination.isShift() {
		// Shift each item past destination.insertDest forward (so we end up occupying extendDest)
		copyTo := destination.extendDest
		var copyFrom *ring.Ring
		for copyTo != destination.insertDest {
			copyFrom = copyTo.Prev()
			copyTo.Value = copyFrom.Value
			copyFrom.Value = nil
			copyTo = copyFrom
		}
	}

	// Write into the destination
	destSlot := destination.destinationSlot()
	if destSlot.Value == nil {
		destSlot.Value = &protocol.ConsumerOffset{
			Offset:    request.Offset,
			Order:     request.Order,
			Timestamp: request.Timestamp,
			Lag:       partitionLag,
		}
	} else {
		ringval, _ := destSlot.Value.(*protocol.ConsumerOffset)
		ringval.Offset = request.Offset
		ringval.Order = request.Order
		ringval.Timestamp = request.Timestamp
		ringval.Lag = partitionLag
	}

	if destination.extendDest != nil {
		// We've extended the ring by either appending or shifting, update the ring pointer
		consumerPartition.offsets = destination.extendDest.Next()
	}
}

func (module *InMemoryStorage) addConsumerOwner(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		// Ignore offsets for clusters that we don't know about - should never happen anyways
		requestLogger.Warn("unknown cluster")
		return
	}

	if !module.acceptConsumerGroup(request.Group) {
		requestLogger.Debug("dropped", zap.String("reason", "group not whitelisted"))
		return
	}

	// Make the consumer group if it does not yet exist
	clusterMap.consumerLock.Lock()
	consumerMap, ok := clusterMap.consumer[request.Group]
	if !ok {
		clusterMap.consumer[request.Group] = &consumerGroup{
			lock:   &sync.RWMutex{},
			topics: make(map[string][]*consumerPartition),
		}
		consumerMap = clusterMap.consumer[request.Group]
	}
	clusterMap.consumerLock.Unlock()

	// Get the partition count for this partition (we don't need the actual broker offset)
	_, partitionCount := module.getBrokerOffset(&clusterMap, request.Topic, request.Partition, requestLogger)
	if partitionCount == 0 {
		// If the returned partitionCount is zero, there was an error that was already logged. Just stop processing
		return
	}

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	defer consumerMap.lock.Unlock()

	// Get the consumer partition state for this partition - we don't need it, but it will properly create the topic and partitions for us
	module.getConsumerPartition(consumerMap, request.Topic, request.Partition, partitionCount, requestLogger)

	if topic, ok := consumerMap.topics[request.Topic]; !ok || (int32(len(topic)) <= request.Partition) {
		requestLogger.Debug("dropped", zap.String("reason", "no partition"))
		return
	}

	// Write the owner for the given topic/partition
	requestLogger.Debug("ok")
	consumerMap.topics[request.Topic][request.Partition].owner = request.Owner
	consumerMap.topics[request.Topic][request.Partition].clientID = request.ClientID
}

func (module *InMemoryStorage) clearConsumerOwners(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		// Ignore metadata for clusters that we don't know about - should never happen anyways
		requestLogger.Warn("unknown cluster")
		return
	}

	if !module.acceptConsumerGroup(request.Group) {
		requestLogger.Debug("dropped", zap.String("reason", "group not whitelisted"))
		return
	}

	// Make the consumer group if it does not yet exist
	clusterMap.consumerLock.Lock()
	consumerMap, ok := clusterMap.consumer[request.Group]
	if !ok {
		// Consumer group doesn't exist, so we can't clear owners for it
		clusterMap.consumerLock.Unlock()
		return
	}
	clusterMap.consumerLock.Unlock()

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	defer consumerMap.lock.Unlock()

	for topic, partitions := range consumerMap.topics {
		for partitionID := range partitions {
			consumerMap.topics[topic][partitionID].owner = ""
			consumerMap.topics[topic][partitionID].clientID = ""
		}
	}

	requestLogger.Debug("ok")
}

func (module *InMemoryStorage) deleteTopic(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	// Work backwards - remove the topic from consumer groups first
	for _, consumerMap := range clusterMap.consumer {
		consumerMap.lock.Lock()
		// No need to check for existence
		delete(consumerMap.topics, request.Topic)
		consumerMap.lock.Unlock()
	}

	// Now remove the topic from the broker list
	clusterMap.brokerLock.Lock()
	delete(clusterMap.broker, request.Topic)
	clusterMap.brokerLock.Unlock()

	requestLogger.Debug("ok")
}

func (module *InMemoryStorage) deleteGroup(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.consumerLock.Lock()
	delete(clusterMap.consumer, request.Group)
	clusterMap.consumerLock.Unlock()

	requestLogger.Debug("ok")
}

func (module *InMemoryStorage) fetchClusterList(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	clusterList := make([]string, 0, len(module.offsets))
	for cluster := range module.offsets {
		clusterList = append(clusterList, cluster)
	}

	requestLogger.Debug("ok")
	request.Reply <- clusterList
}

func (module *InMemoryStorage) fetchTopicList(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.brokerLock.RLock()
	topicList := make([]string, 0, len(clusterMap.broker))
	for topic := range clusterMap.broker {
		topicList = append(topicList, topic)
	}
	clusterMap.brokerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- topicList
}

func (module *InMemoryStorage) fetchConsumerList(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.consumerLock.RLock()
	consumerList := make([]string, 0, len(clusterMap.consumer))
	for consumer := range clusterMap.consumer {
		consumerList = append(consumerList, consumer)
	}
	clusterMap.consumerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- consumerList
}

func (module *InMemoryStorage) fetchTopic(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.brokerLock.RLock()
	topicList, ok := clusterMap.broker[request.Topic]
	if !ok {
		requestLogger.Warn("unknown topic")
		clusterMap.brokerLock.RUnlock()
		return
	}

	offsetList := make([]int64, 0, len(topicList))
	for _, partition := range topicList {
		if partition.Value != nil {
			offsetList = append(offsetList, partition.Value.(*brokerOffset).Offset)
		}
	}
	clusterMap.brokerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- offsetList
}

func getConsumerTopicList(consumerMap *consumerGroup) protocol.ConsumerTopics {
	topicList := make(protocol.ConsumerTopics)
	consumerMap.lock.RLock()
	defer consumerMap.lock.RUnlock()

	for topic, partitions := range consumerMap.topics {
		topicList[topic] = make(protocol.ConsumerPartitions, len(partitions))

		for partitionID, partition := range partitions {
			consumerPartition := &protocol.ConsumerPartition{Owner: partition.owner, ClientID: partition.clientID}
			if partition.offsets != nil {
				offsetRing := partition.offsets
				consumerPartition.Offsets = make([]*protocol.ConsumerOffset, offsetRing.Len())

				ringPtr := offsetRing
				for i := 0; i < offsetRing.Len(); i++ {
					if ringPtr.Value == nil {
						consumerPartition.Offsets[i] = nil
					} else {
						ringval, _ := ringPtr.Value.(*protocol.ConsumerOffset)

						// Make a copy so that we can release the lock and be safe
						consumerPartition.Offsets[i] = &protocol.ConsumerOffset{
							Offset:    ringval.Offset,
							Order:     ringval.Order,
							Lag:       ringval.Lag,
							Timestamp: ringval.Timestamp,
						}
					}
					ringPtr = ringPtr.Next()
				}
			} else {
				consumerPartition.Offsets = make([]*protocol.ConsumerOffset, 0)
			}
			topicList[topic][partitionID] = consumerPartition
		}
	}
	return topicList
}

func (module *InMemoryStorage) fetchConsumer(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.consumerLock.RLock()
	consumerMap, ok := clusterMap.consumer[request.Group]
	if !ok {
		requestLogger.Warn("unknown consumer")
		clusterMap.consumerLock.RUnlock()
		return
	}

	// Lazily purge consumers that haven't committed in longer than the defined interval. Return as a 404
	if ((time.Now().Unix() - module.expireGroup) * 1000) > consumerMap.lastCommit {
		// Swap for a write lock
		clusterMap.consumerLock.RUnlock()

		clusterMap.consumerLock.Lock()
		requestLogger.Debug("purge expired consumer", zap.Int64("last_commit", consumerMap.lastCommit))
		delete(clusterMap.consumer, request.Group)
		clusterMap.consumerLock.Unlock()
		return
	}

	topicList := getConsumerTopicList(consumerMap)
	clusterMap.consumerLock.RUnlock()

	// Calculate the current lag for each now. We do this separate from getting the consumer info so we can avoid
	// locking both the consumers and the brokers at the same time
	clusterMap.brokerLock.RLock()
	for topic, partitions := range topicList {
		topicMap, ok := clusterMap.broker[topic]
		if !ok {
			// The topic may have just been deleted, so we'll skip this part and just return the consumer data we have
			continue
		}

		for p, partition := range partitions {
			// Build the slice of broker offsets to return
			partition.BrokerOffsets = make([]int64, 0, module.intervals)
			brokerOffsetPtr := topicMap[p].Next()
			brokerOffsetPtr.Do(func(item interface{}) {
				if item != nil {
					partition.BrokerOffsets = append(partition.BrokerOffsets, item.(*brokerOffset).Offset)
				}
			})

			if len(partition.Offsets) > 0 {
				brokerOffset := partition.BrokerOffsets[len(partition.BrokerOffsets)-1]
				lastOffset := partition.Offsets[len(partition.Offsets)-1]
				if lastOffset != nil {
					if brokerOffset < lastOffset.Offset {
						// Little bit of a hack - because we only get broker offsets periodically, it's possible the consumer offset could be ahead of where we think the broker
						// is. In this case, just mark it as zero lag.
						partition.CurrentLag = 0
					} else {
						partition.CurrentLag = uint64(brokerOffset - lastOffset.Offset)
					}
				}
			}
		}
	}
	clusterMap.brokerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- topicList
}

func (module *InMemoryStorage) fetchConsumersForTopicList(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		requestLogger.Warn("unknown cluster")
		return
	}

	clusterMap.consumerLock.RLock()

	consumerListForTopic := make([]string, 0)
	for consumerGroup := range clusterMap.consumer {
		consumerMap := clusterMap.consumer[consumerGroup]
		topicList := getConsumerTopicList(consumerMap)
		for topic := range topicList {
			if topic == request.Topic {
				consumerListForTopic = append(consumerListForTopic, consumerGroup)
				break
			}
		}
	}

	clusterMap.consumerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- consumerListForTopic
}
