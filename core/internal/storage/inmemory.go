package storage

import (
	"sync"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"container/ring"
	"regexp"
)

type InMemoryStorage struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.StorageConfig
	RequestChannel  chan *protocol.StorageRequest
	running         sync.WaitGroup

	offsets         map[string]ClusterOffsets
	groupWhitelist  *regexp.Regexp
}

type BrokerOffset struct {
	Offset    int64
	Timestamp int64
}

type ConsumerGroup struct {
	// This lock is held when using the individual group, either for read or write
	lock   *sync.RWMutex
	topics map[string][]*ring.Ring
}

type ClusterOffsets struct {
	broker       map[string][]*BrokerOffset
	consumer     map[string]*ConsumerGroup

	// This lock is used when modifying broker topics or offsets
	brokerLock   *sync.RWMutex

	// This lock is used when modifying the overall consumer list
	// It does not need to be held for modifying an individual group
	consumerLock *sync.RWMutex
}

func (module *InMemoryStorage) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Storage[name]
	module.RequestChannel = make(chan *protocol.StorageRequest)
	module.running = sync.WaitGroup{}
	module.offsets = make(map[string]ClusterOffsets)

	// Set defaults for configs if needed
	if module.App.Configuration.Storage[module.name].Intervals == 0 {
		module.App.Configuration.Storage[module.name].Intervals = 10
	}

	if module.App.Configuration.Storage[module.name].GroupWhitelist != "" {
		re, err := regexp.Compile(module.App.Configuration.Storage[module.name].GroupWhitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}
}

func (module *InMemoryStorage) GetCommunicationChannel() chan *protocol.StorageRequest {
	return module.RequestChannel
}

func (module *InMemoryStorage) Start() error {
	module.running.Add(1)

	for cluster := range module.App.Configuration.Cluster {
		module.
		offsets[cluster] = ClusterOffsets{
			broker:       make(map[string][]*BrokerOffset),
			consumer:     make(map[string]*ConsumerGroup),
			brokerLock:   &sync.RWMutex{},
			consumerLock: &sync.RWMutex{},
		}
	}

	return nil
}

func (module *InMemoryStorage) Stop() error {
	close(module.RequestChannel)
	module.running.Done()
	return nil
}

func (module *InMemoryStorage) mainLoop() {
	select {
	case r := <-module.RequestChannel:
		if r == nil {
			return
		}

		// Easier to set up the structured logger once for the request
		requestLogger := module.Log.With(
			zap.String("cluster", r.Cluster),
			zap.String("consumer", r.Group),
			zap.String("topic", r.Topic),
			zap.Int32("partition", r.Partition),
			zap.Int32("topic_partition_count", r.TopicPartitionCount),
			zap.Int64("offset", r.Offset),
			zap.Int64("timestamp", r.Timestamp),
		)

		switch r.RequestType {
		case protocol.StorageSetBrokerOffset:
			go module.addBrokerOffset(r, requestLogger.With(zap.String("request", "StorageSetBrokerOffset")))
		case protocol.StorageSetConsumerOffset:
			go module.addConsumerOffset(r, requestLogger.With(zap.String("request", "StorageSetConsumerOffset")))
		case protocol.StorageSetDeleteTopic:
			go module.deleteTopic(r, requestLogger.With(zap.String("request", "StorageSetDeleteTopic")))
		case protocol.StorageFetchClusters:
			go module.fetchClusterList(r, requestLogger.With(zap.String("request", "StorageFetchClusters")))
		case protocol.StorageFetchConsumers:
			go module.fetchConsumerList(r, requestLogger.With(zap.String("request", "StorageFetchConsumers")))
		case protocol.StorageFetchTopics:
			go module.fetchTopicList(r, requestLogger.With(zap.String("request", "StorageFetchTopics")))
		case protocol.StorageFetchConsumer:
			go module.fetchConsumer(r, requestLogger.With(zap.String("request", "StorageFetchConsumer")))
		case protocol.StorageFetchTopic:
			go module.fetchTopic(r, requestLogger.With(zap.String("request", "StorageFetchTopic")))
		default:
			requestLogger.Error("unknown storage request type",
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
		clusterMap.broker[request.Topic] = make([]*BrokerOffset, request.TopicPartitionCount)
		topicList = clusterMap.broker[request.Topic]
	}
	if request.TopicPartitionCount >= int32(len(topicList)) {
		// The partition count has increased. Append enough extra partitions to our slice
		for i := int32(len(topicList)); i < request.TopicPartitionCount; i++ {
			topicList = append(topicList, nil)
		}
	}

	partitionEntry := topicList[request.Partition]
	if partitionEntry == nil {
		topicList[request.Partition] = &BrokerOffset{
			Offset:    request.Offset,
			Timestamp: request.Timestamp,
		}
		partitionEntry = topicList[request.Partition]
	} else {
		partitionEntry.Offset = request.Offset
		partitionEntry.Timestamp = request.Timestamp
	}

	requestLogger.Debug("ok")
	clusterMap.broker[request.Topic] = topicList
}

func (module *InMemoryStorage) getBrokerOffset(clusterMap *ClusterOffsets, topic string, partition int32, requestLogger *zap.Logger) (int64, int32) {
	clusterMap.brokerLock.RLock()
	defer clusterMap.brokerLock.RUnlock()

	topicPartitionList, ok := clusterMap.broker[topic]
	if !ok {
		// We don't know about this topic from the brokers yet - skip consumer offsets for now
		requestLogger.Debug("dropped offset", zap.String("reason", "no topic"))
		return 0, 0
	}
	if partition < 0 {
		// This should never happen, but if it does, log an warning with the offset information for review
		requestLogger.Warn("negative partition")
		return 0, 0
	}
	if partition >= int32(len(topicPartitionList)) {
		// We know about the topic, but partitions have been expanded and we haven't seen that from the broker yet
		requestLogger.Debug("dropped offset", zap.String("reason", "no broker partition"))
		return 0, 0
	}
	if topicPartitionList[partition] == nil {
		// We know about the topic and partition, but we haven't actually gotten the broker offset yet
		requestLogger.Debug("dropped offset", zap.String("reason", "no broker offset"))
		return 0, 0
	}
	return topicPartitionList[partition].Offset, int32(len(topicPartitionList))
}

func (module *InMemoryStorage) getPartitionRing(consumerMap *ConsumerGroup, topic string, partition int32, partitionCount int32, requestLogger *zap.Logger) *ring.Ring {
	// Get or create the topic for the consumer
	consumerTopicMap, ok := consumerMap.topics[topic]
	if !ok {
		consumerMap.topics[topic] = make([]*ring.Ring, partitionCount)
		consumerTopicMap = consumerMap.topics[topic]
	}

	// Get the partition specified
	if int(partition) >= len(consumerTopicMap) {
		// The partition count must have increased. Append enough extra partitions to our slice
		for i := int32(len(consumerTopicMap)); i < partitionCount; i++ {
			consumerTopicMap = append(consumerTopicMap, nil)
		}
	}

	// Get or create the offsets ring for this partition
	consumerPartitionRing := consumerTopicMap[partition]
	if consumerPartitionRing == nil {
		consumerTopicMap[partition] = ring.New(module.myConfiguration.Intervals)
		consumerPartitionRing = consumerTopicMap[partition]
	}

	return consumerPartitionRing
}

func (module *InMemoryStorage) addConsumerOffset(request *protocol.StorageRequest, requestLogger *zap.Logger) {
	clusterMap, ok := module.offsets[request.Cluster]
	if !ok {
		// Ignore offsets for clusters that we don't know about - should never happen anyways
		requestLogger.Warn("unknown cluster")
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
		clusterMap.consumer[request.Group] = &ConsumerGroup{
			lock: &sync.RWMutex{},
			topics: make(map[string][]*ring.Ring),
		}
		consumerMap = clusterMap.consumer[request.Group]
	}
	clusterMap.consumerLock.Unlock()

	// For the rest of this, we need the write lock for the consumer group
	consumerMap.lock.Lock()
	defer consumerMap.lock.Unlock()

	// Get the offset ring for this partition - it always points to the earliest offset (or where to insert a new value)
	consumerPartitionRing := module.getPartitionRing(consumerMap, request.Topic, request.Partition, partitionCount, requestLogger)

	if consumerPartitionRing.Prev().Value != nil {
		// If the offset commit is faster than we are allowing (less than the min-distance config), rewind the ring by one spot
		// This lets us store the offset commit without dropping an old one
		if (request.Timestamp - consumerPartitionRing.Prev().Value.(*protocol.ConsumerOffset).Timestamp) < (module.myConfiguration.MinDistance * 1000) {
			// We have to change both pointers here, as we're essentially rewinding the ring one spot to add this commit
			consumerPartitionRing = consumerPartitionRing.Prev()
			consumerMap.topics[request.Topic][request.Partition] = consumerPartitionRing
		}
	}
	// Calculate the lag against the brokerOffset
	partitionLag := brokerOffset - request.Offset
	if partitionLag < 0 {
		// Little bit of a hack - because we only get broker offsets periodically, it's possible the consumer offset could be ahead of where we think the broker
		// is. In this case, just mark it as zero lag.
		partitionLag = 0
	}

	// Update or create the ring value at the current pointer
	if consumerPartitionRing.Value == nil {
		consumerPartitionRing.Value = &protocol.ConsumerOffset{
			Offset:     request.Offset,
			Timestamp:  request.Timestamp,
			Lag:        partitionLag,
		}
	} else {
		ringval, _ := consumerPartitionRing.Value.(*protocol.ConsumerOffset)
		ringval.Offset = request.Offset
		ringval.Timestamp = request.Timestamp
		ringval.Lag = partitionLag
	}

	// Advance the ring pointer
	requestLogger.Debug("ok", zap.Int64("lag", partitionLag))
	consumerMap.topics[request.Topic][request.Partition] = consumerMap.topics[request.Topic][request.Partition].Next()
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
		offsetList = append(offsetList, partition.Offset)
	}
	clusterMap.brokerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- offsetList
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

	topicList := make(map[string][]*protocol.ConsumerPartition)
	for topic, partitions := range consumerMap.topics {
		topicList[topic] = make([]*protocol.ConsumerPartition, 0, len(partitions))

		for _, offsetRing := range partitions {
			partition := &protocol.ConsumerPartition{
				Offsets: make([]*protocol.ConsumerOffset, 0, offsetRing.Len()),
			}

			ringPtr := offsetRing
			for i := 0; i < offsetRing.Len(); i++ {
				if ringPtr.Value == nil {
					partition.Offsets = append(partition.Offsets, nil)
				} else {
					ringval, _ := ringPtr.Value.(*protocol.ConsumerOffset)

					// Make a copy so that we can release the lock and be safe
					partition.Offsets = append(partition.Offsets, &protocol.ConsumerOffset{
						Offset:    ringval.Offset,
						Lag:       ringval.Lag,
						Timestamp: ringval.Timestamp,
					})
				}
				ringPtr = ringPtr.Next()
			}
			topicList[topic] = append(topicList[topic], partition)
		}
	}
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
			partition.CurrentLag = topicMap[p].Offset - partition.Offsets[len(partition.Offsets) - 1].Offset
		}
	}
	clusterMap.brokerLock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- topicList
}
