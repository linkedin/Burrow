package evaluator

import (
	"sync"

	"go.uber.org/zap"
	"github.com/karrick/goswarm"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
	"time"
	"strings"
)

type CachingEvaluator struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	name            string
	myConfiguration *configuration.EvaluatorConfig
	RequestChannel  chan *protocol.EvaluatorRequest
	running         sync.WaitGroup
	cache           *goswarm.Simple
}

type CacheError struct {
	StatusCode int
	Reason     string
}

func (e *CacheError) Error() string {
	return e.Reason
}

func (module *CachingEvaluator) Configure(name string) {
	module.name = name
	module.RequestChannel = make(chan *protocol.EvaluatorRequest)
	module.myConfiguration = module.App.Configuration.Evaluator[name]
	module.running = sync.WaitGroup{}

	// Set defaults for configs if needed
	if module.App.Configuration.Evaluator[module.name].ExpireCache == 0 {
		module.App.Configuration.Evaluator[module.name].ExpireCache = 10
	}
	cacheExpire := time.Duration(module.App.Configuration.Evaluator[module.name].ExpireCache) * time.Second

	newCache, err := goswarm.NewSimple(&goswarm.Config{
		GoodExpiryDuration: cacheExpire,
		BadExpiryDuration:  cacheExpire,
		Lookup:             module.evaluateConsumerStatus,
	})
	if err != nil {
		module.Log.Panic("Failed to start cache")
		panic(err)
	}
	module.cache = newCache
}

func (module *CachingEvaluator) GetCommunicationChannel() chan *protocol.EvaluatorRequest {
	return module.RequestChannel
}

func (module *CachingEvaluator) Start() error {
	go module.mainLoop()
	return nil
}

func (module *CachingEvaluator) Stop() error {
	close(module.RequestChannel)
	module.running.Wait()
	return nil
}

func (module *CachingEvaluator) mainLoop() {
	module.running.Add(1)
	defer module.running.Done()

	for {
		select {
		case request, isOpen := <-module.RequestChannel:
			if ! isOpen {
				return
			}

			if request != nil {
				go module.getConsumerStatus(request)
			}
		}
	}
}

func (module *CachingEvaluator) getConsumerStatus(request *protocol.EvaluatorRequest) {
	// Easier to set up the structured logger once for the request
	requestLogger := module.Log.With(
		zap.String("cluster", request.Cluster),
		zap.String("consumer", request.Group),
		zap.Bool("showall", request.ShowAll),
	)

	result, err := module.cache.Query(request.Cluster + " " + request.Group)
	if err != nil {
		requestLogger.Info(err.Error())

		// We're just returning all errors as a 404 here
		request.Reply <- &protocol.ConsumerGroupStatus{
			Cluster:    request.Cluster,
			Group:      request.Group,
			Status:     protocol.StatusNotFound,
			Complete:   true,
			Partitions: make([]*protocol.PartitionStatus, 0),
			Maxlag:     nil,
			TotalLag:   0,
		}
	} else {
		status := result.(*protocol.ConsumerGroupStatus)

		if ! request.ShowAll {
			// The requestor only wants partitions that are not StatusOK, so we need to filter the result before
			// returning it. However, we can't modify the original, so we need to make a new copy
			cachedStatus := status
			status = &protocol.ConsumerGroupStatus{
				Cluster:         cachedStatus.Cluster,
				Group:           cachedStatus.Group,
				Status:          cachedStatus.Status,
				Complete:        cachedStatus.Complete,
				Maxlag:          cachedStatus.Maxlag,
				TotalLag:        cachedStatus.TotalLag,
				TotalPartitions: cachedStatus.TotalPartitions,
				Partitions:      make([]*protocol.PartitionStatus, cachedStatus.TotalPartitions),
			}

			// Copy over any partitions that do not have the status StatusOK
			count := 0
			for _, partition := range cachedStatus.Partitions {
				if partition.Status > protocol.StatusOK {
					status.Partitions[count] = partition
					count++
				}
			}
			status.Partitions = status.Partitions[0:count]
		}

		requestLogger.Debug("ok")
		request.Reply <- status
	}
	close(request.Reply)
}

func (module *CachingEvaluator) evaluateConsumerStatus(clusterAndConsumer string) (interface{}, error) {
	// First off, we need to separate the cluster and consumer values from the string provided
	parts := strings.Split(clusterAndConsumer, " ")
	if len(parts) != 2 {
		module.Log.Error("query with bad clusterAndConsumer", zap.String("arg", clusterAndConsumer))
		return nil, &CacheError{StatusCode: 500, Reason: "bad request"}
	}
	cluster := parts[0]
	consumer := parts[1]

	// Fetch all the consumer offset and lag information from storage
	storageRequest := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumer,
		Cluster:     cluster,
		Group:       consumer,
		Reply:       make(chan interface{}),
	}
	module.App.StorageChannel <- storageRequest
	response := <- storageRequest.Reply

	if response == nil {
		// Either the cluster or the consumer doesn't exist. In either case, return an error
		module.Log.Debug("evaluation result",
			zap.String("cluster", cluster),
			zap.String("consumer", consumer),
			zap.String("status", protocol.StatusNotFound.String()),
		)
		return nil, &CacheError{StatusCode: 404, Reason: "cluster or consumer not found"}
	}

	// From here out, we're going to return a non-error response, so prepare a status struct
	status := &protocol.ConsumerGroupStatus{
		Cluster:         cluster,
		Group:           consumer,
		Status:          protocol.StatusOK,
		Complete:        true,
		Maxlag:          nil,
		TotalLag:        0,
		TotalPartitions: 0,
	}

	// Count up the number of partitions for this consumer first, so we can size our slice correctly
	topics := response.(protocol.ConsumerTopics)
	for _, partitions := range topics {
		for _, partition := range partitions {
			status.TotalPartitions += 1
			status.TotalLag += uint64(partition.CurrentLag)
		}
	}
	status.Partitions = make([]*protocol.PartitionStatus, status.TotalPartitions)

	count := 0
	for topic, partitions := range topics {
		for partition_id, partition := range partitions {
			partitionStatus := evaluatePartitionStatus(partition)
			partitionStatus.Topic = topic
			partitionStatus.Partition = int32(partition_id)

			if partitionStatus.Status > status.Status {
				// If the partition status is greater than StatusError, we just mark it as StatusError
				if partitionStatus.Status > protocol.StatusError {
					status.Status = protocol.StatusError
				} else {
					status.Status = partitionStatus.Status
				}
			}

			if (status.Maxlag == nil) || (partitionStatus.CurrentLag > status.Maxlag.CurrentLag) {
				status.Maxlag = partitionStatus
			}
			status.Complete = status.Complete && partitionStatus.Complete
			status.Partitions[count] = partitionStatus
			count++
		}
	}

	module.Log.Debug("evaluation result",
		zap.String("cluster", cluster),
		zap.String("consumer", consumer),
		zap.String("status", status.Status.String()),
		zap.Bool("complete", status.Complete),
		zap.Uint64("total_lag", status.TotalLag),
		zap.Int("total_partitions", status.TotalPartitions),
	)
	return status, nil
}

func evaluatePartitionStatus(partition *protocol.ConsumerPartition) *protocol.PartitionStatus {
	status := &protocol.PartitionStatus{
		Status:     protocol.StatusOK,
		Complete:   true,
		CurrentLag: partition.CurrentLag,
	}

	// Slice the offsets to remove all nil entries
	numOffsets := len(partition.Offsets)
	for i, offset := range partition.Offsets {
		if offset == nil {
			numOffsets = i
			break
		}
	}
	offsets := partition.Offsets[:numOffsets]

	// Check if we had any nil offsets, and mark the partition as incomplete
	if len(offsets) < len(partition.Offsets) {
		status.Complete = false
	}

	// If there are no offsets left, just return an OK result as is - we can't determine anything more
	if len(offsets) == 0 {
		return status
	}
	status.Start = offsets[0]
	status.End = offsets[len(offsets) - 1]

	status.Status = calculatePartitionStatus(offsets, partition.CurrentLag, time.Now().Unix())
	return status
}

func calculatePartitionStatus(offsets []*protocol.ConsumerOffset, currentLag int64, timeNow int64) protocol.StatusConstant {
	// First check if the lag was zero at any point, and skip the rest of the checks if this is true
	if (currentLag > 0) && isLagAlwaysNotZero(offsets) {
		// Check for errors, in order of severity starting with the worst. If any check comes back true, skip the rest
		if checkIfOffsetsRewind(offsets) {
			return protocol.StatusRewind
		}
		if checkIfOffsetsStopped(offsets, timeNow) {
			return protocol.StatusStop
		}
		if checkIfOffsetsStalled(offsets) {
			return protocol.StatusStall
		}
		if checkIfLagNotDecreasing(offsets) {
			return protocol.StatusWarning
		}
	}
	return protocol.StatusOK
}

// Rule 1 - If over the stored period, the lag is ever zero for the partition, the period is OK
func isLagAlwaysNotZero(offsets []*protocol.ConsumerOffset) bool {
	for _, offset := range offsets {
		if offset.Lag == 0 {
			return false
		}
	}
	return true
}

// Rule 2 - If the consumer offset decreases from one interval to the next the partition is marked as a rewind (error)
func checkIfOffsetsRewind(offsets []*protocol.ConsumerOffset) bool {
	for i := 1; i < len(offsets); i++ {
		if offsets[i].Offset < offsets[i-1].Offset {
			return true
		}
	}
	return false
}

// Rule 3 - If the difference between now and the last offset timestamp is greater than the difference between the last
//          and first offset timestamps, the consumer has stopped committing offsets for that partition (error)
func checkIfOffsetsStopped(offsets []*protocol.ConsumerOffset, timeNow int64) bool {
	firstTimestamp := offsets[0].Timestamp
	lastTimestamp := offsets[len(offsets) - 1].Timestamp
	if ((timeNow * 1000) - lastTimestamp) > (lastTimestamp - firstTimestamp) {
		return true
	}
	return false
}

// Rule 4 - If the consumer is committing offsets that do not change, it's an error (partition is stalled)
//          NOTE - we already checked for zero lag in Rule 1, so we know that there is currently lag for this partition
func checkIfOffsetsStalled(offsets []*protocol.ConsumerOffset) bool {
	for i := 1; i < len(offsets); i++ {
		if offsets[i].Offset != offsets[i-1].Offset {
			return false
		}
	}
	return true
}

// Rule 5 - If the consumer offsets are advancing, but the lag is not decreasing somewhere, it's a warning (consumer is slow)
func checkIfLagNotDecreasing(offsets []*protocol.ConsumerOffset) bool {
	for i := 1; i < len(offsets); i++ {
		if offsets[i].Lag < offsets[i-1].Lag {
			return false
		}
	}
	return true
}
