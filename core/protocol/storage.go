package protocol

type StorageRequestConstant int
const (
	StorageSetBrokerOffset     StorageRequestConstant = 0
	StorageSetConsumerOffset   StorageRequestConstant = 1
	StorageSetConsumerOwner    StorageRequestConstant = 2
	StorageSetDeleteTopic      StorageRequestConstant = 3
	StorageSetDeleteGroup      StorageRequestConstant = 4
	StorageFetchClusters       StorageRequestConstant = 5
	StorageFetchConsumers      StorageRequestConstant = 6
	StorageFetchTopics         StorageRequestConstant = 7
	StorageFetchConsumer       StorageRequestConstant = 8
	StorageFetchTopic          StorageRequestConstant = 9
)

type StorageRequest struct {
	RequestType         StorageRequestConstant
	Reply               chan interface{}
	Cluster             string
	Group               string
	Topic               string
	Partition           int32
	TopicPartitionCount int32
	Offset              int64
	Timestamp           int64
	Owner               string
}

type ConsumerPartition struct {
	Offsets    []*ConsumerOffset `json:"offsets"`
	Owner      string            `json:"owner"`
	CurrentLag int64             `json:"current-lag"`
}

type ConsumerOffset struct {
	Offset     int64 `json:"offset"`
	Timestamp  int64 `json:"timestamp"`
	Lag        int64 `json:"lag"`
}

type ConsumerTopics map[string]ConsumerPartitions
type ConsumerPartitions []*ConsumerPartition