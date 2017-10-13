package protocol

type PartitionOffset struct {
	Cluster             string
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	Group               string
}

type StorageRequestConstant int
const (
	StorageSetBrokerOffset   StorageRequestConstant = 0
	StorageSetConsumerOffset StorageRequestConstant = 1
	StorageSetDeleteTopic    StorageRequestConstant = 2
	StorageFetchClusters     StorageRequestConstant = 3
	StorageFetchConsumers    StorageRequestConstant = 4
	StorageFetchTopics       StorageRequestConstant = 5
	StorageFetchConsumer     StorageRequestConstant = 6
	StorageFetchTopic        StorageRequestConstant = 7
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
}

type ConsumerPartition struct {
	Offsets    []*ConsumerOffset
	CurrentLag int64
}

type ConsumerOffset struct {
	Offset     int64 `json:"offset"`
	Timestamp  int64 `json:"timestamp"`
	Lag        int64 `json:"lag"`
}

type ConsumerTopics map[string]ConsumerPartitions
type ConsumerPartitions []*ConsumerPartition