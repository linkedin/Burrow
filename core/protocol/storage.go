package protocol

type PartitionOffset struct {
	Cluster             string
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	Group               string
}

type StorageFetchConstant int

const (
	StorageFetchClusters  StorageFetchConstant = 0
	StorageFetchConsumers StorageFetchConstant = 1
	StorageFetchTopics    StorageFetchConstant = 2
	StorageFetchConsumer  StorageFetchConstant = 3
	StorageFetchTopic     StorageFetchConstant = 4
)

type StorageFetchRequest struct {
	RequestType			int
	Cluster             string
	Topic               string
	Group               string
}
