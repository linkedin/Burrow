package protocol

type EvaluatorRequest struct {
	Cluster         string
	Group           string
	ShowAll         bool
}

type PartitionStatus struct {
	Topic     string         `json:"topic"`
	Partition int32          `json:"partition"`
	Status    StatusConstant `json:"status"`
	Start     ConsumerOffset `json:"start"`
	End       ConsumerOffset `json:"end"`
}

type ConsumerGroupStatus struct {
	Cluster         string             `json:"cluster"`
	Group           string             `json:"group"`
	Status          StatusConstant     `json:"status"`
	Complete        bool               `json:"complete"`
	Partitions      []*PartitionStatus `json:"partitions"`
	TotalPartitions int                `json:"partition_count"`
	Maxlag          *PartitionStatus   `json:"maxlag"`
	TotalLag        uint64             `json:"totallag"`
}

var StatusStrings = [...]string{"NOTFOUND", "OK", "WARN", "ERR", "STOP", "STALL", "REWIND"}

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

