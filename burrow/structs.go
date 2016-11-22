package burrow

import "github.com/samuel/go-zookeeper/zk"

type KafkaCluster struct {
	Client    *KafkaClient
	Zookeeper *ZookeeperClient
}

type StormCluster struct {
	Storm *StormClient
}

type ApplicationContext struct {
	Config       *BurrowConfig
	Storage      *OffsetStorage
	Clusters     map[string]*KafkaCluster
	Storms       map[string]*StormCluster
	Server       *HttpServer
	NotifyCenter *NotifyCenter
	NotifierLock *zk.Lock
}
