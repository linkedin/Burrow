package zookeeper

import (
	"time"

	"go.uber.org/zap"
	"github.com/samuel/go-zookeeper/zk"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/internal/helpers"
)

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger

	connectFunc func([]string, time.Duration, *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error)
}

func (zc *Coordinator) Configure() {
	zc.Log.Info("configuring")

	zc.connectFunc = helpers.ZookeeperConnect
}

func (zc *Coordinator) Start() error {
	zc.Log.Info("starting")

	// This ZK client will be shared by other parts of Burrow for things like locks
	// NOTE - samuel/go-zookeeper does not support chroot, so we pass along the configured root path in config
	zkConn, _, err := zc.connectFunc(zc.App.Configuration.Zookeeper.Hosts, time.Duration(zc.App.Configuration.Zookeeper.Timeout)*time.Second, zc.Log)
	if err != nil {
		zc.Log.Panic("Failure to start module", zap.String("error", err.Error()))
	}

	zc.App.Zookeeper = zkConn
	zc.App.ZookeeperRoot = zc.App.Configuration.Zookeeper.RootPath

	return nil
}

func (zc *Coordinator) Stop() error {
	zc.Log.Info("stopping")

	zc.App.Zookeeper.Close()
	return nil
}
