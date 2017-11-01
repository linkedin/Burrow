package zookeeper

import (
	"sync"
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
	zkConn, connEventChan, err := zc.connectFunc(zc.App.Configuration.Zookeeper.Hosts, time.Duration(zc.App.Configuration.Zookeeper.Timeout)*time.Second, zc.Log)
	if err != nil {
		zc.Log.Panic("Failure to start module", zap.String("error", err.Error()))
	}

	zc.App.Zookeeper = zkConn
	zc.App.ZookeeperRoot = zc.App.Configuration.Zookeeper.RootPath
	zc.App.ZookeeperConnected = true
	zc.App.ZookeeperExpired = &sync.Cond{L: &sync.Mutex{}}

	zc.mainLoop(connEventChan)

	return nil
}

func (zc *Coordinator) Stop() error {
	zc.Log.Info("stopping")

	// This will close the event channel, closing the mainLoop
	zc.App.Zookeeper.Close()

	return nil
}

func (zc *Coordinator) mainLoop(eventChan <-chan zk.Event) {
	for {
		select {
		case event, isOpen := <- eventChan:
			if ! isOpen {
				// All done here
				return
			}
			if event.Type == zk.EventSession {
				switch event.State {
				case zk.StateExpired:
					zc.Log.Error("session expired")
					zc.App.ZookeeperConnected = false
					zc.App.ZookeeperExpired.Broadcast()
				case zk.StateConnected:
					if ! zc.App.ZookeeperConnected {
						zc.Log.Info("starting session")
						zc.App.ZookeeperConnected = true
					}
				}
			}
		}
	}
}
