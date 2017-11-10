/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package zookeeper

import (
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type Coordinator struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	servers     []string
	connectFunc func([]string, time.Duration, *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error)
	running     sync.WaitGroup
}

func (zc *Coordinator) Configure() {
	zc.Log.Info("configuring")

	if zc.connectFunc == nil {
		zc.connectFunc = helpers.ZookeeperConnect
	}

	// Set and check configs
	viper.SetDefault("zookeeper.timeout", 6)
	viper.SetDefault("zookeeper.root-path", "/burrow")

	zc.servers = viper.GetStringSlice("zookeeper.servers")
	if len(zc.servers) == 0 {
		panic("No Zookeeper servers specified")
	} else if !helpers.ValidateHostList(zc.servers) {
		panic("Failed to validate Zookeeper servers")
	}

	zc.App.ZookeeperRoot = viper.GetString("zookeeper.root-path")
	if !helpers.ValidateZookeeperPath(zc.App.ZookeeperRoot) {
		panic("Zookeeper root path is not valid")
	}

	zc.running = sync.WaitGroup{}
}

func (zc *Coordinator) Start() error {
	zc.Log.Info("starting")

	// This ZK client will be shared by other parts of Burrow for things like locks
	// NOTE - samuel/go-zookeeper does not support chroot, so we pass along the configured root path in config
	zkConn, connEventChan, err := zc.connectFunc(zc.servers, viper.GetDuration("zookeeper.timeout")*time.Second, zc.Log)
	if err != nil {
		zc.Log.Panic("Failure to start zookeeper", zap.String("error", err.Error()))
		return err
	}
	zc.App.Zookeeper = zkConn

	// Assure that our root path exists
	err = zc.createRecursive(zc.App.ZookeeperRoot)
	if err != nil {
		zc.Log.Error("cannot create root path", zap.Error(err))
		return err
	}

	zc.App.ZookeeperConnected = true
	zc.App.ZookeeperExpired = &sync.Cond{L: &sync.Mutex{}}

	go zc.mainLoop(connEventChan)

	return nil
}

func (zc *Coordinator) Stop() error {
	zc.Log.Info("stopping")

	// This will close the event channel, closing the mainLoop
	zc.App.Zookeeper.Close()
	zc.running.Wait()

	return nil
}

func (zc *Coordinator) createRecursive(path string) error {
	if path == "/" {
		return nil
	}

	parts := strings.Split(path, "/")
	for i := 2; i <= len(parts); i++ {
		_, err := zc.App.Zookeeper.Create(strings.Join(parts[:i], "/"), []byte{}, 0, zk.WorldACL(zk.PermAll))
		// Ignore when the node exists already
		if (err != nil) && (err != zk.ErrNodeExists) {
			return err
		}
	}
	return nil
}

func (zc *Coordinator) mainLoop(eventChan <-chan zk.Event) {
	zc.running.Add(1)
	defer zc.running.Done()

	for {
		select {
		case event, isOpen := <-eventChan:
			if !isOpen {
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
					if !zc.App.ZookeeperConnected {
						zc.Log.Info("starting session")
						zc.App.ZookeeperConnected = true
					}
				}
			}
		}
	}
}
