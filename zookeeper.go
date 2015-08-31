/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZookeeperClient struct {
	app     *ApplicationContext
	cluster string
	conn    *zk.Conn
}

func NewZookeeperClient(app *ApplicationContext, cluster string) (*ZookeeperClient, error) {
	zkconn, _, err := zk.Connect(app.Config.Kafka[cluster].Zookeepers, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &ZookeeperClient{
		app:     app,
		cluster: cluster,
		conn:    zkconn,
	}

	return client, nil
}

func (zkClient *ZookeeperClient) Stop() {
	zkClient.conn.Close()
}

func (zkClient *ZookeeperClient) NewLock(path string) *zk.Lock {
	// Pass through to the connection NewLock, without ACLs
	return zk.NewLock(zkClient.conn, path, make([]zk.ACL, 0))
}

func (zkClient *ZookeeperClient) RecursiveDelete(path string) {
	if path == "/" {
		panic("Do not try and delete the root znode")
	}

	children, _, err := zkClient.conn.Children(path)
	switch {
	case err == nil:
		for _, child := range children {
			zkClient.RecursiveDelete(path + "/" + child)
		}
	case err == zk.ErrNoNode:
		return
	default:
		panic(err)
	}

	_, stat, err := zkClient.conn.Get(path)
	if (err != nil) && (err != zk.ErrNoNode) {
		panic(err)
	}
	err = zkClient.conn.Delete(path, stat.Version)
	if (err != nil) && (err != zk.ErrNoNode) {
		panic(err)
	}
}
