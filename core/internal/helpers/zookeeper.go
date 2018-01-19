/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package helpers

import (
	"time"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// BurrowZookeeperClient is an implementation of protocol.ZookeeperClient
type BurrowZookeeperClient struct {
	client *zk.Conn
}

// ZookeeperConnect establishes a new connection to a pool of Zookeeper servers. The provided session timeout sets the
// amount of time for which a session is considered valid after losing connection to a server. Within the session
// timeout it's possible to reestablish a connection to a different server and keep the same session. This is means any
// ephemeral nodes and watches are maintained.
func ZookeeperConnect(servers []string, sessionTimeout time.Duration, logger *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error) {
	// We need a function to set the logger for the ZK connection
	zkSetLogger := func(c *zk.Conn) {
		c.SetLogger(zap.NewStdLog(logger))
	}

	zkconn, connEventChan, err := zk.Connect(servers, sessionTimeout, zkSetLogger)
	return &BurrowZookeeperClient{client: zkconn}, connEventChan, err
}

// Close shuts down the connection to the Zookeeper ensemble.
func (z *BurrowZookeeperClient) Close() {
	z.client.Close()
}

// ChildrenW returns a slice of names of child ZNodes immediately underneath the specified parent path. It also returns
// a zk.Stat describing the parent path, and a channel over which a zk.Event object will be sent if the child list
// changes (a child is added or deleted).
func (z *BurrowZookeeperClient) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return z.client.ChildrenW(path)
}

// GetW returns the data in the specified ZNode as a slice of bytes. It also returns a zk.Stat describing the ZNode, and
// a channel over which a zk.Event object will be sent if the ZNode changes (data changed, or ZNode deleted).
func (z *BurrowZookeeperClient) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return z.client.GetW(path)
}

// ExistsW returns a boolean stating whether or not the specified path exists. This method also sets a watch on the node
// (exists if it does not currently exist, or a data watch otherwise), providing an event channel that will receive a
// message when the watch fires
func (z *BurrowZookeeperClient) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	return z.client.ExistsW(path)
}

// Create makes a new ZNode at the specified path with the contents set to the data byte-slice. Flags can be provided
// to specify that this is an ephemeral or sequence node, and an ACL must be provided. If no ACL is desired, specify
//  zk.WorldACL(zk.PermAll)
func (z *BurrowZookeeperClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return z.client.Create(path, data, flags, acl)
}

// NewLock creates a lock using the provided path. Multiple Zookeeper clients, using the same lock path, can synchronize
// with each other to assure that only one client has the lock at any point.
func (z *BurrowZookeeperClient) NewLock(path string) protocol.ZookeeperLock {
	return zk.NewLock(z.client, path, zk.WorldACL(zk.PermAll))
}

// MockZookeeperClient is a mock of the protocol.ZookeeperClient interface to be used for testing. It should not be
// used in normal code.
type MockZookeeperClient struct {
	mock.Mock

	// InitialError can be set before using the MockZookeeperConnect call to specify an error that should be returned
	// from that call.
	InitialError error

	// EventChannel can be set before using the MockZookeeperConnect call to provide the channel that that call returns.
	EventChannel chan zk.Event

	// Servers stores the slice of strings that is provided to MockZookeeperConnect
	Servers []string

	// SessionTimeout stores the value that is provided to MockZookeeperConnect
	SessionTimeout time.Duration
}

// Close mocks protocol.ZookeeperClient.Close
func (m *MockZookeeperClient) Close() {
	m.Called()
	if m.EventChannel != nil {
		close(m.EventChannel)
	}
}

// ChildrenW mocks protocol.ZookeeperClient.ChildrenW
func (m *MockZookeeperClient) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	args := m.Called(path)
	return args.Get(0).([]string), args.Get(1).(*zk.Stat), args.Get(2).(<-chan zk.Event), args.Error(3)
}

// GetW mocks protocol.ZookeeperClient.GetW
func (m *MockZookeeperClient) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	args := m.Called(path)
	return args.Get(0).([]byte), args.Get(1).(*zk.Stat), args.Get(2).(<-chan zk.Event), args.Error(3)
}

// ExistsW mocks protocol.ZookeeperClient.ExistsW
func (m *MockZookeeperClient) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	args := m.Called(path)
	return args.Bool(0), args.Get(1).(*zk.Stat), args.Get(2).(<-chan zk.Event), args.Error(3)
}

// Create mocks protocol.ZookeeperClient.Create
func (m *MockZookeeperClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	args := m.Called(path, data, flags, acl)
	return args.String(0), args.Error(1)
}

// NewLock mocks protocol.ZookeeperClient.NewLock
func (m *MockZookeeperClient) NewLock(path string) protocol.ZookeeperLock {
	args := m.Called(path)
	return args.Get(0).(protocol.ZookeeperLock)
}

// MockZookeeperConnect is a func that mocks the ZookeeperConnect call, but allows us to pre-populate the return
// values and save the arguments provided for assertions.
func (m *MockZookeeperClient) MockZookeeperConnect(servers []string, sessionTimeout time.Duration, logger *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error) {
	m.Servers = servers
	m.SessionTimeout = sessionTimeout

	if m.EventChannel == nil {
		m.EventChannel = make(chan zk.Event)
	}
	return m, m.EventChannel, m.InitialError
}

// MockZookeeperLock is a mock of the protocol.ZookeeperLock interface. It should not be used in normal code.
type MockZookeeperLock struct {
	mock.Mock
}

// Lock mocks protocol.ZookeeperLock.Lock
func (m *MockZookeeperLock) Lock() error {
	args := m.Called()
	return args.Error(0)
}

// Unlock mocks protocol.ZookeeperLock.Unlock
func (m *MockZookeeperLock) Unlock() error {
	args := m.Called()
	return args.Error(0)
}
