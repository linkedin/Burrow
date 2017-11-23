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

	"github.com/linkedin/burrow/core/protocol"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// Implementation of the ZookeeperClient interface
type BurrowZookeeperClient struct {
	Client *zk.Conn
}

func ZookeeperConnect(servers []string, sessionTimeout time.Duration, logger *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error) {
	// We need a function to set the logger for the ZK connection
	zkSetLogger := func(c *zk.Conn) {
		c.SetLogger(zap.NewStdLog(logger))
	}

	zkconn, connEventChan, err := zk.Connect(servers, sessionTimeout, zkSetLogger)
	return &BurrowZookeeperClient{Client: zkconn}, connEventChan, err
}

func (z *BurrowZookeeperClient) Close() {
	z.Client.Close()
}

func (z *BurrowZookeeperClient) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return z.Client.ChildrenW(path)
}

func (z *BurrowZookeeperClient) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return z.Client.GetW(path)
}

func (z *BurrowZookeeperClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return z.Client.Create(path, data, flags, acl)
}

func (z *BurrowZookeeperClient) NewLock(path string) protocol.ZookeeperLock {
	return zk.NewLock(z.Client, path, zk.WorldACL(zk.PermAll))
}

// Mock ZookeeperClient for testing
type MockZookeeperClient struct {
	mock.Mock
	InitialError   error
	EventChannel   chan zk.Event
	Servers        []string
	SessionTimeout time.Duration
}

func (m *MockZookeeperClient) Close() {
	m.Called()
	if m.EventChannel != nil {
		close(m.EventChannel)
	}
}
func (m *MockZookeeperClient) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	args := m.Called(path)
	return args.Get(0).([]string), args.Get(1).(*zk.Stat), args.Get(2).(<-chan zk.Event), args.Error(3)
}
func (m *MockZookeeperClient) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	args := m.Called(path)
	return args.Get(0).([]byte), args.Get(1).(*zk.Stat), args.Get(2).(<-chan zk.Event), args.Error(3)
}
func (m *MockZookeeperClient) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	args := m.Called(path, data, flags, acl)
	return args.String(0), args.Error(1)
}
func (m *MockZookeeperClient) NewLock(path string) protocol.ZookeeperLock {
	args := m.Called(path)
	return args.Get(0).(protocol.ZookeeperLock)
}

// This method allows us to prepopulate the mock with calls before feeding it into the connect call
func (m *MockZookeeperClient) MockZookeeperConnect(servers []string, sessionTimeout time.Duration, logger *zap.Logger) (protocol.ZookeeperClient, <-chan zk.Event, error) {
	m.Servers = servers
	m.SessionTimeout = sessionTimeout

	if m.EventChannel == nil {
		m.EventChannel = make(chan zk.Event)
	}
	return m, m.EventChannel, m.InitialError
}

type MockZookeeperLock struct {
	mock.Mock
}

func (m *MockZookeeperLock) Lock() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockZookeeperLock) Unlock() error {
	args := m.Called()
	return args.Error(0)
}
