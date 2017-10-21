package helpers

import (
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/mock"
)

// Zookeeper client interface
// Note that this is a minimal interface for what Burrow needs - add functions as necessary
type ZookeeperClient interface {
	Close()
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
}
type BurrowZookeeperClient struct {
	Client *zk.Conn
}

func ZookeeperConnect(servers []string, sessionTimeout time.Duration) (ZookeeperClient, <-chan zk.Event, error) {
	zkconn, connEventChan, err := zk.Connect(servers, sessionTimeout)
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
	if (m.EventChannel != nil) {
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

// This method allows us to prepopulate the mock with calls before feeding it into the connect call
func (m *MockZookeeperClient) MockZookeeperConnect(servers []string, sessionTimeout time.Duration) (ZookeeperClient, <-chan zk.Event, error) {
	m.Servers = servers
	m.SessionTimeout = sessionTimeout

	if m.EventChannel == nil {
		m.EventChannel = make(chan zk.Event)
	}
	return m, m.EventChannel, m.InitialError
}
