package helpers

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/linkedin/Burrow/core/protocol"
)

func TestBurrowZookeeperClient_ImplementsZookeeperClient(t *testing.T) {
	assert.Implements(t, (*protocol.ZookeeperClient)(nil), new(BurrowZookeeperClient))
}

func TestMockZookeeperClient_ImplementsZookeeperClient(t *testing.T) {
	assert.Implements(t, (*protocol.ZookeeperClient)(nil), new(MockZookeeperClient))
}
