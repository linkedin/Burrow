package helpers

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestBurrowZookeeperClient_ImplementsZookeeperClient(t *testing.T) {
	assert.Implements(t, (*ZookeeperClient)(nil), new(BurrowZookeeperClient))
}

func TestMockZookeeperClient_ImplementsZookeeperClient(t *testing.T) {
	assert.Implements(t, (*ZookeeperClient)(nil), new(MockZookeeperClient))
}
