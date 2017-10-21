package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/Shopify/sarama"
)

func TestBurrowSaramaClient_ImplementsSaramaClient(t *testing.T) {
	assert.Implements(t, (*SaramaClient)(nil), new(BurrowSaramaClient))
}

func TestMockSaramaClient_ImplementsSaramaClient(t *testing.T) {
	assert.Implements(t, (*SaramaClient)(nil), new(MockSaramaClient))
}

func TestBurrowSaramaBroker_ImplementsSaramaBroker(t *testing.T) {
	assert.Implements(t, (*SaramaBroker)(nil), new(BurrowSaramaBroker))
}

func TestMockSaramaBroker_ImplementsSaramaBroker(t *testing.T) {
	assert.Implements(t, (*SaramaBroker)(nil), new(MockSaramaBroker))
}

func TestMockSaramaConsumer_ImplementsSaramaConsumer(t *testing.T) {
	assert.Implements(t, (*sarama.Consumer)(nil), new(MockSaramaConsumer))
}

func TestMockSaramaPartitionConsumer_ImplementsSaramaPartitionConsumer(t *testing.T) {
	assert.Implements(t, (*sarama.PartitionConsumer)(nil), new(MockSaramaPartitionConsumer))
}
