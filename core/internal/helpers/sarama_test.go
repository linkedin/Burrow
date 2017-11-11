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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
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
