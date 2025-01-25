// Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package helpers

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/IBM/sarama"
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

func TestInitSaramaLogging(t *testing.T) {
	// given
	var entries = make([]zapcore.Entry, 0)
	d, _ := zap.NewDevelopment()
	logger := zap.New(zapcore.RegisterHooks(d.Core(), func(entry zapcore.Entry) error {
		entries = append(entries, entry)
		return nil
	}))
	InitSaramaLogging(logger)

	// when
	sarama.Logger.Printf("hello")

	// then
	assert.Len(t, entries, 1)
	assert.Equal(t, entries[0].Message, "hello")
	assert.Equal(t, entries[0].Level, zap.DebugLevel)
}

func shouldPanicForVersion(t *testing.T, v string) {
	defer func() { recover() }()
	out := parseKafkaVersion(v)
	t.Errorf("Kafka version %s should have panicked, but got: %s", v, out.String())
}

func TestVersionMapping(t *testing.T) {
	assert.Equal(t, parseKafkaVersion(""), sarama.V0_10_2_0)
	assert.Equal(t, parseKafkaVersion("0.10"), sarama.V0_10_0_0)
	assert.Equal(t, parseKafkaVersion("0.10.2"), sarama.V0_10_2_0)
	assert.Equal(t, parseKafkaVersion("0.10.2.0"), sarama.V0_10_2_0)
	// some other legacy cases
	assert.Equal(t, parseKafkaVersion("0.8.0"), sarama.V0_8_2_0)
	assert.Equal(t, parseKafkaVersion("0.8.1"), sarama.V0_8_2_1)
	assert.Equal(t, parseKafkaVersion("0.8.2"), sarama.V0_8_2_2)
	// and older versions that want to use the 4-part version
	assert.Equal(t, parseKafkaVersion("0.8.2.2"), sarama.V0_8_2_2)
	assert.Equal(t, parseKafkaVersion("0.10.2.1"), sarama.V0_10_2_1)
	assert.Equal(t, parseKafkaVersion("0.11.0.1"), sarama.V0_11_0_1)
	// check some of the newer versions
	assert.Equal(t, parseKafkaVersion("1.0.0"), sarama.V1_0_0_0)
	assert.Equal(t, parseKafkaVersion("1.0.2"), sarama.V1_0_2_0)
	assert.Equal(t, parseKafkaVersion("2.1.0"), sarama.V2_1_0_0)
	assert.Equal(t, parseKafkaVersion("2.2.0"), sarama.V2_2_0_0)
	assert.Equal(t, parseKafkaVersion("3.0.0"), sarama.V3_0_0_0)

	// check that we fail a 4-part version for newer versions
	shouldPanicForVersion(t, "3.0.0.0")
	// or for other unknown/unsupported versions
	shouldPanicForVersion(t, "foo")
}
