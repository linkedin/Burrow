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
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"

	"github.com/linkedin/Burrow/core/configuration"
)

func GetSaramaConfigFromClientProfile(profile *configuration.ClientProfile) *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = profile.ClientID
	switch profile.KafkaVersion {
	case "0.8", "0.8.0":
		saramaConfig.Version = sarama.V0_8_2_0
	case "0.8.1":
		saramaConfig.Version = sarama.V0_8_2_1
	case "0.8.2":
		saramaConfig.Version = sarama.V0_8_2_2
	case "0.9", "0.9.0", "0.9.0.0":
		saramaConfig.Version = sarama.V0_9_0_0
	case "0.9.0.1":
		saramaConfig.Version = sarama.V0_9_0_1
	case "0.10", "0.10.0", "0.10.0.0":
		saramaConfig.Version = sarama.V0_10_0_0
	case "0.10.0.1":
		saramaConfig.Version = sarama.V0_10_0_1
	case "0.10.1", "0.10.1.0":
		saramaConfig.Version = sarama.V0_10_1_0
	case "", "0.10.2", "0.10.2.0":
		saramaConfig.Version = sarama.V0_10_2_0
	default:
		panic("Unknown Kafka Version: " + profile.KafkaVersion)
	}

	// Configure TLS if enabled
	if profile.TLS {
		saramaConfig.Net.TLS.Enable = true
		if profile.TLSCertFilePath == "" || profile.TLSKeyFilePath == "" || profile.TLSCAFilePath == "" {
			saramaConfig.Net.TLS.Config = &tls.Config{}
		} else {
			caCert, err := ioutil.ReadFile(profile.TLSCAFilePath)
			if err != nil {
				panic("cannot read TLS CA file: " + err.Error())
			}
			cert, err := tls.LoadX509KeyPair(profile.TLSCertFilePath, profile.TLSKeyFilePath)
			if err != nil {
				panic("cannot read TLS certificate or key file: " + err.Error())
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			saramaConfig.Net.TLS.Config = &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      caCertPool,
			}
			saramaConfig.Net.TLS.Config.BuildNameToCertificate()
		}
		saramaConfig.Net.TLS.Config.InsecureSkipVerify = profile.TLSNoVerify
	}

	// Configure SASL if enabled
	if profile.SASL {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Handshake = profile.HandshakeFirst
		saramaConfig.Net.SASL.User = profile.Username
		saramaConfig.Net.SASL.Password = profile.Password
	}

	return saramaConfig
}

// Sarama shim interface for Client and Broker
// We need this because the Broker type in Sarama is not an interface. Therefore it's very difficult to mock and test
// around it.

// Shim interface for sarama.Client. This duplicates the sarama.Client interface, except it changes the Broker type
// to SaramaBroker (our shim interface) to permit mocking
type SaramaClient interface {
	Config() *sarama.Config
	Brokers() []SaramaBroker
	Topics() ([]string, error)
	Partitions(topic string) ([]int32, error)
	WritablePartitions(topic string) ([]int32, error)
	Leader(topic string, partitionID int32) (SaramaBroker, error)
	Replicas(topic string, partitionID int32) ([]int32, error)
	InSyncReplicas(topic string, partitionID int32) ([]int32, error)
	RefreshMetadata(topics ...string) error
	GetOffset(topic string, partitionID int32, time int64) (int64, error)
	Coordinator(consumerGroup string) (SaramaBroker, error)
	RefreshCoordinator(consumerGroup string) error
	Close() error
	Closed() bool

	// This is an extra, needed for the consumer modules
	NewConsumerFromClient() (sarama.Consumer, error)
}
type BurrowSaramaClient struct {
	Client sarama.Client
}

// Implementation of SaramaClient which calls through to sarama.Client methods, and then creates BurrowSaramaBroker for
// each Broker returned
func (c *BurrowSaramaClient) Config() *sarama.Config {
	return c.Client.Config()
}
func (c *BurrowSaramaClient) Brokers() []SaramaBroker {
	brokers := c.Client.Brokers()
	shimBrokers := make([]SaramaBroker, len(brokers))
	for i, broker := range brokers {
		shimBrokers[i] = &BurrowSaramaBroker{broker}
	}
	return shimBrokers
}
func (c *BurrowSaramaClient) Topics() ([]string, error) {
	return c.Client.Topics()
}
func (c *BurrowSaramaClient) Partitions(topic string) ([]int32, error) {
	return c.Client.Partitions(topic)
}
func (c *BurrowSaramaClient) WritablePartitions(topic string) ([]int32, error) {
	return c.Client.WritablePartitions(topic)
}
func (c *BurrowSaramaClient) Leader(topic string, partitionID int32) (SaramaBroker, error) {
	broker, err := c.Client.Leader(topic, partitionID)
	var shimBroker *BurrowSaramaBroker
	if broker != nil {
		shimBroker = &BurrowSaramaBroker{broker}
	}
	return shimBroker, err
}
func (c *BurrowSaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	return c.Client.Replicas(topic, partitionID)
}
func (c *BurrowSaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	return c.Client.InSyncReplicas(topic, partitionID)
}
func (c *BurrowSaramaClient) RefreshMetadata(topics ...string) error {
	return c.Client.RefreshMetadata(topics...)
}
func (c *BurrowSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return c.Client.GetOffset(topic, partitionID, time)
}
func (c *BurrowSaramaClient) Coordinator(consumerGroup string) (SaramaBroker, error) {
	broker, err := c.Client.Coordinator(consumerGroup)
	var shimBroker *BurrowSaramaBroker
	if broker != nil {
		shimBroker = &BurrowSaramaBroker{broker}
	}
	return shimBroker, err
}
func (c *BurrowSaramaClient) RefreshCoordinator(consumerGroup string) error {
	return c.Client.RefreshCoordinator(consumerGroup)
}
func (c *BurrowSaramaClient) Close() error {
	return c.Client.Close()
}
func (c *BurrowSaramaClient) Closed() bool {
	return c.Client.Closed()
}
func (c *BurrowSaramaClient) NewConsumerFromClient() (sarama.Consumer, error) {
	return sarama.NewConsumerFromClient(c.Client)
}

// Right now, this interface only defines the methods that Burrow is using. It should not be considered a complete
// interface for sarama.Broker
type SaramaBroker interface {
	ID() int32
	Close() error
	GetAvailableOffsets(*sarama.OffsetRequest) (*sarama.OffsetResponse, error)
}
type BurrowSaramaBroker struct {
	broker *sarama.Broker
}

// Implementation of SaramaBroker which calls through to sarama.Broker methods
func (b *BurrowSaramaBroker) ID() int32 {
	return b.broker.ID()
}
func (b *BurrowSaramaBroker) Close() error {
	return b.broker.Close()
}
func (b *BurrowSaramaBroker) GetAvailableOffsets(request *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	return b.broker.GetAvailableOffsets(request)
}

// mock SaramaClient to use in tests
type MockSaramaClient struct {
	mock.Mock
}

func (m *MockSaramaClient) Config() *sarama.Config {
	args := m.Called()
	return args.Get(0).(*sarama.Config)
}
func (m *MockSaramaClient) Brokers() []SaramaBroker {
	args := m.Called()
	return args.Get(0).([]SaramaBroker)
}
func (m *MockSaramaClient) Topics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockSaramaClient) Partitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}
func (m *MockSaramaClient) WritablePartitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}
func (m *MockSaramaClient) Leader(topic string, partitionID int32) (SaramaBroker, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).(SaramaBroker), args.Error(1)
}
func (m *MockSaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}
func (m *MockSaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}
func (m *MockSaramaClient) RefreshMetadata(topics ...string) error {
	if len(topics) > 0 {
		args := m.Called([]interface{}{topics}...)
		return args.Error(0)
	} else {
		args := m.Called()
		return args.Error(0)
	}
}
func (m *MockSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	args := m.Called(topic, partitionID, time)
	return args.Get(0).(int64), args.Error(1)
}
func (m *MockSaramaClient) Coordinator(consumerGroup string) (SaramaBroker, error) {
	args := m.Called(consumerGroup)
	return args.Get(0).(SaramaBroker), args.Error(1)
}
func (m *MockSaramaClient) RefreshCoordinator(consumerGroup string) error {
	args := m.Called(consumerGroup)
	return args.Error(0)
}
func (m *MockSaramaClient) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockSaramaClient) Closed() bool {
	args := m.Called()
	return args.Bool(0)
}
func (m *MockSaramaClient) NewConsumerFromClient() (sarama.Consumer, error) {
	args := m.Called()
	return args.Get(0).(sarama.Consumer), args.Error(1)
}

// mock SaramaClient to use in tests
type MockSaramaBroker struct {
	mock.Mock
}

func (m *MockSaramaBroker) ID() int32 {
	args := m.Called()
	return args.Get(0).(int32)
}
func (m *MockSaramaBroker) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockSaramaBroker) GetAvailableOffsets(request *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	args := m.Called(request)
	return args.Get(0).(*sarama.OffsetResponse), args.Error(1)
}

// mock sarama.Consumer for testing kafka_client module
type MockSaramaConsumer struct {
	mock.Mock
}

func (m *MockSaramaConsumer) Topics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockSaramaConsumer) Partitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}
func (m *MockSaramaConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	args := m.Called(topic, partition, offset)
	return args.Get(0).(sarama.PartitionConsumer), args.Error(1)
}
func (m *MockSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	args := m.Called()
	return args.Get(0).(map[string]map[int32]int64)
}
func (m *MockSaramaConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

// mock sarama.PartitionConsumer for testing kafka_client module
type MockSaramaPartitionConsumer struct {
	mock.Mock
}

func (m *MockSaramaPartitionConsumer) AsyncClose() {
	m.Called()
}
func (m *MockSaramaPartitionConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockSaramaPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	args := m.Called()
	return args.Get(0).(<-chan *sarama.ConsumerMessage)
}
func (m *MockSaramaPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	args := m.Called()
	return args.Get(0).(<-chan *sarama.ConsumerError)
}
func (m *MockSaramaPartitionConsumer) HighWaterMarkOffset() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}
