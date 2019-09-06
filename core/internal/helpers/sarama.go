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
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

var kafkaVersions = map[string]sarama.KafkaVersion{
	"":         sarama.V0_10_2_0,
	"0.8.0":    sarama.V0_8_2_0,
	"0.8.1":    sarama.V0_8_2_1,
	"0.8.2":    sarama.V0_8_2_2,
	"0.8":      sarama.V0_8_2_0,
	"0.9.0.0":  sarama.V0_9_0_0,
	"0.9.0.1":  sarama.V0_9_0_1,
	"0.9.0":    sarama.V0_9_0_0,
	"0.9":      sarama.V0_9_0_0,
	"0.10.0.0": sarama.V0_10_0_0,
	"0.10.0.1": sarama.V0_10_0_1,
	"0.10.0":   sarama.V0_10_0_0,
	"0.10.1.0": sarama.V0_10_1_0,
	"0.10.1":   sarama.V0_10_1_0,
	"0.10.2.0": sarama.V0_10_2_0,
	"0.10.2.1": sarama.V0_10_2_0,
	"0.10.2":   sarama.V0_10_2_0,
	"0.10":     sarama.V0_10_0_0,
	"0.11.0.1": sarama.V0_11_0_0,
	"0.11.0.2": sarama.V0_11_0_0,
	"0.11.0":   sarama.V0_11_0_0,
	"1.0.0":    sarama.V1_0_0_0,
	"1.1.0":    sarama.V1_1_0_0,
	"1.1.1":    sarama.V1_1_0_0,
	"2.0.0":    sarama.V2_0_0_0,
	"2.0.1":    sarama.V2_0_0_0,
	"2.1.0":    sarama.V2_1_0_0,
	"2.2.0":    sarama.V2_2_0_0,
	"2.3.0":    sarama.V2_3_0_0,
}

func parseKafkaVersion(kafkaVersion string) sarama.KafkaVersion {
	version, ok := kafkaVersions[string(kafkaVersion)]
	if !ok {
		panic("Unknown Kafka Version: " + kafkaVersion)
	}

	return version
}

// GetSaramaConfigFromClientProfile takes the name of a client-profile configuration entry and returns a sarama.Config
// object that can be used to create a Sarama client with the specified configuration. This includes the Kafka version,
// client ID, TLS, and SASL configs. If there is any error in the configuration, such as a bad TLS certificate file,
// this func will panic as it is normally called when configuring modules.
func GetSaramaConfigFromClientProfile(profileName string) *sarama.Config {
	// Set config root and defaults
	configRoot := "client-profile." + profileName
	if (profileName != "") && (!viper.IsSet("client-profile." + profileName)) {
		panic("unknown client-profile '" + profileName + "'")
	}

	viper.SetDefault(configRoot+".client-id", "burrow-lagchecker")
	viper.SetDefault(configRoot+".kafka-version", "0.8")

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = viper.GetString(configRoot + ".client-id")
	saramaConfig.Version = parseKafkaVersion(viper.GetString(configRoot + ".kafka-version"))
	saramaConfig.Consumer.Return.Errors = true

	// Configure TLS if enabled
	if viper.IsSet(configRoot + ".tls") {
		tlsName := viper.GetString(configRoot + ".tls")

		saramaConfig.Net.TLS.Enable = true
		certFile := viper.GetString("tls." + tlsName + ".certfile")
		keyFile := viper.GetString("tls." + tlsName + ".keyfile")
		caFile := viper.GetString("tls." + tlsName + ".cafile")

		if caFile == "" {
			saramaConfig.Net.TLS.Config = &tls.Config{}
		} else {
			caCert, err := ioutil.ReadFile(caFile)
			if err != nil {
				panic("cannot read TLS CA file: " + err.Error())
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			saramaConfig.Net.TLS.Config = &tls.Config{
				RootCAs: caCertPool,
			}

			if certFile != "" && keyFile != "" {
				cert, err := tls.LoadX509KeyPair(certFile, keyFile)
				if err != nil {
					panic("cannot read TLS certificate or key file: " + err.Error())
				}
				saramaConfig.Net.TLS.Config.Certificates = []tls.Certificate{cert}
				saramaConfig.Net.TLS.Config.BuildNameToCertificate()
			}
		}
		saramaConfig.Net.TLS.Config.InsecureSkipVerify = viper.GetBool("tls." + tlsName + ".noverify")
	}

	// Configure SASL if enabled
	if viper.IsSet(configRoot + ".sasl") {
		saslName := viper.GetString(configRoot + ".sasl")

		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Handshake = viper.GetBool("sasl." + saslName + ".handshake-first")
		saramaConfig.Net.SASL.User = viper.GetString("sasl." + saslName + ".username")
		saramaConfig.Net.SASL.Password = viper.GetString("sasl." + saslName + ".password")
	}

	return saramaConfig
}

// SaramaClient is an internal interface to the sarama.Client. We use our own interface because while sarama.Client is
// an interface, sarama.Broker is not. This makes it difficult to test code which uses the Broker objects. This
// interface operates in the same way, with the addition of an interface function for creating consumers on the client.
type SaramaClient interface {
	// Config returns the Config struct of the client. This struct should not be altered after it has been created.
	Config() *sarama.Config

	// Brokers returns the current set of active brokers as retrieved from cluster metadata.
	Brokers() []SaramaBroker

	// Topics returns the set of available topics as retrieved from cluster metadata.
	Topics() ([]string, error)

	// Partitions returns the sorted list of all partition IDs for the given topic.
	Partitions(topic string) ([]int32, error)

	// WritablePartitions returns the sorted list of all writable partition IDs for the given topic, where "writable"
	// means "having a valid leader accepting writes".
	WritablePartitions(topic string) ([]int32, error)

	// Leader returns the broker object that is the leader of the current topic/partition, as determined by querying the
	// cluster metadata.
	Leader(topic string, partitionID int32) (SaramaBroker, error)

	// Replicas returns the set of all replica IDs for the given partition.
	Replicas(topic string, partitionID int32) ([]int32, error)

	// InSyncReplicas returns the set of all in-sync replica IDs for the given partition. In-sync replicas are replicas
	// which are fully caught up with the partition leader.
	InSyncReplicas(topic string, partitionID int32) ([]int32, error)

	// RefreshMetadata takes a list of topics and queries the cluster to refresh the available metadata for those topics.
	// If no topics are provided, it will refresh metadata for all topics.
	RefreshMetadata(topics ...string) error

	// GetOffset queries the cluster to get the most recent available offset at the given time (in milliseconds) on the
	// topic/partition combination. Time should be OffsetOldest for the earliest available offset, OffsetNewest for the
	// offset of the message that will be produced next, or a time.
	GetOffset(topic string, partitionID int32, time int64) (int64, error)

	// Coordinator returns the coordinating broker for a consumer group. It will return a locally cached value if it's
	// available. You can call RefreshCoordinator to update the cached value. This function only works on Kafka 0.8.2 and
	// higher.
	Coordinator(consumerGroup string) (SaramaBroker, error)

	// RefreshCoordinator retrieves the coordinator for a consumer group and stores it in local cache. This function only
	// works on Kafka 0.8.2 and higher.
	RefreshCoordinator(consumerGroup string) error

	// Close shuts down all broker connections managed by this client. It is required to call this function before a client
	// object passes out of scope, as it will otherwise leak memory. You must close any Producers or Consumers using a
	// client before you close the client.
	Close() error

	// Closed returns true if the client has already had Close called on it
	Closed() bool

	// NewConsumerFromClient creates a new consumer using the given client. It is still necessary to call Close() on the
	// underlying client when shutting down this consumer.
	NewConsumerFromClient() (sarama.Consumer, error)
}

// BurrowSaramaClient is an implementation of the SaramaClient interface for use in Burrow modules
type BurrowSaramaClient struct {
	Client sarama.Client
}

// Config returns the Config struct of the client. This struct should not be altered after it has been created.
func (c *BurrowSaramaClient) Config() *sarama.Config {
	return c.Client.Config()
}

// Brokers returns the current set of active brokers as retrieved from cluster metadata.
func (c *BurrowSaramaClient) Brokers() []SaramaBroker {
	brokers := c.Client.Brokers()
	shimBrokers := make([]SaramaBroker, len(brokers))
	for i, broker := range brokers {
		shimBrokers[i] = &BurrowSaramaBroker{broker}
	}
	return shimBrokers
}

// Topics returns the set of available topics as retrieved from cluster metadata.
func (c *BurrowSaramaClient) Topics() ([]string, error) {
	return c.Client.Topics()
}

// Partitions returns the sorted list of all partition IDs for the given topic.
func (c *BurrowSaramaClient) Partitions(topic string) ([]int32, error) {
	return c.Client.Partitions(topic)
}

// WritablePartitions returns the sorted list of all writable partition IDs for the given topic, where "writable"
// means "having a valid leader accepting writes".
func (c *BurrowSaramaClient) WritablePartitions(topic string) ([]int32, error) {
	return c.Client.WritablePartitions(topic)
}

// Leader returns the broker object that is the leader of the current topic/partition, as determined by querying the
// cluster metadata.
func (c *BurrowSaramaClient) Leader(topic string, partitionID int32) (SaramaBroker, error) {
	broker, err := c.Client.Leader(topic, partitionID)
	var shimBroker *BurrowSaramaBroker
	if broker != nil {
		shimBroker = &BurrowSaramaBroker{broker}
	}
	return shimBroker, err
}

// Replicas returns the set of all replica IDs for the given partition.
func (c *BurrowSaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	return c.Client.Replicas(topic, partitionID)
}

// InSyncReplicas returns the set of all in-sync replica IDs for the given partition. In-sync replicas are replicas
// which are fully caught up with the partition leader.
func (c *BurrowSaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	return c.Client.InSyncReplicas(topic, partitionID)
}

// RefreshMetadata takes a list of topics and queries the cluster to refresh the available metadata for those topics.
// If no topics are provided, it will refresh metadata for all topics.
func (c *BurrowSaramaClient) RefreshMetadata(topics ...string) error {
	return c.Client.RefreshMetadata(topics...)
}

// GetOffset queries the cluster to get the most recent available offset at the given time (in milliseconds) on the
// topic/partition combination. Time should be OffsetOldest for the earliest available offset, OffsetNewest for the
// offset of the message that will be produced next, or a time.
func (c *BurrowSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return c.Client.GetOffset(topic, partitionID, time)
}

// Coordinator returns the coordinating broker for a consumer group. It will return a locally cached value if it's
// available. You can call RefreshCoordinator to update the cached value. This function only works on Kafka 0.8.2 and
// higher.
func (c *BurrowSaramaClient) Coordinator(consumerGroup string) (SaramaBroker, error) {
	broker, err := c.Client.Coordinator(consumerGroup)
	var shimBroker *BurrowSaramaBroker
	if broker != nil {
		shimBroker = &BurrowSaramaBroker{broker}
	}
	return shimBroker, err
}

// RefreshCoordinator retrieves the coordinator for a consumer group and stores it in local cache. This function only
// works on Kafka 0.8.2 and higher.
func (c *BurrowSaramaClient) RefreshCoordinator(consumerGroup string) error {
	return c.Client.RefreshCoordinator(consumerGroup)
}

// Close shuts down all broker connections managed by this client. It is required to call this function before a client
// object passes out of scope, as it will otherwise leak memory. You must close any Producers or Consumers using a
// client before you close the client.
func (c *BurrowSaramaClient) Close() error {
	return c.Client.Close()
}

// Closed returns true if the client has already had Close called on it
func (c *BurrowSaramaClient) Closed() bool {
	return c.Client.Closed()
}

// NewConsumerFromClient creates a new consumer using the given client. It is still necessary to call Close() on the
// underlying client when shutting down this consumer.
func (c *BurrowSaramaClient) NewConsumerFromClient() (sarama.Consumer, error) {
	return sarama.NewConsumerFromClient(c.Client)
}

// SaramaBroker is an internal interface on the sarama.Broker struct. It is used with the SaramaClient interface in
// order to provide a fully testable interface for the pieces of Sarama that are used inside Burrow. Currently, this
// interface only defines the methods that Burrow is using. It should not be considered a complete interface for
// sarama.Broker
type SaramaBroker interface {
	// ID returns the broker ID retrieved from Kafka's metadata, or -1 if that is not known.
	ID() int32

	// Close closes the connection associated with the broker
	Close() error

	// GetAvailableOffsets sends an OffsetRequest to the broker and returns the OffsetResponse that was received
	GetAvailableOffsets(*sarama.OffsetRequest) (*sarama.OffsetResponse, error)
}

// BurrowSaramaBroker is an implementation of the SaramaBroker interface that is used with SaramaClient
type BurrowSaramaBroker struct {
	broker *sarama.Broker
}

// ID returns the broker ID retrieved from Kafka's metadata, or -1 if that is not known.
func (b *BurrowSaramaBroker) ID() int32 {
	return b.broker.ID()
}

// Close closes the connection associated with the broker
func (b *BurrowSaramaBroker) Close() error {
	return b.broker.Close()
}

// GetAvailableOffsets sends an OffsetRequest to the broker and returns the OffsetResponse that was received
func (b *BurrowSaramaBroker) GetAvailableOffsets(request *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	return b.broker.GetAvailableOffsets(request)
}

// MockSaramaClient is a mock of SaramaClient. It is used in tests by multiple packages. It should never be used in the
// normal code.
type MockSaramaClient struct {
	mock.Mock
}

// Config mocks SaramaClient.Config
func (m *MockSaramaClient) Config() *sarama.Config {
	args := m.Called()
	return args.Get(0).(*sarama.Config)
}

// Brokers mocks SaramaClient.Brokers
func (m *MockSaramaClient) Brokers() []SaramaBroker {
	args := m.Called()
	return args.Get(0).([]SaramaBroker)
}

// Topics mocks SaramaClient.Topics
func (m *MockSaramaClient) Topics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

// Partitions mocks SaramaClient.Partitions
func (m *MockSaramaClient) Partitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

// WritablePartitions mocks SaramaClient.WritablePartitions
func (m *MockSaramaClient) WritablePartitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

// Leader mocks SaramaClient.Leader
func (m *MockSaramaClient) Leader(topic string, partitionID int32) (SaramaBroker, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).(SaramaBroker), args.Error(1)
}

// Replicas mocks SaramaClient.Replicas
func (m *MockSaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}

// InSyncReplicas mocks SaramaClient.InSyncReplicas
func (m *MockSaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).([]int32), args.Error(1)
}

// RefreshMetadata mocks SaramaClient.RefreshMetadata
func (m *MockSaramaClient) RefreshMetadata(topics ...string) error {
	if len(topics) > 0 {
		args := m.Called([]interface{}{topics}...)
		return args.Error(0)
	}

	args := m.Called()
	return args.Error(0)
}

// GetOffset mocks SaramaClient.GetOffset
func (m *MockSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	args := m.Called(topic, partitionID, time)
	return args.Get(0).(int64), args.Error(1)
}

// Coordinator mocks SaramaClient.Coordinator
func (m *MockSaramaClient) Coordinator(consumerGroup string) (SaramaBroker, error) {
	args := m.Called(consumerGroup)
	return args.Get(0).(SaramaBroker), args.Error(1)
}

// RefreshCoordinator mocks SaramaClient.RefreshCoordinator
func (m *MockSaramaClient) RefreshCoordinator(consumerGroup string) error {
	args := m.Called(consumerGroup)
	return args.Error(0)
}

// Close mocks SaramaClient.Close
func (m *MockSaramaClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Closed mocks SaramaClient.Closed
func (m *MockSaramaClient) Closed() bool {
	args := m.Called()
	return args.Bool(0)
}

// NewConsumerFromClient mocks SaramaClient.NewConsumerFromClient
func (m *MockSaramaClient) NewConsumerFromClient() (sarama.Consumer, error) {
	args := m.Called()
	return args.Get(0).(sarama.Consumer), args.Error(1)
}

// MockSaramaBroker is a mock of SaramaBroker. It is used in tests by multiple packages. It should never be used in the
// normal code.
type MockSaramaBroker struct {
	mock.Mock
}

// ID mocks SaramaBroker.ID
func (m *MockSaramaBroker) ID() int32 {
	args := m.Called()
	return args.Get(0).(int32)
}

// Close mocks SaramaBroker.Close
func (m *MockSaramaBroker) Close() error {
	args := m.Called()
	return args.Error(0)
}

// GetAvailableOffsets mocks SaramaBroker.GetAvailableOffsets
func (m *MockSaramaBroker) GetAvailableOffsets(request *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	args := m.Called(request)
	return args.Get(0).(*sarama.OffsetResponse), args.Error(1)
}

// MockSaramaConsumer is a mock of sarama.Consumer. It is used in tests by multiple packages. It should never be used
// in the normal code.
type MockSaramaConsumer struct {
	mock.Mock
}

// Topics mocks sarama.Consumer.Topics
func (m *MockSaramaConsumer) Topics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

// Partitions mocks sarama.Consumer.Partitions
func (m *MockSaramaConsumer) Partitions(topic string) ([]int32, error) {
	args := m.Called(topic)
	return args.Get(0).([]int32), args.Error(1)
}

// ConsumePartition mocks sarama.Consumer.ConsumePartition
func (m *MockSaramaConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	args := m.Called(topic, partition, offset)
	return args.Get(0).(sarama.PartitionConsumer), args.Error(1)
}

// HighWaterMarks mocks sarama.Consumer.HighWaterMarks
func (m *MockSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	args := m.Called()
	return args.Get(0).(map[string]map[int32]int64)
}

// Close mocks sarama.Consumer.Close
func (m *MockSaramaConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockSaramaPartitionConsumer is a mock of sarama.PartitionConsumer. It is used in tests by multiple packages. It
// should never be used in the normal code.
type MockSaramaPartitionConsumer struct {
	mock.Mock
}

// AsyncClose mocks sarama.PartitionConsumer.AsyncClose
func (m *MockSaramaPartitionConsumer) AsyncClose() {
	m.Called()
}

// Close mocks sarama.PartitionConsumer.Close
func (m *MockSaramaPartitionConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Messages mocks sarama.PartitionConsumer.Messages
func (m *MockSaramaPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	args := m.Called()
	return args.Get(0).(<-chan *sarama.ConsumerMessage)
}

// Errors mocks sarama.PartitionConsumer.Errors
func (m *MockSaramaPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	args := m.Called()
	return args.Get(0).(<-chan *sarama.ConsumerError)
}

// HighWaterMarkOffset mocks sarama.PartitionConsumer.HighWaterMarkOffset
func (m *MockSaramaPartitionConsumer) HighWaterMarkOffset() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}
