package helpers

import (
	"io/ioutil"
	"crypto/tls"
	"crypto/x509"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

func GetSaramaConfigFromClientProfile(profile *configuration.ClientProfile) *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = profile.ClientID
	switch profile.KafkaVersion {
	case "0.8", "0.8.0": saramaConfig.Version = sarama.V0_8_2_0
	case "0.8.1": saramaConfig.Version = sarama.V0_8_2_1
	case "0.8.2": saramaConfig.Version = sarama.V0_8_2_2
	case "0.9", "0.9.0", "0.9.0.0": saramaConfig.Version = sarama.V0_9_0_0
	case "0.9.0.1": saramaConfig.Version = sarama.V0_9_0_1
	case "0.10", "0.10.0", "0.10.0.0": saramaConfig.Version = sarama.V0_10_0_0
	case "0.10.0.1": saramaConfig.Version = sarama.V0_10_0_1
	case "0.10.1", "0.10.1.0": saramaConfig.Version = sarama.V0_10_1_0
	case "", "0.10.2", "0.10.2.0": saramaConfig.Version = sarama.V0_10_2_0
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
				RootCAs: caCertPool,
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

func TimeoutSendStorageRequest(storageChannel chan *protocol.StorageRequest, request *protocol.StorageRequest, maxTime int) {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case storageChannel <- request:
	case <-timeout:
	}
}

// Sarama shim interface for Client and Broker
// We need this because the Broker type in Sarama is not an interface. Therefore it's very difficult to mock and test
// around it.

// Shim interface for sarama.Client. This duplicates the sarama.Client interface, except it changes the Broker type
// to BurrowSaramaBroker (our shim interface) to permit mocking
type BurrowSaramaClient interface {
	Config() *sarama.Config
	Brokers() []BurrowSaramaBroker
	Topics() ([]string, error)
	Partitions(topic string) ([]int32, error)
	WritablePartitions(topic string) ([]int32, error)
	Leader(topic string, partitionID int32) (BurrowSaramaBroker, error)
	Replicas(topic string, partitionID int32) ([]int32, error)
	InSyncReplicas(topic string, partitionID int32) ([]int32, error)
	RefreshMetadata(topics ...string) error
	GetOffset(topic string, partitionID int32, time int64) (int64, error)
	Coordinator(consumerGroup string) (BurrowSaramaBroker, error)
	RefreshCoordinator(consumerGroup string) error
	Close() error
	Closed() bool
}
type SaramaClient struct {
	Client sarama.Client
}

// Implementation of BurrowSaramaClient which calls through to sarama.Client methods, and then creates SaramaBroker for
// each Broker returned
func (c *SaramaClient) Config() *sarama.Config {
	return c.Client.Config()
}
func (c *SaramaClient) Brokers() []BurrowSaramaBroker {
	brokers := c.Client.Brokers()
	shimBrokers := make([]BurrowSaramaBroker, len(brokers))
	for i, broker := range brokers {
		shimBrokers[i] = &SaramaBroker{broker}
	}
	return shimBrokers
}
func (c *SaramaClient) Topics() ([]string, error) {
	return c.Client.Topics()
}
func (c *SaramaClient) Partitions(topic string) ([]int32, error) {
	return c.Client.Partitions(topic)
}
func (c *SaramaClient) WritablePartitions(topic string) ([]int32, error) {
	return c.Client.WritablePartitions(topic)
}
func (c *SaramaClient) Leader(topic string, partitionID int32) (BurrowSaramaBroker, error) {
	broker, err := c.Client.Leader(topic, partitionID)
	var shimBroker *SaramaBroker
	if broker != nil {
		shimBroker = &SaramaBroker{broker}
	}
	return shimBroker, err
}
func (c *SaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	return c.Client.Replicas(topic, partitionID)
}
func (c *SaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	return c.Client.InSyncReplicas(topic, partitionID)
}
func (c *SaramaClient) RefreshMetadata(topics ...string) error {
	return c.Client.RefreshMetadata(topics...)
}
func (c *SaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return c.Client.GetOffset(topic, partitionID, time)
}
func (c *SaramaClient) Coordinator(consumerGroup string) (BurrowSaramaBroker, error) {
	broker, err := c.Client.Coordinator(consumerGroup)
	var shimBroker *SaramaBroker
	if broker != nil {
		shimBroker = &SaramaBroker{broker}
	}
	return shimBroker, err
}
func (c *SaramaClient) RefreshCoordinator(consumerGroup string) error {
	return c.Client.RefreshCoordinator(consumerGroup)
}
func (c *SaramaClient) Close() error {
	return c.Client.Close()
}
func (c *SaramaClient) Closed() bool {
	return c.Client.Closed()
}

// Right now, this interface only defines the methods that Burrow is using. It should not be considered a complete
// interface for sarama.Broker
type BurrowSaramaBroker interface {
	ID() int32
	Close() error
	GetAvailableOffsets(*sarama.OffsetRequest) (*sarama.OffsetResponse, error)
}
type SaramaBroker struct {
	broker *sarama.Broker
}

// Implementation of BurrowSaramaBroker which calls through to sarama.Broker methods
func (b *SaramaBroker) ID() int32 {
	return b.broker.ID()
}
func (b *SaramaBroker) Close() error {
	return b.broker.Close()
}
func (b *SaramaBroker) GetAvailableOffsets(request *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	return b.broker.GetAvailableOffsets(request)
}

// mock BurrowSaramaClient to use in tests
type MockSaramaClient struct {
	mock.Mock
}
func (m *MockSaramaClient) Config() *sarama.Config {
	args := m.Called()
	return args.Get(0).(*sarama.Config)
}
func (m *MockSaramaClient) Brokers() []BurrowSaramaBroker {
	args := m.Called()
	return args.Get(0).([]BurrowSaramaBroker)
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
func (m *MockSaramaClient) Leader(topic string, partitionID int32) (BurrowSaramaBroker, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).(BurrowSaramaBroker), args.Error(1)
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
func (m *MockSaramaClient) Coordinator(consumerGroup string) (BurrowSaramaBroker, error) {
	args := m.Called(consumerGroup)
	return args.Get(0).(BurrowSaramaBroker), args.Error(1)
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

// mock BurrowSaramaClient to use in tests
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
