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

// mock sarama.Client to use in tests
type MockSaramaClient struct {
	mock.Mock
}
func (m *MockSaramaClient) Config() *sarama.Config {
	args := m.Called()
	return args.Get(0).(*sarama.Config)
}
func (m *MockSaramaClient) Brokers() []*sarama.Broker {
	args := m.Called()
	return args.Get(0).([]*sarama.Broker)
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
func (m *MockSaramaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	args := m.Called(topic, partitionID)
	return args.Get(0).(*sarama.Broker), args.Error(1)
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
		args := m.Called(topics)
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
func (m *MockSaramaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	args := m.Called(consumerGroup)
	return args.Get(0).(*sarama.Broker), args.Error(1)
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
