/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package consumer

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

func fixtureModule() *KafkaClient {
	module := KafkaClient{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		StorageChannel: make(chan *protocol.StorageRequest),
	}

	viper.Reset()
	viper.Set("client-profile..client-id", "testid")
	viper.Set("cluster.test.class-name", "kafka")
	viper.Set("cluster.test.servers", []string{"broker1.example.com:1234"})
	viper.Set("consumer.test.class-name", "kafka")
	viper.Set("consumer.test.servers", []string{"broker1.example.com:1234"})
	viper.Set("consumer.test.cluster", "test")

	return &module
}

type errorTestSetBytes struct {
	KeyBytes   []byte
	ValueBytes []byte
}
type errorTestSetBytesWithString struct {
	Bytes   []byte
	ErrorAt string
}

func TestKafkaClient_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(KafkaClient))
}

func TestKafkaClient_Configure(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")
	assert.NotNil(t, module.saramaConfig, "Expected saramaConfig to be populated")
	assert.Equal(t, "__consumer_offsets", module.offsetsTopic, "Default OffsetTopic value of __consumer_offsets did not get set")
}

func TestKafkaClient_Configure_BadCluster(t *testing.T) {
	module := fixtureModule()
	viper.Set("consumer.test.cluster", "nocluster")

	assert.Panics(t, func() { module.Configure("test", "consumer.test") }, "The code did not panic")
}

func TestKafkaClient_Configure_BadRegexp(t *testing.T) {
	module := fixtureModule()
	viper.Set("consumer.test.group-whitelist", "[")
	assert.Panics(t, func() { module.Configure("test", "consumer.test") }, "The code did not panic")
}

func TestKafkaClient_partitionConsumer(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	// Channels for testing
	messageChan := make(chan *sarama.ConsumerMessage)
	errorChan := make(chan *sarama.ConsumerError)

	consumer := &helpers.MockSaramaPartitionConsumer{}
	consumer.On("AsyncClose").Return()
	consumer.On("Messages").Return(func() <-chan *sarama.ConsumerMessage { return messageChan }())
	consumer.On("Errors").Return(func() <-chan *sarama.ConsumerError { return errorChan }())

	module.running.Add(1)
	go module.partitionConsumer(consumer)

	// Send a message over the error channel to make sure it doesn't block
	testError := &sarama.ConsumerError{
		Topic:     "testtopic",
		Partition: 0,
		Err:       errors.New("test error"),
	}
	errorChan <- testError

	// Assure the partitionConsumer closes properly
	close(module.quitChannel)
	module.running.Wait()

	consumer.AssertExpectations(t)
}

func TestKafkaClient_startKafkaConsumer(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	// Channels for testing
	messageChan := make(chan *sarama.ConsumerMessage)
	errorChan := make(chan *sarama.ConsumerError)

	// Don't assert expectations on this - the way it goes down, they're called but don't show up
	mockPartitionConsumer := &helpers.MockSaramaPartitionConsumer{}
	mockPartitionConsumer.On("AsyncClose").Return()
	mockPartitionConsumer.On("Messages").Return(func() <-chan *sarama.ConsumerMessage { return messageChan }())
	mockPartitionConsumer.On("Errors").Return(func() <-chan *sarama.ConsumerError { return errorChan }())

	consumer := &helpers.MockSaramaConsumer{}
	consumer.On("ConsumePartition", "__consumer_offsets", int32(0), sarama.OffsetOldest).Return(mockPartitionConsumer, nil)

	client := &helpers.MockSaramaClient{}
	client.On("NewConsumerFromClient").Return(consumer, nil)
	client.On("Partitions", "__consumer_offsets").Return([]int32{0}, nil)

	err := module.startKafkaConsumer(client)
	assert.Nil(t, err, "Expected startKafkaConsumer to return no error")

	close(module.quitChannel)
	module.running.Wait()

	consumer.AssertExpectations(t)
	client.AssertExpectations(t)
}

func TestKafkaClient_startKafkaConsumer_FailCreateConsumer(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	// Set up the mock to return the leader broker for a test topic and partition
	testError := errors.New("test error")
	client := &helpers.MockSaramaClient{}
	client.On("NewConsumerFromClient").Return((*helpers.MockSaramaConsumer)(nil), testError)
	client.On("Close").Return(nil)

	err := module.startKafkaConsumer(client)
	client.AssertExpectations(t)
	assert.Equal(t, testError, err, "Expected startKafkaConsumer to return error")
}

func TestKafkaClient_startKafkaConsumer_FailGetPartitions(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	consumer := &helpers.MockSaramaConsumer{}
	testError := errors.New("test error")
	client := &helpers.MockSaramaClient{}
	client.On("NewConsumerFromClient").Return(consumer, nil)
	client.On("Partitions", "__consumer_offsets").Return([]int32{}, testError)
	client.On("Close").Return(nil)

	err := module.startKafkaConsumer(client)
	consumer.AssertExpectations(t)
	client.AssertExpectations(t)
	assert.Equal(t, testError, err, "Expected startKafkaConsumer to return error")
}

func TestKafkaClient_startKafkaConsumer_FailConsumePartition(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	testError := errors.New("test error")
	consumer := &helpers.MockSaramaConsumer{}
	consumer.On("ConsumePartition", "__consumer_offsets", int32(0), sarama.OffsetOldest).Return((*helpers.MockSaramaPartitionConsumer)(nil), testError)

	client := &helpers.MockSaramaClient{}
	client.On("NewConsumerFromClient").Return(consumer, nil)
	client.On("Partitions", "__consumer_offsets").Return([]int32{0}, nil)

	err := module.startKafkaConsumer(client)
	consumer.AssertExpectations(t)
	client.AssertExpectations(t)
	assert.Equal(t, testError, err, "Expected startKafkaConsumer to return error")
}

func TestKafkaClient_readString(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x04test"))
	result, err := readString(buf)

	assert.Equalf(t, "test", result, "Expected readString to return test, not %v", result)
	assert.Nil(t, err, "Expected readString to return no error")
}

func TestKafkaClient_readString_Underflow(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x05test"))
	result, err := readString(buf)

	assert.Equalf(t, "", result, "Expected readString to return empty string, not %v", result)
	assert.NotNil(t, err, "Expected readString to return an error")
}

func TestKafkaClient_decodeMetadataValueHeader(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader"))
	result, errorAt := decodeMetadataValueHeader(buf)

	assert.Equalf(t, "testtype", result.ProtocolType, "Expected ProtocolType to be testtype, not %v", result.ProtocolType)
	assert.Equalf(t, int32(1), result.Generation, "Expected Generation to be 1, not %v", result.Generation)
	assert.Equalf(t, "testprotocol", result.Protocol, "Expected Protocol to be testprotocol, not %v", result.Protocol)
	assert.Equalf(t, "testleader", result.Leader, "Expected Leader to be testleader, not %v", result.Leader)
	assert.Equalf(t, "", errorAt, "Expected decodeMetadataValueHeader to return empty errorAt, not %v", errorAt)
}

var decodeMetadataValueHeaderErrors = []errorTestSetBytesWithString{
	{[]byte("\x00\x08testt"), "protocol_type"},
	{[]byte("\x00\x08testtype\x00\x00"), "generation"},
	{[]byte("\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestp"), "protocol"},
	{[]byte("\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atest"), "leader"},
}

func TestKafkaClient_decodeMetadataValueHeader_Errors(t *testing.T) {
	for _, values := range decodeMetadataValueHeaderErrors {
		_, errorAt := decodeMetadataValueHeader(bytes.NewBuffer(values.Bytes))
		assert.Equalf(t, values.ErrorAt, errorAt, "Expected errorAt to be %v, not %v", values.ErrorAt, errorAt)
	}
}

func TestKafkaClient_decodeMetadataMember(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"))
	result, errorAt := decodeMetadataMember(buf, 1)

	assert.Equalf(t, "", errorAt, "Expected decodeMetadataMember to return empty errorAt, not %v", errorAt)
	assert.Equalf(t, "testmemberid", result.MemberID, "Expected MemberID to be testmemberid, not %v", result.MemberID)
	assert.Equalf(t, "testclientid", result.ClientID, "Expected ClientID to be testclientid, not %v", result.ClientID)
	assert.Equalf(t, "testclienthost", result.ClientHost, "Expected ClientHost to be testclienthost, not %v", result.ClientHost)
	assert.Equalf(t, int32(4), result.RebalanceTimeout, "Expected RebalanceTimeout to be 4, not %v", result.RebalanceTimeout)
	assert.Equalf(t, int32(8), result.SessionTimeout, "Expected SessionTimeout to be 8, not %v", result.SessionTimeout)
}

var decodeMetadataMemberErrors = []errorTestSetBytesWithString{
	{[]byte("\x00\x0ctestmemb"), "member_id"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclie"), "client_id"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestcl"), "client_host"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00"), "rebalance_timeout"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00"), "session_timeout"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00"), "subscription_bytes"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00"), "assignment_bytes"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x16\xff\xff\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00"), "consumer_protocol_version"},
	{[]byte("\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00\x00\x01\x00\x07topic1\x00\x00\x00\x01\x00\x00\x00\x00"), "assignment"},
}

func TestKafkaClient_decodeMetadataMember_Errors(t *testing.T) {
	for _, values := range decodeMetadataMemberErrors {
		_, errorAt := decodeMetadataMember(bytes.NewBuffer(values.Bytes), 1)
		assert.Equalf(t, values.ErrorAt, errorAt, "Expected errorAt to be %v, not %v", values.ErrorAt, errorAt)
	}
}

func TestKafkaClient_decodeMemberAssignmentV0(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"))
	assignment, errorAt := decodeMemberAssignmentV0(buf)

	assert.Equalf(t, "", errorAt, "Expected decodeMemberAssignmentV0 to return empty errorAt, not %v", errorAt)
	assert.Lenf(t, assignment, 1, "Expected Assignment to have 1 topic, not %v", len(assignment))
	topic, ok := assignment["topic1"]
	assert.True(t, ok, "Expected to find topic1 in Assignment")
	assert.Lenf(t, topic, 1, "Expected topic1 to have 1 partition, not %v", len(topic))
	assert.Equalf(t, int32(0), topic[0], "Expected partition ID to be 0, not %v", topic[0])
}

var decodeMemberAssignmentV0Errors = []errorTestSetBytesWithString{
	{[]byte("\x00\x00\x00"), "assignment_topic_count"},
	{[]byte("\x00\x00\x00\x01\x00\x06top"), "topic_name"},
	{[]byte("\x00\x00\x00\x01\x00\x06topic1\x00\x00"), "assignment_partition_count"},
	{[]byte("\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00"), "assignment_partition_id"},
}

func TestKafkaClient_decodeMemberAssignmentV0_Errors(t *testing.T) {
	for _, values := range decodeMemberAssignmentV0Errors {
		_, errorAt := decodeMemberAssignmentV0(bytes.NewBuffer(values.Bytes))
		assert.Equalf(t, values.ErrorAt, errorAt, "Expected errorAt to be %v, not %v", values.ErrorAt, errorAt)
	}
}

func TestKafkaClient_decodeOffsetKeyV0(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x09testgroup\x00\x09testtopic\x00\x00\x00\x0b"))
	result, errorAt := decodeOffsetKeyV0(buf)

	assert.Equalf(t, "", errorAt, "Expected decodeOffsetKeyV0 to return empty errorAt, not %v", errorAt)
	assert.Equalf(t, "testgroup", result.Group, "Expected Group to be testgroup, not %v", result.Group)
	assert.Equalf(t, "testtopic", result.Topic, "Expected Topic to be testtopic, not %v", result.Topic)
	assert.Equalf(t, int32(11), result.Partition, "Expected Partition to be 11, not %v", result.Partition)
}

var decodeOffsetKeyV0Errors = []errorTestSetBytesWithString{
	{[]byte("\x00\x09testg"), "group"},
	{[]byte("\x00\x09testgroup\x00\x09testto"), "topic"},
	{[]byte("\x00\x09testgroup\x00\x09testtopic\x00\x00"), "partition"},
}

func TestKafkaClient_decodeOffsetKeyV0_Errors(t *testing.T) {
	for _, values := range decodeOffsetKeyV0Errors {
		_, errorAt := decodeOffsetKeyV0(bytes.NewBuffer(values.Bytes))
		assert.Equalf(t, values.ErrorAt, errorAt, "Expected errorAt to be %v, not %v", values.ErrorAt, errorAt)
	}
}

func TestKafkaClient_decodeOffsetValueV0(t *testing.T) {
	buf := bytes.NewBuffer([]byte("\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65"))
	result, errorAt := decodeOffsetValueV0(buf)

	assert.Equalf(t, "", errorAt, "Expected decodeOffsetValueV0 to return empty errorAt, not %v", errorAt)
	assert.Equalf(t, int64(8372), result.Offset, "Expected Offset to be 8372, not %v", result.Offset)
	assert.Equalf(t, int64(1637), result.Timestamp, "Expected Timestamp to be 1637, not %v", result.Timestamp)
}

var decodeOffsetValueV0Errors = []errorTestSetBytesWithString{
	{[]byte("\x00\x00\x00\x00\x00"), "offset"},
	{[]byte("\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08tes"), "metadata"},
	{[]byte("\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00"), "timestamp"},
}

func TestKafkaClient_decodeOffsetValueV0_Errors(t *testing.T) {
	for _, values := range decodeOffsetValueV0Errors {
		_, errorAt := decodeOffsetValueV0(bytes.NewBuffer(values.Bytes))
		assert.Equalf(t, values.ErrorAt, errorAt, "Expected errorAt to be %v, not %v", values.ErrorAt, errorAt)
	}
}

func TestKafkaClient_decodeKeyAndOffset(t *testing.T) {
	module := fixtureModule()
	viper.Set("consumer.test.group-whitelist", "test.*")
	module.Configure("test", "consumer.test")

	keyBuf := bytes.NewBuffer([]byte("\x00\x09testgroup\x00\x09testtopic\x00\x00\x00\x0b"))
	valueBytes := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65")

	go module.decodeKeyAndOffset(keyBuf, valueBytes, zap.NewNop())
	request := <-module.App.StorageChannel

	assert.Equalf(t, protocol.StorageSetConsumerOffset, request.RequestType, "Expected request sent with type StorageSetConsumerOffset, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "testtopic", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(11), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, "testgroup", request.Group, "Expected request sent with Group testgroup, not %v", request.Group)
	assert.Equalf(t, int64(8372), request.Offset, "Expected Offset to be 8372, not %v", request.Offset)
	assert.Equalf(t, int64(1637), request.Timestamp, "Expected Timestamp to be 1637, not %v", request.Timestamp)
}

var decodeKeyAndOffsetErrors = []errorTestSetBytes{
	{[]byte("\x00\x09testgroup\x00\x09testt"), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65")},
	{[]byte("\x00\x09testgroup\x00\x09testtopic\x00\x00\x00\x0b"), []byte("\x00")},
	{[]byte("\x00\x09testgroup\x00\x09testtopic\x00\x00\x00\x0b"), []byte("\x00\x02\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65")},
}

func TestKafkaClient_decodeKeyAndOffset_BadValueVersion(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	for _, values := range decodeKeyAndOffsetErrors {
		// Should not timeout
		module.decodeKeyAndOffset(bytes.NewBuffer(values.KeyBytes), values.ValueBytes, zap.NewNop())
	}
}

func TestKafkaClient_decodeKeyAndOffset_Whitelist(t *testing.T) {
	module := fixtureModule()
	viper.Set("consumer.test.group-whitelist", "test.*")
	module.Configure("test", "consumer.test")

	keyBuf := bytes.NewBuffer([]byte("\x00\x0ddropthisgroup\x00\x09testtopic\x00\x00\x00\x0b"))
	valueBytes := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65")

	// Should not timeout as the group should be dropped by the whitelist
	module.decodeKeyAndOffset(keyBuf, valueBytes, zap.NewNop())
}

func TestKafkaClient_decodeAndSendOffset_ErrorValue(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	offsetKey := offsetKey{
		Group:     "testgroup",
		Topic:     "testtopic",
		Partition: 11,
	}
	valueBuf := bytes.NewBuffer([]byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testd"))

	module.decodeAndSendOffset(offsetKey, valueBuf, zap.NewNop())
	// Should not timeout
}

func TestKafkaClient_decodeGroupMetadata(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	keyBuf := bytes.NewBuffer([]byte("\x00\x09testgroup"))
	valueBytes := []byte("\x00\x01\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00\x01\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x0b\x00\x00\x00\x00")

	go module.decodeGroupMetadata(keyBuf, valueBytes, zap.NewNop())
	request := <-module.App.StorageChannel

	assert.Equalf(t, protocol.StorageSetConsumerOwner, request.RequestType, "Expected request sent with type StorageSetConsumerOwner, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "topic1", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(11), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, "testgroup", request.Group, "Expected request sent with Group testgroup, not %v", request.Group)
	assert.Equalf(t, "testclienthost", request.Owner, "Expected request sent with Owner testclienthost, not %v", request.Owner)
}

var decodeGroupMetadataErrors = []errorTestSetBytes{
	{[]byte("\x00\x09testg"), []byte("\x00\x01\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00\x01\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x0b")},
	{[]byte("\x00\x09testgroup"), []byte("\x00")},
	{[]byte("\x00\x09testgroup"), []byte("\x00\x02\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00\x01\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x16\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x0b")},
	{[]byte("\x00\x09testgroup"), []byte("\x00\x01\x00\x08test")},
	{[]byte("\x00\x09testgroup"), []byte("\x00\x01\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00")},
}

func TestKafkaClient_decodeGroupMetadata_Errors(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	for _, values := range decodeGroupMetadataErrors {
		// Should not timeout
		module.decodeGroupMetadata(bytes.NewBuffer(values.KeyBytes), values.ValueBytes, zap.NewNop())
	}
}

var decodeAndSendGroupMetadataErrors = [][]byte{
	[]byte("\x00\x08test"),
	[]byte("\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00"),
	[]byte("\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00\x01\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclie"),
}

func TestKafkaClient_decodeAndSendGroupMetadata_Errors(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	for _, value := range decodeAndSendGroupMetadataErrors {
		// Should not timeout
		module.decodeAndSendGroupMetadata(1, "testgroup", bytes.NewBuffer(value), zap.NewNop())
	}
}

func TestKafkaClient_processConsumerOffsetsMessage_Offset(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	msg := &sarama.ConsumerMessage{
		Key:       []byte("\x00\x02\x00\x09testgroup"),
		Value:     []byte("\x00\x01\x00\x08testtype\x00\x00\x00\x01\x00\x0ctestprotocol\x00\x0atestleader\x00\x00\x00\x01\x00\x0ctestmemberid\x00\x0ctestclientid\x00\x0etestclienthost\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x1a\x00\x00\x00\x00\x00\x01\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x0b\x00\x00\x00\x00"),
		Topic:     "__consumer_offsets",
		Partition: 0,
		Offset:    8232,
		Timestamp: time.Now(),
	}

	go module.processConsumerOffsetsMessage(msg)
	request := <-module.App.StorageChannel

	assert.Equalf(t, protocol.StorageSetConsumerOwner, request.RequestType, "Expected request sent with type StorageSetConsumerOwner, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "topic1", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(11), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, "testgroup", request.Group, "Expected request sent with Group testgroup, not %v", request.Group)
	assert.Equalf(t, "testclienthost", request.Owner, "Expected request sent with Owner testclienthost, not %v", request.Owner)
}

func TestKafkaClient_processConsumerOffsetsMessage_Metadata(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	msg := &sarama.ConsumerMessage{
		Key:       []byte("\x00\x01\x00\x09testgroup\x00\x09testtopic\x00\x00\x00\x0b"),
		Value:     []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65"),
		Topic:     "__consumer_offsets",
		Partition: 0,
		Offset:    8232,
		Timestamp: time.Now(),
	}

	go module.processConsumerOffsetsMessage(msg)
	request := <-module.App.StorageChannel

	assert.Equalf(t, protocol.StorageSetConsumerOffset, request.RequestType, "Expected request sent with type StorageSetConsumerOffset, not %v", request.RequestType)
	assert.Equalf(t, "test", request.Cluster, "Expected request sent with cluster test, not %v", request.Cluster)
	assert.Equalf(t, "testtopic", request.Topic, "Expected request sent with topic testtopic, not %v", request.Topic)
	assert.Equalf(t, int32(11), request.Partition, "Expected request sent with partition 0, not %v", request.Partition)
	assert.Equalf(t, "testgroup", request.Group, "Expected request sent with Group testgroup, not %v", request.Group)
	assert.Equalf(t, int64(8372), request.Offset, "Expected Offset to be 8372, not %v", request.Offset)
	assert.Equalf(t, int64(1637), request.Timestamp, "Expected Timestamp to be 1637, not %v", request.Timestamp)
}

var processConsumerOffsetsMessageErrors = []errorTestSetBytes{
	{[]byte("\x00"), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65")},
	{[]byte("\x00\x03\x00\x09testgroup\x00\x09testtopic\x00\x00\x00\x0b"), []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x20\xb4\x00\x08testdata\x00\x00\x00\x00\x00\x00\x06\x65")},
}

func TestKafkaClient_processConsumerOffsetsMessage_Errors(t *testing.T) {
	module := fixtureModule()
	module.Configure("test", "consumer.test")

	for _, values := range processConsumerOffsetsMessageErrors {
		msg := &sarama.ConsumerMessage{
			Key:       values.KeyBytes,
			Value:     values.ValueBytes,
			Topic:     "__consumer_offsets",
			Partition: 0,
			Offset:    8232,
			Timestamp: time.Now(),
		}

		// Should not timeout
		module.processConsumerOffsetsMessage(msg)
	}
}
