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
	"encoding/binary"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/spf13/viper"
	"regexp"
)

type KafkaClient struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name           string
	cluster        string
	servers        []string
	offsetsTopic   string
	startLatest    bool
	saramaConfig   *sarama.Config
	groupWhitelist *regexp.Regexp

	quitChannel chan struct{}
	running     sync.WaitGroup
}

func (module *KafkaClient) Configure(name string, configRoot string) {
	module.Log.Info("configuring")

	module.name = name
	module.quitChannel = make(chan struct{})
	module.running = sync.WaitGroup{}

	module.cluster = viper.GetString(configRoot + ".cluster")
	if !viper.IsSet("cluster." + module.cluster) {
		panic("Consumer '" + name + "' references an unknown cluster '" + module.cluster + "'")
	}

	profile := viper.GetString(configRoot + ".client-profile")
	module.saramaConfig = helpers.GetSaramaConfigFromClientProfile(profile)

	module.servers = viper.GetStringSlice(configRoot + ".servers")
	if len(module.servers) == 0 {
		panic("No Kafka brokers specified for consumer " + module.name)
	} else if !helpers.ValidateHostList(module.servers) {
		panic("Consumer '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed, and get them
	viper.SetDefault(configRoot+".offsets-topic", "__consumer_offsets")
	module.offsetsTopic = viper.GetString(configRoot + ".offsets-topic")
	module.startLatest = viper.GetBool(configRoot + ".start-latest")

	whitelist := viper.GetString(configRoot + ".group-whitelist")
	if whitelist != "" {
		re, err := regexp.Compile(whitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}
}

func (module *KafkaClient) Start() error {
	module.Log.Info("starting")

	// Connect Kafka client
	client, err := sarama.NewClient(module.servers, module.saramaConfig)
	if err != nil {
		return err
	}

	// Start the consumers
	err = module.startKafkaConsumer(&helpers.BurrowSaramaClient{Client: client})
	if err != nil {
		client.Close()
		return err
	}

	return nil
}

func (module *KafkaClient) Stop() error {
	module.Log.Info("stopping")

	close(module.quitChannel)
	module.running.Wait()

	return nil
}

func (module *KafkaClient) partitionConsumer(consumer sarama.PartitionConsumer) {
	module.running.Add(1)
	defer module.running.Done()
	defer consumer.AsyncClose()

	for {
		select {
		case msg := <-consumer.Messages():
			module.processConsumerOffsetsMessage(msg)
		case err := <-consumer.Errors():
			module.Log.Error("consume error",
				zap.String("topic", err.Topic),
				zap.Int32("partition", err.Partition),
				zap.String("error", err.Err.Error()),
			)
		case <-module.quitChannel:
			return
		}
	}
}

func (module *KafkaClient) startKafkaConsumer(client helpers.SaramaClient) error {
	// Create the consumer from the client
	consumer, err := client.NewConsumerFromClient()
	if err != nil {
		client.Close()
		return err
	}

	// Get a partition count for the consumption topic
	partitions, err := client.Partitions(module.offsetsTopic)
	if err != nil {
		module.Log.Error("failed to get partition count",
			zap.String("topic", module.offsetsTopic),
			zap.String("error", err.Error()),
		)
		client.Close()
		return err
	}

	// Default to bootstrapping the offsets topic, unless configured otherwise
	startFrom := sarama.OffsetOldest
	if module.startLatest {
		startFrom = sarama.OffsetNewest
	}

	// Start consumers for each partition with fan in
	module.Log.Info("starting consumers",
		zap.String("topic", module.offsetsTopic),
		zap.Int("count", len(partitions)),
	)
	for i, partition := range partitions {
		pconsumer, err := consumer.ConsumePartition(module.offsetsTopic, partition, startFrom)
		if err != nil {
			module.Log.Error("failed to consume partition",
				zap.String("topic", module.offsetsTopic),
				zap.Int("partition", i),
				zap.String("error", err.Error()),
			)
			return err
		}
		go module.partitionConsumer(pconsumer)
	}

	return nil
}

func (module *KafkaClient) processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) {
	logger := module.Log.With(
		zap.String("offset_topic", msg.Topic),
		zap.Int32("offset_partition", msg.Partition),
		zap.Int64("offset_offset", msg.Offset),
	)

	if len(msg.Value) == 0 {
		// Tombstone message - we don't handle them for now
		logger.Debug("dropped tombstone")
		return
	}

	var keyver int16
	keyBuffer := bytes.NewBuffer(msg.Key)
	err := binary.Read(keyBuffer, binary.BigEndian, &keyver)
	if err != nil {
		logger.Warn("failed to decode",
			zap.String("reason", "no key version"),
		)
		return
	}

	switch keyver {
	case 0, 1:
		module.decodeKeyAndOffset(keyBuffer, msg.Value, logger)
	case 2:
		module.decodeGroupMetadata(keyBuffer, msg.Value, logger)
	default:
		logger.Warn("failed to decode",
			zap.String("reason", "key version"),
			zap.Int16("version", keyver),
		)
	}
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen int16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	if strlen == -1 {
		return "", nil
	}

	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

type OffsetKey struct {
	Group     string
	Topic     string
	Partition int32
	ErrorAt   string
}
type OffsetValue struct {
	Offset    int64
	Timestamp int64
	ErrorAt   string
}
type MetadataHeader struct {
	ProtocolType string
	Generation   int32
	Protocol     string
	Leader       string
}
type MetadataMember struct {
	MemberId         string
	ClientId         string
	ClientHost       string
	RebalanceTimeout int32
	SessionTimeout   int32
	Assignment       map[string][]int32
}

func (module *KafkaClient) acceptConsumerGroup(group string) bool {
	// No whitelist means everything passes
	if module.groupWhitelist == nil {
		return true
	}
	return module.groupWhitelist.MatchString(group)
}

func (module *KafkaClient) decodeKeyAndOffset(keyBuffer *bytes.Buffer, value []byte, logger *zap.Logger) {
	// Version 0 and 1 keys are decoded the same way
	offsetKey, errorAt := decodeOffsetKeyV0(keyBuffer)
	if errorAt != "" {
		logger.Warn("failed to decode",
			zap.String("message_type", "offset"),
			zap.String("group", offsetKey.Group),
			zap.String("topic", offsetKey.Topic),
			zap.Int32("partition", offsetKey.Partition),
			zap.String("reason", errorAt),
		)
		return
	}

	offsetLogger := logger.With(
		zap.String("message_type", "offset"),
		zap.String("group", offsetKey.Group),
		zap.String("topic", offsetKey.Topic),
		zap.Int32("partition", offsetKey.Partition),
	)

	if !module.acceptConsumerGroup(offsetKey.Group) {
		offsetLogger.Debug("dropped", zap.String("reason", "whitelist"))
		return
	}

	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err := binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		offsetLogger.Warn("failed to decode",
			zap.String("reason", "no value version"),
		)
		return
	}

	switch valueVersion {
	case 0, 1:
		module.decodeAndSendOffset(offsetKey, valueBuffer, offsetLogger)
	default:
		offsetLogger.Warn("failed to decode",
			zap.String("reason", "value version"),
			zap.Int16("version", valueVersion),
		)
	}
}

func (module *KafkaClient) decodeAndSendOffset(offsetKey OffsetKey, valueBuffer *bytes.Buffer, logger *zap.Logger) {
	offsetValue, errorAt := decodeOffsetValueV0(valueBuffer)
	if errorAt != "" {
		logger.Warn("failed to decode",
			zap.Int64("offset", offsetValue.Offset),
			zap.Int64("timestamp", offsetValue.Timestamp),
			zap.String("reason", errorAt),
		)
		return
	}

	partitionOffset := &protocol.StorageRequest{
		RequestType: protocol.StorageSetConsumerOffset,
		Cluster:     module.cluster,
		Topic:       offsetKey.Topic,
		Partition:   int32(offsetKey.Partition),
		Group:       offsetKey.Group,
		Timestamp:   int64(offsetValue.Timestamp),
		Offset:      int64(offsetValue.Offset),
	}
	logger.Debug("consumer offset",
		zap.Int64("offset", offsetValue.Offset),
		zap.Int64("timestamp", offsetValue.Timestamp),
	)
	helpers.TimeoutSendStorageRequest(module.App.StorageChannel, partitionOffset, 1)
}

func (module *KafkaClient) decodeGroupMetadata(keyBuffer *bytes.Buffer, value []byte, logger *zap.Logger) {
	group, err := readString(keyBuffer)
	if err != nil {
		logger.Warn("failed to decode",
			zap.String("message_type", "metadata"),
			zap.String("reason", "group"),
		)
		return
	}

	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err = binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		logger.Warn("failed to decode",
			zap.String("message_type", "metadata"),
			zap.String("group", group),
			zap.String("reason", "no value version"),
		)
		return
	}

	switch valueVersion {
	case 0, 1:
		module.decodeAndSendGroupMetadata(valueVersion, group, valueBuffer, logger.With(
			zap.String("message_type", "metadata"),
			zap.String("group", group),
		))
	default:
		logger.Warn("failed to decode",
			zap.String("message_type", "metadata"),
			zap.String("group", group),
			zap.String("reason", "value version"),
			zap.Int16("version", valueVersion),
		)
	}
}

func (module *KafkaClient) decodeAndSendGroupMetadata(valueVersion int16, group string, valueBuffer *bytes.Buffer, logger *zap.Logger) {
	metadataHeader, errorAt := decodeMetadataValueHeader(valueBuffer)
	metadataLogger := logger.With(
		zap.String("protocol_type", metadataHeader.ProtocolType),
		zap.Int32("generation", metadataHeader.Generation),
		zap.String("protocol", metadataHeader.Protocol),
		zap.String("leader", metadataHeader.Leader),
	)
	if errorAt != "" {
		metadataLogger.Warn("failed to decode",
			zap.String("reason", errorAt),
		)
		return
	}

	var memberCount int32
	err := binary.Read(valueBuffer, binary.BigEndian, &memberCount)
	if err != nil {
		metadataLogger.Warn("failed to decode",
			zap.String("reason", "no member size"),
		)
		return
	}

	// If memberCount is zero, clear all ownership
	if memberCount == 0 {
		metadataLogger.Debug("clear owners")
		helpers.TimeoutSendStorageRequest(module.App.StorageChannel, &protocol.StorageRequest{
			RequestType: protocol.StorageClearConsumerOwners,
			Cluster:     module.cluster,
			Group:       group,
		}, 1)
		return
	}

	count := int(memberCount)
	for i := 0; i < count; i++ {
		member, errorAt := decodeMetadataMember(valueBuffer, valueVersion)
		if errorAt != "" {
			metadataLogger.Warn("failed to decode",
				zap.String("reason", errorAt),
			)
			return
		}

		metadataLogger.Debug("group metadata")
		for topic, partitions := range member.Assignment {
			for _, partition := range partitions {
				helpers.TimeoutSendStorageRequest(module.App.StorageChannel, &protocol.StorageRequest{
					RequestType: protocol.StorageSetConsumerOwner,
					Cluster:     module.cluster,
					Topic:       topic,
					Partition:   partition,
					Group:       group,
					Owner:       member.ClientHost,
				}, 1)
			}
		}
	}
}

func decodeMetadataValueHeader(buf *bytes.Buffer) (MetadataHeader, string) {
	var err error
	metadataHeader := MetadataHeader{}

	metadataHeader.ProtocolType, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol_type"
	}
	err = binary.Read(buf, binary.BigEndian, &metadataHeader.Generation)
	if err != nil {
		return metadataHeader, "generation"
	}
	metadataHeader.Protocol, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol"
	}
	metadataHeader.Leader, err = readString(buf)
	if err != nil {
		return metadataHeader, "leader"
	}
	return metadataHeader, ""
}

func decodeMetadataMember(buf *bytes.Buffer, memberVersion int16) (MetadataMember, string) {
	var err error
	memberMetadata := MetadataMember{}

	memberMetadata.MemberId, err = readString(buf)
	if err != nil {
		return memberMetadata, "member_id"
	}
	memberMetadata.ClientId, err = readString(buf)
	if err != nil {
		return memberMetadata, "client_id"
	}
	memberMetadata.ClientHost, err = readString(buf)
	if err != nil {
		return memberMetadata, "client_host"
	}
	if memberVersion == 1 {
		err = binary.Read(buf, binary.BigEndian, &memberMetadata.RebalanceTimeout)
		if err != nil {
			return memberMetadata, "rebalance_timeout"
		}
	}
	err = binary.Read(buf, binary.BigEndian, &memberMetadata.SessionTimeout)
	if err != nil {
		return memberMetadata, "session_timeout"
	}

	var subscriptionBytes int32
	err = binary.Read(buf, binary.BigEndian, &subscriptionBytes)
	if err != nil {
		return memberMetadata, "subscription_bytes"
	}
	if subscriptionBytes > 0 {
		buf.Next(int(subscriptionBytes))
	}

	var assignmentBytes int32
	err = binary.Read(buf, binary.BigEndian, &assignmentBytes)
	if err != nil {
		return memberMetadata, "assignment_bytes"
	}

	if assignmentBytes > 0 {
		var consumerProtocolVersion int16
		err = binary.Read(buf, binary.BigEndian, &consumerProtocolVersion)
		if err != nil {
			return memberMetadata, "consumer_protocol_version"
		}
		if consumerProtocolVersion != 0 {
			return memberMetadata, "consumer_protocol_version"
		}
		assignment, errorAt := decodeMemberAssignmentV0(buf)
		if errorAt != "" {
			return memberMetadata, "assignment"
		}
		memberMetadata.Assignment = assignment
	}

	return memberMetadata, ""
}

func decodeMemberAssignmentV0(buf *bytes.Buffer) (map[string][]int32, string) {
	var err error
	var topics map[string][]int32
	var numTopics, numPartitions, partitionId, userDataLen int32

	err = binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return topics, "assignment_topic_count"
	}

	topicCount := int(numTopics)
	topics = make(map[string][]int32, numTopics)
	for i := 0; i < topicCount; i++ {
		topicName, err := readString(buf)
		if err != nil {
			return topics, "topic_name"
		}

		err = binary.Read(buf, binary.BigEndian, &numPartitions)
		if err != nil {
			return topics, "assignment_partition_count"
		}
		partitionCount := int(numPartitions)
		topics[topicName] = make([]int32, numPartitions)
		for j := 0; j < partitionCount; j++ {
			err = binary.Read(buf, binary.BigEndian, &partitionId)
			if err != nil {
				return topics, "assignment_partition_id"
			}
			topics[topicName][j] = int32(partitionId)
		}
	}

	err = binary.Read(buf, binary.BigEndian, &userDataLen)
	if err != nil {
		return topics, "user_bytes"
	}
	if userDataLen > 0 {
		buf.Next(int(userDataLen))
	}

	return topics, ""
}

func decodeOffsetKeyV0(buf *bytes.Buffer) (OffsetKey, string) {
	var err error
	offsetKey := OffsetKey{}

	offsetKey.Group, err = readString(buf)
	if err != nil {
		return offsetKey, "group"
	}
	offsetKey.Topic, err = readString(buf)
	if err != nil {
		return offsetKey, "topic"
	}
	err = binary.Read(buf, binary.BigEndian, &offsetKey.Partition)
	if err != nil {
		return offsetKey, "partition"
	}
	return offsetKey, ""
}

func decodeOffsetValueV0(valueBuffer *bytes.Buffer) (OffsetValue, string) {
	var err error
	offsetValue := OffsetValue{}

	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		return offsetValue, "offset"
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offsetValue, "metadata"
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		return offsetValue, "timestamp"
	}
	return offsetValue, ""
}
