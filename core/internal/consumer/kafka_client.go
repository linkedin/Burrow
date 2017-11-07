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

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"regexp"
)

type KafkaClient struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name            string
	myConfiguration *configuration.ConsumerConfig

	saramaConfig   *sarama.Config
	clientProfile  *configuration.ClientProfile
	messageChannel chan *sarama.ConsumerMessage
	errorChannel   chan *sarama.ConsumerError
	groupWhitelist *regexp.Regexp

	quitChannel         chan struct{}
	consumerQuitChannel chan struct{}
	running             sync.WaitGroup
	consumerRunning     sync.WaitGroup
}

func (module *KafkaClient) Configure(name string) {
	module.Log.Info("configuring")

	module.name = name
	module.quitChannel = make(chan struct{})
	module.consumerQuitChannel = make(chan struct{})
	module.running = sync.WaitGroup{}
	module.consumerRunning = sync.WaitGroup{}
	module.myConfiguration = module.App.Configuration.Consumer[name]
	module.messageChannel = make(chan *sarama.ConsumerMessage)

	if _, ok := module.App.Configuration.Cluster[module.myConfiguration.Cluster]; !ok {
		panic("Consumer '" + name + "' references an unknown cluster '" + module.myConfiguration.Cluster + "'")
	}
	if profile, ok := module.App.Configuration.ClientProfile[module.myConfiguration.ClientProfile]; ok {
		module.saramaConfig = helpers.GetSaramaConfigFromClientProfile(profile)
	} else {
		panic("Consumer '" + name + "' references an unknown client-profile '" + module.myConfiguration.ClientProfile + "'")
	}
	if len(module.myConfiguration.Servers) == 0 {
		panic("No Kafka brokers specified for consumer " + module.name)
	} else if !configuration.ValidateHostList(module.myConfiguration.Servers) {
		panic("Consumer '" + name + "' has one or more improperly formatted servers (must be host:port)")
	}

	// Set defaults for configs if needed
	if module.App.Configuration.Consumer[module.name].OffsetsTopic == "" {
		module.App.Configuration.Consumer[module.name].OffsetsTopic = "__consumer_offsets"
	}

	if module.App.Configuration.Consumer[module.name].GroupWhitelist != "" {
		re, err := regexp.Compile(module.App.Configuration.Consumer[module.name].GroupWhitelist)
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
	client, err := sarama.NewClient(module.myConfiguration.Servers, module.saramaConfig)
	if err != nil {
		return err
	}

	err = module.startKafkaConsumer(&helpers.BurrowSaramaClient{Client: client})
	if err != nil {
		client.Close()
		return err
	}

	// Start main loop
	go module.mainLoop()

	return nil
}

func (module *KafkaClient) Stop() error {
	module.Log.Info("stopping")

	// We use 2 quit channels so that the consumers don't block on shutdown because the mainLoop went down first
	close(module.consumerQuitChannel)
	module.consumerRunning.Wait()

	close(module.quitChannel)
	module.running.Wait()

	return nil
}

func (module *KafkaClient) mainLoop() {
	module.running.Add(1)
	defer module.running.Done()

	for {
		select {
		case msg := <-module.messageChannel:
			go module.processConsumerOffsetsMessage(msg)
		case <-module.quitChannel:
			return
		}
	}
}

func (module *KafkaClient) partitionConsumer(consumer sarama.PartitionConsumer) {
	module.consumerRunning.Add(1)
	defer module.consumerRunning.Done()
	defer consumer.AsyncClose()

	for {
		select {
		case msg := <-consumer.Messages():
			module.messageChannel <- msg
		case err := <-consumer.Errors():
			module.Log.Error("consume error",
				zap.String("topic", err.Topic),
				zap.Int32("partition", err.Partition),
				zap.String("error", err.Err.Error()),
			)
		case <-module.consumerQuitChannel:
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
	partitions, err := client.Partitions(module.myConfiguration.OffsetsTopic)
	if err != nil {
		module.Log.Error("failed to get partition count",
			zap.String("topic", module.myConfiguration.OffsetsTopic),
			zap.String("error", err.Error()),
		)
		client.Close()
		return err
	}

	// Default to bootstrapping the offsets topic, unless configured otherwise
	startFrom := sarama.OffsetOldest
	if module.myConfiguration.StartLatest {
		startFrom = sarama.OffsetNewest
	}

	// Start consumers for each partition with fan in
	module.Log.Info("starting consumers",
		zap.String("topic", module.myConfiguration.OffsetsTopic),
		zap.Int("count", len(partitions)),
	)
	for i, partition := range partitions {
		pconsumer, err := consumer.ConsumePartition(module.myConfiguration.OffsetsTopic, partition, startFrom)
		if err != nil {
			module.Log.Error("failed to consume partition",
				zap.String("topic", module.myConfiguration.OffsetsTopic),
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
		zap.Int64("offset", msg.Offset),
	)

	var keyver uint16
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
			zap.Uint16("version", keyver),
		)
	}
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
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
	Partition uint32
	ErrorAt   string
}
type OffsetValue struct {
	Offset    uint64
	Timestamp uint64
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
			zap.Uint32("partition", offsetKey.Partition),
			zap.String("reason", errorAt),
		)
		return
	}

	offsetLogger := logger.With(
		zap.String("message_type", "offset"),
		zap.String("group", offsetKey.Group),
		zap.String("topic", offsetKey.Topic),
		zap.Uint32("partition", offsetKey.Partition),
	)

	if !module.acceptConsumerGroup(offsetKey.Group) {
		offsetLogger.Debug("dropped", zap.String("reason", "whitelist"))
		return
	}

	var valueVersion uint16
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
			zap.Uint16("version", valueVersion),
		)
	}
}

func (module *KafkaClient) decodeAndSendOffset(offsetKey OffsetKey, valueBuffer *bytes.Buffer, logger *zap.Logger) {
	offsetValue, errorAt := decodeOffsetValueV0(valueBuffer)
	if errorAt != "" {
		logger.Warn("failed to decode",
			zap.Uint64("offset", offsetValue.Offset),
			zap.Uint64("timestamp", offsetValue.Timestamp),
			zap.String("reason", errorAt),
		)
		return
	}

	partitionOffset := &protocol.StorageRequest{
		RequestType: protocol.StorageSetConsumerOffset,
		Cluster:     module.myConfiguration.Cluster,
		Topic:       offsetKey.Topic,
		Partition:   int32(offsetKey.Partition),
		Group:       offsetKey.Group,
		Timestamp:   int64(offsetValue.Timestamp),
		Offset:      int64(offsetValue.Offset),
	}
	logger.Debug("consumer offset",
		zap.Uint64("offset", offsetValue.Offset),
		zap.Uint64("timestamp", offsetValue.Timestamp),
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

	var valueVersion uint16
	valueBuffer := bytes.NewBuffer(value)
	err = binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		logger.Warn("failed to decode",
			zap.String("message_type", "metadata"),
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
			zap.Uint16("version", valueVersion),
		)
	}
}

func (module *KafkaClient) decodeAndSendGroupMetadata(valueVersion uint16, group string, valueBuffer *bytes.Buffer, logger *zap.Logger) {
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

	var memberCount uint32
	err := binary.Read(valueBuffer, binary.BigEndian, &memberCount)
	if err != nil {
		metadataLogger.Warn("failed to decode",
			zap.String("reason", "no member size"),
		)
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
					Cluster:     module.myConfiguration.Cluster,
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

func decodeMetadataMember(buf *bytes.Buffer, memberVersion uint16) (MetadataMember, string) {
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

	var subscriptionBytes uint32
	err = binary.Read(buf, binary.BigEndian, &subscriptionBytes)
	if err != nil {
		return memberMetadata, "subscription_bytes"
	}
	buf.Next(int(subscriptionBytes))

	var assignmentBytes uint32
	err = binary.Read(buf, binary.BigEndian, &assignmentBytes)
	if err != nil {
		return memberMetadata, "assignment_bytes"
	}

	var consumerProtocolVersion uint16
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

	return memberMetadata, ""
}

func decodeMemberAssignmentV0(buf *bytes.Buffer) (map[string][]int32, string) {
	var err error
	var topics map[string][]int32
	var numTopics, numPartitions, partitionId uint32

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
