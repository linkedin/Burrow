/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package httpserver

import "github.com/linkedin/Burrow/protocol"

type httpResponseRequestInfo struct {
	URI  string `json:"url"`
	Host string `json:"host"`
}

type httpResponseError struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Request httpResponseRequestInfo `json:"request"`
}

type httpResponseTLSProfile struct {
	Name     string `json:"name"`
	NoVerify bool   `json:"noverify"`
	CertFile string `json:"certfile"`
	KeyFile  string `json:"keyfile"`
	CAFile   string `json:"cafile"`
}

type httpResponseSASLProfile struct {
	Name           string `json:"name"`
	HandshakeFirst bool   `json:"handshake-first"`
	Username       string `json:"username"`
}

type httpResponseClientProfile struct {
	Name         string                   `json:"name"`
	ClientID     string                   `json:"client-id"`
	KafkaVersion string                   `json:"kafka-version"`
	TLS          *httpResponseTLSProfile  `json:"tls"`
	SASL         *httpResponseSASLProfile `json:"sasl"`
}

type httpResponseClusterList struct {
	Error    bool                    `json:"error"`
	Message  string                  `json:"message"`
	Clusters []string                `json:"clusters"`
	Request  httpResponseRequestInfo `json:"request"`
}

type httpResponseTopicList struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Topics  []string                `json:"topics"`
	Request httpResponseRequestInfo `json:"request"`
}

type httpResponseTopicDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Offsets []int64                 `json:"offsets"`
	Request httpResponseRequestInfo `json:"request"`
}

type httpResponseTopicConsumerDetail struct {
	Error     bool                    `json:"error"`
	Message   string                  `json:"message"`
	Consumers []string                `json:"consumers"`
	Request   httpResponseRequestInfo `json:"request"`
}

type httpResponseConsumerList struct {
	Error     bool                    `json:"error"`
	Message   string                  `json:"message"`
	Consumers []string                `json:"consumers"`
	Request   httpResponseRequestInfo `json:"request"`
}

type httpResponseConsumerDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Topics  protocol.ConsumerTopics `json:"topics"`
	Request httpResponseRequestInfo `json:"request"`
}

type httpResponseConsumerStatus struct {
	Error   bool                         `json:"error"`
	Message string                       `json:"message"`
	Status  protocol.ConsumerGroupStatus `json:"status"`
	Request httpResponseRequestInfo      `json:"request"`
}

type httpResponseConfigModuleDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Module  interface{}             `json:"module"`
	Request httpResponseRequestInfo `json:"request"`
}

type httpResponseConfigModuleCluster struct {
	ClassName     string                    `json:"class-name"`
	Servers       []string                  `json:"servers"`
	ClientProfile httpResponseClientProfile `json:"client-profile"`
	TopicRefresh  int64                     `json:"topic-refresh"`
	OffsetRefresh int64                     `json:"offset-refresh"`
}
