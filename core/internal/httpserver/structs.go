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

import "github.com/linkedin/Burrow/core/protocol"

type logLevelRequest struct {
	Level string `json:"level"`
}

type httpResponseLogLevel struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Level   string                  `json:"level"`
	Request httpResponseRequestInfo `json:"request"`
}

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

type httpResponseConfigGeneral struct {
	PIDFile                  string `json:"pidfile"`
	StdoutLogfile            string `json:"stdout-logfile"`
	AccessControlAllowOrigin string `json:"access-control-allow-origin"`
}
type httpResponseConfigLogging struct {
	Filename       string `json:"filename"`
	MaxSize        int    `json:"max-size"`
	MaxBackups     int    `json:"max-backups"`
	MaxAge         int    `json:"max-age"`
	UseLocalTime   bool   `json:"use-local-time"`
	UseCompression bool   `json:"use-compression"`
	Level          string `json:"level"`
}
type httpResponseConfigZookeeper struct {
	Servers  []string `json:"servers"`
	Timeout  int      `json:"timeout"`
	RootPath string   `json:"root-path"`
}
type httpResponseConfigHTTPServer struct {
	Address string `json:"address"`
	TLS     string `json:"tls"`
	Timeout int    `json:"timeout"`
}
type httpResponseConfigMain struct {
	Error      bool                                    `json:"error"`
	Message    string                                  `json:"message"`
	Request    httpResponseRequestInfo                 `json:"request"`
	General    httpResponseConfigGeneral               `json:"general"`
	Logging    httpResponseConfigLogging               `json:"logging"`
	Zookeeper  httpResponseConfigZookeeper             `json:"zookeeper"`
	HTTPServer map[string]httpResponseConfigHTTPServer `json:"httpserver"`
}

type httpResponseConfigModuleList struct {
	Error       bool                    `json:"error"`
	Message     string                  `json:"message"`
	Request     httpResponseRequestInfo `json:"request"`
	Coordinator string                  `json:"coordinator"`
	Modules     []string                `json:"modules"`
}
type httpResponseConfigModuleDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Module  interface{}             `json:"module"`
	Request httpResponseRequestInfo `json:"request"`
}

type httpResponseConfigModuleStorage struct {
	ClassName      string `json:"class-name"`
	Intervals      int    `json:"intervals"`
	MinDistance    int64  `json:"min-distance"`
	GroupWhitelist string `json:"group-whitelist"`
	ExpireGroup    int64  `json:"expire-group"`
}

type httpResponseConfigModuleCluster struct {
	ClassName     string                    `json:"class-name"`
	Servers       []string                  `json:"servers"`
	ClientProfile httpResponseClientProfile `json:"client-profile"`
	TopicRefresh  int64                     `json:"topic-refresh"`
	OffsetRefresh int64                     `json:"offset-refresh"`
}

type httpResponseConfigModuleConsumer struct {
	ClassName        string                    `json:"class-name"`
	Cluster          string                    `json:"cluster"`
	Servers          []string                  `json:"servers"`
	GroupWhitelist   string                    `json:"group-whitelist"`
	ZookeeperPath    string                    `json:"zookeeper-path"`
	ZookeeperTimeout int32                     `json:"zookeeper-timeout"`
	ClientProfile    httpResponseClientProfile `json:"client-profile"`
	OffsetsTopic     string                    `json:"offsets-topic"`
	StartLatest      bool                      `json:"start-latest"`
}

type httpResponseConfigModuleEvaluator struct {
	ClassName   string `json:"class-name"`
	ExpireCache int64  `json:"expire-cache"`
}

type httpResponseConfigModuleNotifierHTTP struct {
	ClassName      string            `json:"class-name"`
	GroupWhitelist string            `json:"group-whitelist"`
	Interval       int64             `json:"interval"`
	Threshold      int               `json:"threshold"`
	Timeout        int               `json:"timeout"`
	Keepalive      int               `json:"keepalive"`
	URLOpen        string            `json:"url-open"`
	URLClose       string            `json:"url-close"`
	MethodOpen     string            `json:"method-open"`
	MethodClose    string            `json:"method-close"`
	TemplateOpen   string            `json:"template-open"`
	TemplateClose  string            `json:"template-close"`
	Extras         map[string]string `json:"extra"`
	SendClose      bool              `json:"send-close"`
	ExtraCa        string            `json:"extra-ca"`
	NoVerify       string            `json:"noverify"`
}

type httpResponseConfigModuleNotifierSlack struct {
	ClassName      string            `json:"class-name"`
	GroupWhitelist string            `json:"group-whitelist"`
	Interval       int64             `json:"interval"`
	Threshold      int               `json:"threshold"`
	Timeout        int               `json:"timeout"`
	Keepalive      int               `json:"keepalive"`
	TemplateOpen   string            `json:"template-open"`
	TemplateClose  string            `json:"template-close"`
	Extras         map[string]string `json:"extra"`
	SendClose      bool              `json:"send-close"`
	Channel        string            `json:"channel"`
	Username       string            `json:"username"`
	IconURL        string            `json:"icon-url"`
	IconEmoji      string            `json:"icon-emoji"`
}

type httpResponseConfigModuleNotifierEmail struct {
	ClassName      string            `json:"class-name"`
	GroupWhitelist string            `json:"group-whitelist"`
	Interval       int64             `json:"interval"`
	Threshold      int               `json:"threshold"`
	TemplateOpen   string            `json:"template-open"`
	TemplateClose  string            `json:"template-close"`
	Extras         map[string]string `json:"extra"`
	SendClose      bool              `json:"send-close"`
	Server         string            `json:"server"`
	Port           int               `json:"port"`
	AuthType       string            `json:"auth-type"`
	Username       string            `json:"username"`
	From           string            `json:"from"`
	To             string            `json:"to"`
	ExtraCa        string            `json:"extra-ca"`
	NoVerify       string            `json:"noverify"`
}

type httpResponseConfigModuleNotifierNull struct {
	ClassName      string            `json:"class-name"`
	GroupWhitelist string            `json:"group-whitelist"`
	Interval       int64             `json:"interval"`
	Threshold      int               `json:"threshold"`
	TemplateOpen   string            `json:"template-open"`
	TemplateClose  string            `json:"template-close"`
	Extras         map[string]string `json:"extra"`
	SendClose      bool              `json:"send-close"`
}
