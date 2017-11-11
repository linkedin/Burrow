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

type LogLevelRequest struct {
	Level string `json:"level"`
}

type HTTPResponseLogLevel struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Level   string                  `json:"level"`
	Request HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseRequestInfo struct {
	URI  string `json:"url"`
	Host string `json:"host"`
}

type HTTPResponseError struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Request HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseTLSProfile struct {
	Name     string `json:"name"`
	NoVerify bool   `json:"noverify"`
	CertFile string `json:"certfile"`
	KeyFile  string `json:"keyfile"`
	CAFile   string `json:"cafile"`
}

type HTTPResponseSASLProfile struct {
	Name           string `json:"name"`
	HandshakeFirst bool   `json:"handshake-first"`
	Username       string `json:"username"`
}

type HTTPResponseClientProfile struct {
	Name         string                   `json:"name"`
	ClientID     string                   `json:"client-id"`
	KafkaVersion string                   `json:"kafka-version"`
	TLS          *HTTPResponseTLSProfile  `json:"tls"`
	SASL         *HTTPResponseSASLProfile `json:"tls"`
}

type HTTPResponseClusterList struct {
	Error    bool                    `json:"error"`
	Message  string                  `json:"message"`
	Clusters []string                `json:"clusters"`
	Request  HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseTopicList struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Topics  []string                `json:"topics"`
	Request HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseTopicDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Offsets []int64                 `json:"offsets"`
	Request HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseConsumerList struct {
	Error     bool                    `json:"error"`
	Message   string                  `json:"message"`
	Consumers []string                `json:"consumers"`
	Request   HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseConsumerDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Topics  protocol.ConsumerTopics `json:"topics"`
	Request HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseConsumerStatus struct {
	Error   bool                         `json:"error"`
	Message string                       `json:"message"`
	Status  protocol.ConsumerGroupStatus `json:"status"`
	Request HTTPResponseRequestInfo      `json:"request"`
}

type HTTPResponseConfigGeneral struct {
	PIDFile                  string `json:"pidfile"`
	StdoutLogfile            string `json:"stdout-logfile"`
	AccessControlAllowOrigin string `json:"access-control-allow-origin"`
}
type HTTPResponseConfigLogging struct {
	Filename       string `json:"filename"`
	MaxSize        int    `json:"max-size"`
	MaxBackups     int    `json:"max-backups"`
	MaxAge         int    `json:"max-age"`
	UseLocalTime   bool   `json:"use-local-time"`
	UseCompression bool   `json:"use-compression"`
	Level          string `json:"level"`
}
type HTTPResponseConfigZookeeper struct {
	Servers  []string `json:"servers"`
	Timeout  int      `json:"timeout"`
	RootPath string   `json:"root-path"`
}
type HTTPResponseConfigHttpServer struct {
	Address string `json:"address"`
	TLS     string `json:"tls"`
	Timeout int    `json:"timeout"`
}
type HTTPResponseConfigMain struct {
	Error      bool                                    `json:"error"`
	Message    string                                  `json:"message"`
	Request    HTTPResponseRequestInfo                 `json:"request"`
	General    HTTPResponseConfigGeneral               `json:"general"`
	Logging    HTTPResponseConfigLogging               `json:"logging"`
	Zookeeper  HTTPResponseConfigZookeeper             `json:"zookeeper"`
	HttpServer map[string]HTTPResponseConfigHttpServer `json:"httpserver"`
}

type HTTPResponseConfigModuleList struct {
	Error       bool                    `json:"error"`
	Message     string                  `json:"message"`
	Request     HTTPResponseRequestInfo `json:"request"`
	Coordinator string                  `json:"coordinator"`
	Modules     []string                `json:"modules"`
}
type HTTPResponseConfigModuleDetail struct {
	Error   bool                    `json:"error"`
	Message string                  `json:"message"`
	Module  interface{}             `json:"module"`
	Request HTTPResponseRequestInfo `json:"request"`
}

type HTTPResponseConfigModuleStorage struct {
	ClassName      string `json:"class-name"`
	Intervals      int    `json:"intervals"`
	MinDistance    int64  `json:"min-distance"`
	GroupWhitelist string `json:"group-whitelist"`
	ExpireGroup    int64  `json:"expire-group"`
}

type HTTPResponseConfigModuleCluster struct {
	ClassName     string                    `json:"class-name"`
	Servers       []string                  `json:"servers"`
	ClientProfile HTTPResponseClientProfile `json:"client-profile"`
	TopicRefresh  int64                     `json:"topic-refresh"`
	OffsetRefresh int64                     `json:"offset-refresh"`
}

type HTTPResponseConfigModuleConsumer struct {
	ClassName        string                    `json:"class-name"`
	Cluster          string                    `json:"cluster"`
	Servers          []string                  `json:"servers"`
	GroupWhitelist   string                    `json:"group-whitelist"`
	ZookeeperPath    string                    `json:"zookeeper-path"`
	ZookeeperTimeout int32                     `json:"zookeeper-timeout"`
	ClientProfile    HTTPResponseClientProfile `json:"client-profile"`
	OffsetsTopic     string                    `json:"offsets-topic"`
	StartLatest      bool                      `json:"start-latest"`
}

type HTTPResponseConfigModuleEvaluator struct {
	ClassName   string `json:"class-name"`
	ExpireCache int64  `json:"expire-cache"`
}

type HTTPResponseConfigModuleNotifierHttp struct {
	ClassName      string   `json:"class-name"`
	GroupWhitelist string   `json:"group-whitelist"`
	Interval       int64    `json:"interval"`
	Threshold      int      `json:"threshold"`
	Timeout        int      `json:"timeout"`
	Keepalive      int      `json:"keepalive"`
	UrlOpen        string   `json:"url-open"`
	UrlClose       string   `json:"url-close"`
	MethodOpen     string   `json:"method-open"`
	MethodClose    string   `json:"method-close"`
	TemplateOpen   string   `json:"template-open"`
	TemplateClose  string   `json:"template-close"`
	Extras         []string `json:"extra"`
	SendClose      bool     `json:"send-close"`
}

type HTTPResponseConfigModuleNotifierSlack struct {
	ClassName      string   `json:"class-name"`
	GroupWhitelist string   `json:"group-whitelist"`
	Interval       int64    `json:"interval"`
	Threshold      int      `json:"threshold"`
	Timeout        int      `json:"timeout"`
	Keepalive      int      `json:"keepalive"`
	TemplateOpen   string   `json:"template-open"`
	TemplateClose  string   `json:"template-close"`
	Extras         []string `json:"extra"`
	SendClose      bool     `json:"send-close"`
	Channel        string   `json:"channel"`
	Username       string   `json:"username"`
	IconUrl        string   `json:"icon-url"`
	IconEmoji      string   `json:"icon-emoji"`
}

type HTTPResponseConfigModuleNotifierEmail struct {
	ClassName      string   `json:"class-name"`
	GroupWhitelist string   `json:"group-whitelist"`
	Interval       int64    `json:"interval"`
	Threshold      int      `json:"threshold"`
	TemplateOpen   string   `json:"template-open"`
	TemplateClose  string   `json:"template-close"`
	Extras         []string `json:"extra"`
	SendClose      bool     `json:"send-close"`
	Server         string   `json:"server"`
	Port           int      `json:"port"`
	AuthType       string   `json:"auth-type"`
	Username       string   `json:"username"`
	From           string   `json:"from"`
	To             string   `json:"to"`
}

type HTTPResponseConfigModuleNotifierNull struct {
	ClassName      string   `json:"class-name"`
	GroupWhitelist string   `json:"group-whitelist"`
	Interval       int64    `json:"interval"`
	Threshold      int      `json:"threshold"`
	TemplateOpen   string   `json:"template-open"`
	TemplateClose  string   `json:"template-close"`
	Extras         []string `json:"extra"`
	SendClose      bool     `json:"send-close"`
}
