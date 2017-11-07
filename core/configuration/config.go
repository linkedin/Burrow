/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package configuration

import (
	"errors"
	"log"
	"os"
	"strings"

	"gopkg.in/gcfg.v1"
)

// Configuration definition
type ClientProfile struct {
	ClientID        string `gcfg:"client-id"`
	KafkaVersion    string `gcfg:"kafka-version"`
	TLS             bool   `gcfg:"tls"`
	TLSNoVerify     bool   `gcfg:"tls-noverify"`
	TLSCertFilePath string `gcfg:"tls-certfilepath"`
	TLSKeyFilePath  string `gcfg:"tls-keyfilepath"`
	TLSCAFilePath   string `gcfg:"tls-cafilepath"`
	SASL            bool   `gcfg:"sasl"`
	HandshakeFirst  bool   `gcfg:"handshake-first"`
	Username        string `gcfg:"username"`
	Password        string `gcfg:"password"`
}
type HttpNotifierProfile struct {
	UrlOpen        string   `gcfg:"url-open"`
	UrlClose       string   `gcfg:"url-close"`
	MethodOpen     string   `gcfg:"method-open"`
	MethodClose    string   `gcfg:"method-close"`
}
type SlackNotifierProfile struct {
	Token     string   `gcfg:"token"`
	Channel   string   `gcfg:"channel"`
	Username  string   `gcfg:"username"`
	IconUrl   string   `gcfg:"icon-url"`
	IconEmoji string   `gfcg:"icon-emoji"`
}
type EmailNotifierProfile struct {
	Server   string `gcfg:"server"`
	Port     int    `gcfg:"port"`
	AuthType string `gcfg:"auth-type"`
	Username string `gcfg:"username"`
	Password string `gcfg:"password"`
	From     string `gcfg:"from"`
	To       string `gcfg:"to"`
}

type StorageConfig struct {
	ClassName      string `gcfg:"class-name"`
	Intervals      int    `gcfg:"intervals"`
	MinDistance    int64  `gcfg:"min-distance"`
	GroupWhitelist string `gcfg:"group-whitelist"`
	ExpireGroup    int64  `gcfg:"expire-group"`
}
type ConsumerConfig struct {
	ClassName        string   `gcfg:"class-name"`
	Cluster          string   `gcfg:"cluster"`
	Servers          []string `gcfg:"server"`
	GroupWhitelist   string   `gcfg:"group-whitelist"`

	// kafka_zk options
	ZookeeperPath    string   `gcfg:"zookeeper-path"`
	ZookeeperTimeout int32    `gcfg:"zookeeper-timeout"`

	// kafka_client options
	ClientProfile    string   `gcfg:"client-profile"`
	OffsetsTopic     string   `gcfg:"offsets-topic"`
	StartLatest      bool     `gcfg:"start-latest"`
}
type ClusterConfig struct {
	ClassName     string   `gcfg:"class-name"`
	Servers       []string `gcfg:"server"`
	ClientProfile string   `gcfg:"client-profile"`
	TopicRefresh  int64    `gcfg:"topic-refresh"`
	OffsetRefresh int64    `gcfg:"offset-refresh"`
}
type EvaluatorConfig struct {
	ClassName     string `gcfg:"class-name"`
	ExpireCache   int64  `gcfg:"expire-cache"`
}
type NotifierConfig struct {
	ClassName      string   `gcfg:"class-name"`
	GroupWhitelist string   `gcfg:"group-whitelist"`
	Interval       int64    `gcfg:"interval"`
	Threshold      int      `gcfg:"threshold"`

	Timeout        int      `gcfg:"timeout"`
	Keepalive      int      `gcfg:"keepalive"`

	Profile        string   `gcfg:"profile"`
	TemplateOpen   string   `gcfg:"template-open"`
	TemplateClose  string   `gcfg:"template-close"`
	Extras         []string `gcfg:"extra"`
	SendClose      bool     `gcfg:"send-close"`
}

type HttpServerConfig struct {
	Address         string `gcfg:"address"`
	TLS             bool   `gcfg:"tls"`
	TLSCertFilePath string `gcfg:"tls-certfilepath"`
	TLSKeyFilePath  string `gcfg:"tls-keyfilepath"`
	TLSCAFilePath   string `gcfg:"tls-cafilepath"`
	Timeout         int    `gcfg:"timeout"`
}

type Configuration struct {
	General struct {
		PIDFile        string `gcfg:"pidfile"`
		StdoutLogfile  string `gcfg:"stdout-logfile"`
	}
	Logging struct {
		Filename       string `gcfg:"filename"`
		MaxSize        int    `gcfg:"max-size"`
		MaxBackups     int    `gcfg:"max-backups"`
		MaxAge         int    `gcfg:"max-age"`
		UseLocalTime   bool   `gcfg:"use-local-time"`
		UseCompression bool   `gcfg:"use-compression"`
		Level          string `gcfg:"level"`
	}
	Zookeeper struct {
		Server   []string `gcfg:"server"`
		Timeout  int      `gcfg:"timeout"`
		RootPath string   `gcfg:"root-path"`
	}
	HttpServer map[string]*HttpServerConfig

	// These are profiles used in modules to provide more configuration detail that could be shared
	ClientProfile        map[string]*ClientProfile
	HttpNotifierProfile  map[string]*HttpNotifierProfile
	SlackNotifierProfile map[string]*SlackNotifierProfile
	EmailNotifierProfile map[string]*EmailNotifierProfile

	// These are the module configurations. They're broken out so we can use them in the modules separately
	Storage             map[string]*StorageConfig
	Consumer            map[string]*ConsumerConfig
	Cluster             map[string]*ClusterConfig
	Evaluator           map[string]*EvaluatorConfig
	Notifier            map[string]*NotifierConfig
}

func ReadConfig(cfgFile string) *Configuration {
	var cfg Configuration

	err := gcfg.ReadFileInto(&cfg, cfgFile)
	if err != nil {
		log.Fatalf("Failed to parse gcfg data: %s", err)
		os.Exit(1)
	}
	return &cfg
}

// Validate that the config is complete for the basic sections, and set defaults where values are missing
// Modules are expected to validate their own configs on load
func ValidateConfig(config *Configuration) error {
	if config == nil {
		return errors.New("configuration struct is nil")
	}
	errs := make([]string, 0)

	// General
	if config.General.PIDFile == "" {
		config.General.PIDFile = "burrow.pid"
	} else {
		if !ValidateFilename(config.General.PIDFile) {
			errs = append(errs, "PID filename is invalid")
		}
	}

	// Logging
	if config.Logging.Filename != "" {
		if config.Logging.MaxSize == 0 {
			config.Logging.MaxSize = 100
		}
		if config.Logging.MaxBackups == 0 {
			config.Logging.MaxBackups = 10
		}
		if config.Logging.MaxAge == 0 {
			config.Logging.MaxAge = 30
		}
	}

	// Zookeeper
	if len(config.Zookeeper.Server) == 0 {
		errs = append(errs, "No Zookeeper servers specified")
	} else {
		if ! ValidateHostList(config.Zookeeper.Server) {
			errs = append(errs, "Failed to validate Zookeeper servers")
		}
	}
	if config.Zookeeper.Timeout == 0 {
		config.Zookeeper.Timeout = 6
	}
	if config.Zookeeper.RootPath == "" {
		config.Zookeeper.RootPath = "/burrow"
	} else {
		if !ValidateZookeeperPath(config.Zookeeper.RootPath) {
			errs = append(errs, "Zookeeper root path is not valid")
		}
	}

	// Assure we have a default ClientProfile and create it if not
	if _, ok := config.ClientProfile[""]; !ok {
		config.ClientProfile[""] = &ClientProfile{
			ClientID:     "burrow-lagchecker",
			KafkaVersion: "0.8.2",
		}
	}

	// Module configurations and profiles are not validated here. Each module is expected to check their own config in
	// their Configure method. They should also set up default profile information (if defaults are allowed)

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ". ") + ".")
	} else {
		return nil
	}
}
