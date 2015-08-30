/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"code.google.com/p/gcfg"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
)

// Configuration definition
type BurrowConfig struct {
	General struct {
		LogDir         string `gcfg:"logdir"`
		LogConfig      string `gcfg:"logconfig"`
		PIDFile        string `gcfg:"pidfile"`
		ClientID       string `gcfg:"client-id"`
		GroupBlacklist string `gcfg:"group-blacklist"`
	}
	Zookeeper struct {
		Hosts    []string `gcfg:"hostname"`
		Port     int      `gcfg:"port"`
		Timeout  int      `gcfg:"timeout"`
		LockPath string   `gcfg:"lock-path"`
	}
	Kafka map[string]*struct {
		Brokers       []string `gcfg:"broker"`
		BrokerPort    int      `gcfg:"broker-port"`
		Zookeepers    []string `gcfg:"zookeeper"`
		ZookeeperPort int      `gcfg:"zookeeper-port"`
		ZookeeperPath string   `gcfg:"zookeeper-path"`
		OffsetsTopic  string   `gcfg:"offsets-topic"`
		ZookeeperOffsetPaths	[]string	`gcfg:"zookeeper-offsets-path"`
		StormOffsetPaths		[]string	`gcfg:"storm-offsets-path"`
	}
	Tickers struct {
		BrokerOffsets int `gcfg:"broker-offsets"`
		ZooKeeperOffsets int `gcfg:"zookeeper-offsets"`
		StormOffsets int `gcfg:"storm-offsets"`
	}
	Lagcheck struct {
		Intervals   int   `gcfg:"intervals"`
		ExpireGroup int64 `gcfg:"expire-group"`
	}
	Httpserver struct {
		Enable bool `gcfg:"server"`
		Port   int  `gcfg:"port"`
	}
	Smtp struct {
		Server   string `gcfg:"server"`
		Port     int    `gcfg:"port"`
		AuthType string `gcfg:"auth-type"`
		Username string `gcfg:"username"`
		Password string `gcfg:"password"`
		From     string `gcfg:"from"`
		Template string `gcfg:"template"`
	}
	Email map[string]*struct {
		Groups    []string `gcfg:"group"`
		Interval  int      `gcfg:"interval"`
		Threshold string   `gcfg:"threhsold"`
	}
	Httpnotifier struct {
		Url            string   `gcfg:"url"`
		Interval       int      `gcfg:"interval"`
		Extras         []string `gcfg:"extra"`
		TemplatePost   string   `gcfg:"template-post"`
		TemplateDelete string   `gcfg:"template-delete"`
	}
}

func ReadConfig(cfgFile string) *BurrowConfig {
	var cfg BurrowConfig
	err := gcfg.ReadFileInto(&cfg, cfgFile)
	if err != nil {
		log.Fatalf("Failed to parse gcfg data: %s", err)
		os.Exit(1)
	}
	return &cfg
}

// Validate that the config is complete
// For a couple values, this will set reasonable defaults for missing values
func ValidateConfig(app *ApplicationContext) error {
	if (app == nil) || (app.Config == nil) {
		return errors.New("Application context or configuration struct are nil")
	}
	errs := make([]string, 0)

	// General
	if app.Config.General.LogDir == "" {
		app.Config.General.LogDir, _ = os.Getwd()
	}
	if _, err := os.Stat(app.Config.General.LogDir); os.IsNotExist(err) {
		errs = append(errs, "Log directory does not exist")
	}
	if app.Config.General.LogConfig != "" {
		if _, err := os.Stat(app.Config.General.LogConfig); os.IsNotExist(err) {
			errs = append(errs, "Log configuration file does not exist")
		}
	}
	if app.Config.General.PIDFile == "" {
		app.Config.General.PIDFile = "burrow.pid"
	} else {
		if !validateFilename(app.Config.General.PIDFile) {
			errs = append(errs, "PID filename is invalid")
		}
	}
	if app.Config.General.ClientID == "" {
		app.Config.General.ClientID = "burrow-client"
	} else {
		if !validateTopic(app.Config.General.ClientID) {
			errs = append(errs, "Kafka client ID is not valid")
		}
	}

	// Zookeeper
	if len(app.Config.Zookeeper.Hosts) == 0 {
		errs = append(errs, "No Zookeeper hostnames specified")
	} else {
		for _, host := range app.Config.Zookeeper.Hosts {
			if !validateHostname(host) {
				errs = append(errs, "One or more Zookeeper hostnames are invalid")
				break
			}
		}
	}
	if app.Config.Zookeeper.Port == 0 {
		errs = append(errs, "Zookeeper port is not specified")
	}
	if app.Config.Zookeeper.Timeout == 0 {
		app.Config.Zookeeper.Timeout = 6
	}
	if app.Config.Zookeeper.LockPath == "" {
		app.Config.Zookeeper.LockPath = "/burrow/notifier"
	} else {
		if !validateZookeeperPath(app.Config.Zookeeper.LockPath) {
			errs = append(errs, "Zookeeper path is not valid")
		}
	}

	// Kafka Clusters
	if len(app.Config.Kafka) == 0 {
		errs = append(errs, "No Kafka clusters are configured")
	}
	for cluster, cfg := range app.Config.Kafka {
		if len(cfg.Brokers) == 0 {
			errs = append(errs, fmt.Sprintf("No Kafka brokers specified for cluster %s", cluster))
		} else {
			for _, host := range cfg.Brokers {
				if !validateHostname(host) {
					errs = append(errs, fmt.Sprintf("One or more Kafka brokers is invalid for cluster %s", cluster))
					break
				}
			}
		}
		if cfg.ZookeeperPort == 0 {
			errs = append(errs, fmt.Sprintf("Kafka port is not specified for cluster %s", cluster))
		}
		if len(cfg.Zookeepers) == 0 {
			errs = append(errs, fmt.Sprintf("No Zookeeper hosts specified for cluster %s", cluster))
		} else {
			for _, host := range cfg.Zookeepers {
				if !validateHostname(host) {
					errs = append(errs, fmt.Sprintf("One or more Zookeeper hosts is invalid for cluster %s", cluster))
					break
				}
			}
		}
		if cfg.ZookeeperPort == 0 {
			errs = append(errs, fmt.Sprintf("Zookeeper port is not specified for cluster %s", cluster))
		}
		if cfg.ZookeeperPath == "" {
			errs = append(errs, fmt.Sprintf("Zookeeper path is not specified for cluster %s", cluster))
		} else {
			if !validateZookeeperPath(cfg.ZookeeperPath) {
				errs = append(errs, fmt.Sprintf("Zookeeper path is not valid for cluster %s", cluster))
			}
		}
		if cfg.OffsetsTopic == "" {
			cfg.OffsetsTopic = "__consumer_offsets"
		} else {
			if !validateTopic(cfg.OffsetsTopic) {
				errs = append(errs, fmt.Sprintf("Kafka offsets topic is not valid for cluster %s", cluster))
			}
		}
	}

	// Tickers
	if app.Config.Tickers.BrokerOffsets == 0 {
		app.Config.Tickers.BrokerOffsets = 60
	}

	// Intervals
	if app.Config.Lagcheck.Intervals == 0 {
		app.Config.Lagcheck.Intervals = 10
	}
	if app.Config.Lagcheck.ExpireGroup == 0 {
		app.Config.Lagcheck.ExpireGroup = 604800
	}

	// HTTP Server
	if app.Config.Httpserver.Enable {
		if app.Config.Httpserver.Port == 0 {
			errs = append(errs, "HTTP server port is not specified")
		}
	}

	// SMTP server config
	if app.Config.Smtp.Server != "" {
		if !validateHostname(app.Config.Smtp.Server) {
			errs = append(errs, "SMTP server is invalid")
		}
		if app.Config.Smtp.Port == 0 {
			app.Config.Smtp.Port = 25
		}
		if app.Config.Smtp.From == "" {
			errs = append(errs, "Email from address is not defined")
		} else {
			if !validateEmail(app.Config.Smtp.From) {
				errs = append(errs, "Email from address is invalid")
			}
		}
		if app.Config.Smtp.Template == "" {
			app.Config.Smtp.Template = "config/default-email.tmpl"
		}
		if _, err := os.Stat(app.Config.Smtp.Template); os.IsNotExist(err) {
			errs = append(errs, "Email template file does not exist")
		}
		if app.Config.Smtp.AuthType != "" {
			if (app.Config.Smtp.AuthType != "plain") && (app.Config.Smtp.AuthType != "crammd5") {
				errs = append(errs, "Email auth-type must be plain, crammd5, or blank")
			}
		}
		// Username and password are not validated - they're optional

		// Email configs
		for email, cfg := range app.Config.Email {
			if !validateEmail(email) {
				errs = append(errs, "Email address is invalid")
			}
			if len(cfg.Groups) == 0 {
				errs = append(errs, "Email notification configured with no groups")
			} else {
				for _, group := range cfg.Groups {
					groupParts := strings.Split(group, ",")
					if len(groupParts) != 2 {
						errs = append(errs, "Email notification groups must be specified as 'cluster,groupname'")
						break
					}
					if !validateTopic(groupParts[1]) {
						errs = append(errs, "One or more email notification groups are invalid")
						break
					}
				}
			}
			if cfg.Interval == 0 {
				errs = append(errs, "Email notification interval is not specified")
			}
			if cfg.Threshold == "" {
				cfg.Threshold = "WARNING"
			} else {
				// Upcase the threshold
				cfg.Threshold = strings.ToUpper(cfg.Threshold)
				if (cfg.Threshold != "WARNING") && (cfg.Threshold != "ERROR") {
					errs = append(errs, "Email notification threshold is invalid (must be WARNING or ERROR)")
				}
			}
		}
	} else {
		if len(app.Config.Email) > 0 {
			errs = append(errs, "Email notifications are configured, but SMTP server is not configured")
		}
	}

	// HTTP Notifier config
	if app.Config.Httpnotifier.Url != "" {
		if !validateUrl(app.Config.Httpnotifier.Url) {
			errs = append(errs, "HTTP notifier URL is invalid")
		}
		if app.Config.Httpnotifier.TemplatePost == "" {
			app.Config.Httpnotifier.TemplatePost = "config/default-http-post.tmpl"
		}
		if _, err := os.Stat(app.Config.Httpnotifier.TemplatePost); os.IsNotExist(err) {
			errs = append(errs, "HTTP notifier POST template file does not exist")
		}
		if app.Config.Httpnotifier.TemplateDelete == "" {
			app.Config.Httpnotifier.TemplateDelete = "config/default-http-delete.tmpl"
		}
		if _, err := os.Stat(app.Config.Httpnotifier.TemplateDelete); os.IsNotExist(err) {
			errs = append(errs, "HTTP notifier DELETE template file does not exist")
		}
		if app.Config.Httpnotifier.Interval == 0 {
			app.Config.Httpnotifier.Interval = 60
		}
		for _, extra := range app.Config.Httpnotifier.Extras {
			// Each extra should be formatted as "string=string"
			if matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\-]+=.*$`, extra); !matches {
				errs = append(errs, "One or more HTTP notifier extra fields are invalid")
				break
			}
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ". ") + ".")
	} else {
		return nil
	}
}

func validateIP(ipaddr string) bool {
	addr := net.ParseIP(ipaddr)
	return addr != nil
}

func validateHostname(hostname string) bool {
	matches, _ := regexp.MatchString(`^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*$`, hostname)
	if !matches {
		// Try as an IP address
		return validateIP(hostname)
	}
	return matches
}

func validateZookeeperPath(path string) bool {
	parts := strings.Split(path, "/")
	if (len(parts) < 2) || (parts[0] != "") {
		return false
	}
	if (len(parts) == 2) && (parts[1] == "") {
		// Root node is OK
		return true
	}
	for i, node := range parts {
		if i == 0 {
			continue
		}
		matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\-][a-zA-Z0-9_\-\.]*$`, node)
		if !matches {
			return false
		}
	}
	return true
}

func validateTopic(topic string) bool {
	matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\-]+$`, topic)
	return matches
}

// We don't match valid filenames, we match decent filenames
func validateFilename(filename string) bool {
	matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\.-]+$`, filename)
	return matches
}

// Very simplistic email address validator - just looks for an @ and a .
func validateEmail(email string) bool {
	matches, _ := regexp.MatchString(`^.+@.+\..+$`, email)
	return matches
}

// Just use the golang Url library for this
func validateUrl(rawUrl string) bool {
	_, err := url.Parse(rawUrl)
	return err == nil
}
