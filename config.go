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
	"errors"
	"fmt"
	"gopkg.in/gcfg.v1"
	"log"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Configuration definition
type ClientProfile struct {
	ClientID    string `gcfg:"client-id"`
	TLS         bool   `gcfg:"tls"`
	TLSNoVerify bool   `gcfg:"tls-noverify"`
}
type BurrowConfig struct {
	General struct {
		LogDir         string `gcfg:"logdir"`
		LogConfig      string `gcfg:"logconfig"`
		PIDFile        string `gcfg:"pidfile"`
		ClientID       string `gcfg:"client-id"`
		GroupBlacklist string `gcfg:"group-blacklist"`
		GroupWhitelist string `gcfg:"group-whitelist"`
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
		ZKOffsets     bool     `gcfg:"zookeeper-offsets"`
		Clientprofile string   `gcfg:"client-profile"`
	}
	Storm map[string]*struct {
		Zookeepers    []string `gcfg:"zookeeper"`
		ZookeeperPort int      `gcfg:"zookeeper-port"`
		ZookeeperPath string   `gcfg:"zookeeper-path"`
	}
	Tickers struct {
		BrokerOffsets int `gcfg:"broker-offsets"`
	}
	Lagcheck struct {
		Intervals         int   `gcfg:"intervals"`
		MinDistance       int64 `gcfg:"min-distance"`
		ExpireGroup       int64 `gcfg:"expire-group"`
		ZKCheck           int64 `gcfg:"zookeeper-interval"`
		ZKGroupRefresh    int64 `gcfg:"zk-group-refresh"`
		StormCheck        int64 `gcfg:"storm-interval"`
		StormGroupRefresh int64 `gcfg:"storm-group-refresh"`
	}
	Httpserver struct {
		Enable bool `gcfg:"server"`
		Port   int  `gcfg:"port"`
		Listen []string `gcfg:"listen"`
	}
	Notify struct {
		Interval int64 `gcfg:"interval"`
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
	Emailnotifier map[string]*struct {
		Enable    bool     `gcfg:"enable"`
		Groups    []string `gcfg:"group"`
		Interval  int64    `gcfg:"interval"`
		Threshold int      `gcfg:"threshold"`
	}
	Httpnotifier struct {
		Enable         bool     `gcfg:"enable"`
		Groups         []string `gcfg:"group"`
		UrlOpen        string   `gcfg:"url"`
		UrlClose       string   `gcfg:"url-delete"`
		MethodOpen     string   `gcfg:"method"`
		MethodClose    string   `gcfg:"method-delete"`
		Interval       int64    `gcfg:"interval"`
		Extras         []string `gcfg:"extra"`
		TemplateOpen   string   `gcfg:"template-post"`
		TemplateClose  string   `gcfg:"template-delete"`
		SendClose      bool     `gcfg:"send-delete"`
		PostThreshold  int      `gcfg:"post-threshold"`
		Timeout        int      `gcfg:"timeout"`
		Keepalive      int      `gcfg:"keepalive"`
	}
	Slacknotifier struct {
		Enable    bool     `gcfg:"enable"`
		Groups    []string `gcfg:"group"`
		Url       string   `gcfg:"url"`
		Interval  int64    `gcfg:"interval"`
		Channel   string   `gcfg:"channel"`
		Username  string   `gcfg:"username"`
		IconUrl   string   `gcfg:"icon-url"`
		IconEmoji string   `gfcg:"icon-emoji"`
		Threshold int      `gcfg:"threshold"`
		Timeout   int      `gcfg:"timeout"`
		Keepalive int      `gcfg:"keepalive"`
	}
	Clientprofile map[string]*ClientProfile
}

func ReadConfig(cfgFile string) *BurrowConfig {
	var cfg BurrowConfig

	// Set some non-standard defaults
	cfg.Httpnotifier.MethodOpen = "POST"
	cfg.Httpnotifier.SendClose = true
	cfg.Httpnotifier.MethodClose = "DELETE"

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
	if app.Config.Zookeeper.Port == 0 {
		app.Config.Zookeeper.Port = 2181
	}
	if len(app.Config.Zookeeper.Hosts) == 0 {
		errs = append(errs, "No Zookeeper hostnames specified")
	} else {
		hostlistError := checkHostlist(app.Config.Zookeeper.Hosts, app.Config.Zookeeper.Port, "Zookeeper")
		if hostlistError != "" {
			errs = append(errs, hostlistError)
		}
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

	// Kafka Client Profiles
	// Set up a default profile, if needed
	if app.Config.Clientprofile == nil {
		app.Config.Clientprofile = make(map[string]*ClientProfile)
	}
	if _, ok := app.Config.Clientprofile["default"]; !ok {
		app.Config.Clientprofile["default"] = &ClientProfile{
			ClientID: app.Config.General.ClientID,
			TLS:      false,
		}
	}

	for name, cfg := range app.Config.Clientprofile {
		if cfg.ClientID == "" {
			cfg.ClientID = "burrow-client"
		} else {
			if !validateTopic(cfg.ClientID) {
				errs = append(errs, fmt.Sprintf("Kafka client ID is not valid for profile %s", name))
			}
		}
	}

	// Kafka Clusters
	if len(app.Config.Kafka) == 0 {
		errs = append(errs, "No Kafka clusters are configured")
	}
	for cluster, cfg := range app.Config.Kafka {
		if cfg.BrokerPort == 0 {
			cfg.BrokerPort = 9092
		}
		if len(cfg.Brokers) == 0 {
			errs = append(errs, fmt.Sprintf("No Kafka brokers specified for cluster %s", cluster))
		} else {
			hostlistError := checkHostlist(cfg.Brokers, cfg.BrokerPort, "Kafka broker")
			if hostlistError != "" {
				errs = append(errs, hostlistError)
			}
		}
		if cfg.ZookeeperPort == 0 {
			cfg.ZookeeperPort = 2181
		}
		if len(cfg.Zookeepers) == 0 {
			errs = append(errs, fmt.Sprintf("No Zookeeper hosts specified for cluster %s", cluster))
		} else {
			hostlistError := checkHostlist(cfg.Zookeepers, cfg.ZookeeperPort, "Zookeeper")
			if hostlistError != "" {
				errs = append(errs, hostlistError)
			}
		}
		switch cfg.ZookeeperPath {
		case "":
			errs = append(errs, fmt.Sprintf("Zookeeper path is not specified for cluster %s", cluster))
		case "/":
			// If we're using the root path, instead of chroot, set it blank here so we don't get double slashes
			cfg.ZookeeperPath = ""
		default:
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
		if cfg.Clientprofile == "" {
			cfg.Clientprofile = "default"
		} else {
			if _, ok := app.Config.Clientprofile[cfg.Clientprofile]; !ok {
				errs = append(errs, fmt.Sprintf("Kafka client profile is not defined for cluster %s", cluster))
			}
		}
	}

	// Storm Clusters
	if len(app.Config.Storm) > 0 {
		for cluster, cfg := range app.Config.Storm {
			if cfg.ZookeeperPort == 0 {
				cfg.ZookeeperPort = 2181
			}
			if len(cfg.Zookeepers) == 0 {
				errs = append(errs, fmt.Sprintf("No Zookeeper hosts specified for cluster %s", cluster))
			} else {
				hostlistError := checkHostlist(cfg.Zookeepers, cfg.ZookeeperPort, "Zookeeper")
				if hostlistError != "" {
					errs = append(errs, hostlistError)
				}
			}
			if cfg.ZookeeperPath == "" {
				errs = append(errs, fmt.Sprintf("Zookeeper path is not specified for cluster %s", cluster))
			} else {
				if !validateZookeeperPath(cfg.ZookeeperPath) {
					errs = append(errs, fmt.Sprintf("Zookeeper path is not valid for cluster %s", cluster))
				}
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
	if app.Config.Lagcheck.ZKCheck == 0 {
		app.Config.Lagcheck.ZKCheck = 60
	}
	if app.Config.Lagcheck.StormCheck == 0 {
		app.Config.Lagcheck.StormCheck = 60
	}
	if app.Config.Lagcheck.MinDistance == 0 {
		app.Config.Lagcheck.MinDistance = 1
	}
	if app.Config.Lagcheck.ZKGroupRefresh == 0 {
		app.Config.Lagcheck.ZKGroupRefresh = 300
	}
	if app.Config.Lagcheck.StormGroupRefresh == 0 {
		app.Config.Lagcheck.StormGroupRefresh = 300
	}

	// HTTP Server
	if app.Config.Httpserver.Enable {
		if len(app.Config.Httpserver.Listen) == 0 {
			if app.Config.Httpserver.Port == 0 {
				errs = append(errs, "HTTP server port is not specified")
			}
			listenPort := fmt.Sprintf(":%v", app.Config.Httpserver.Port)
			app.Config.Httpserver.Listen = append(app.Config.Httpserver.Listen, listenPort)
		} else {
			if app.Config.Httpserver.Port != 0 {
				errs = append(errs, "Either HTTP server port or listen can be specified, but not both")
			}
		}
	}

	// Notify
	if app.Config.Notify.Interval == 0 {
		app.Config.Notify.Interval = 10
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
		for email, cfg := range app.Config.Emailnotifier {
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
					if _, ok := app.Config.Kafka[groupParts[0]]; !ok {
						errs = append(errs, "One or more email notification groups has a bad cluster name")
						break
					}
					if !validateTopic(groupParts[1]) {
						errs = append(errs, "One or more email notification groups has an invalid group name")
						break
					}
				}
			}
			if cfg.Interval == 0 {
				errs = append(errs, "Email notification interval is not specified")
			}
		}
	} else {
		if len(app.Config.Emailnotifier) > 0 {
			errs = append(errs, "Email notifications are configured, but SMTP server is not configured")
		}
	}

	// HTTP Notifier config
	if app.Config.Httpnotifier.UrlOpen != "" {
		if !validateUrl(app.Config.Httpnotifier.UrlOpen) {
			errs = append(errs, "HTTP notifier URL is invalid")
		}
		if app.Config.Httpnotifier.TemplateOpen == "" {
			app.Config.Httpnotifier.TemplateOpen = "config/default-http-post.tmpl"
		}
		if _, err := os.Stat(app.Config.Httpnotifier.TemplateOpen); os.IsNotExist(err) {
			errs = append(errs, "HTTP notifier template file does not exist")
		}
		if app.Config.Httpnotifier.TemplateClose == "" {
			app.Config.Httpnotifier.TemplateClose = "config/default-http-delete.tmpl"
		}
		if app.Config.Httpnotifier.UrlClose == "" {
			app.Config.Httpnotifier.UrlClose = app.Config.Httpnotifier.UrlOpen
		} else {
			if !validateUrl(app.Config.Httpnotifier.UrlClose) {
				errs = append(errs, "HTTP notifier close URL is invalid")
			}
		}
		if _, err := os.Stat(app.Config.Httpnotifier.TemplateClose); os.IsNotExist(err) {
			errs = append(errs, "HTTP notifier close template file does not exist")
		}
		if app.Config.Httpnotifier.PostThreshold == 0 {
			app.Config.Httpnotifier.PostThreshold = 2
		}
		if (app.Config.Httpnotifier.PostThreshold < 1) || (app.Config.Httpnotifier.PostThreshold > 3) {
			errs = append(errs, "HTTP notifier post-threshold must be between 1 and 3")
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

	// Slack Notifier config
	if app.Config.Slacknotifier.Url != "" {
		if !validateUrl(app.Config.Slacknotifier.Url) {
			errs = append(errs, "Slack notifier URL is invalid")
		}
		if app.Config.Slacknotifier.Channel == "" {
			app.Config.Slacknotifier.Channel = "#general"
		}
		if app.Config.Slacknotifier.Username == "" {
			app.Config.Slacknotifier.Username = "Burrower"
		}
		if app.Config.Slacknotifier.IconUrl == "" {
			app.Config.Slacknotifier.IconUrl = "https://slack.com/img/icons/app-57.png"
		}
		if app.Config.Slacknotifier.IconEmoji == "" {
			app.Config.Slacknotifier.IconEmoji = ":ghost:"
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
	matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\.-]+$`, topic)
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

// Validate a list of ZK or Kafka hosts with optional ports
func checkHostlist(hosts []string, defaultPort int, appName string) string {
	for i, host := range hosts {
		hostparts := strings.Split(host, ":")
		hostport := defaultPort
		hostname := hostparts[0]

		if len(hostparts) == 2 {
			// Must be a hostname or IPv4 address with a port
			var err error
			hostport, err = strconv.Atoi(hostparts[1])
			if (err != nil) || (hostport == 0) {
				return fmt.Sprintf("One or more %s hostnames have invalid port components", appName)
			}
		}

		if len(hostparts) > 2 {
			// Must be an IPv6 address
			// Try without popping off the last segment as a port number first
			if validateIP(host) {
				hostname = host
			} else {
				// The full host didn't validate as an IP, so let's pull off the last piece as a port number and try again
				hostname = strings.Join(hostparts[:len(hostparts)-1], ":")

				hostport, err := strconv.Atoi(hostparts[len(hostparts)-1])
				if (err != nil) || (hostport == 0) {
					return fmt.Sprintf("One or more %s hostnames have invalid port components", appName)
				}
			}
		}

		if !validateHostname(hostname) {
			return fmt.Sprintf("One or more %s hostnames are invalid", appName)
		}

		hosts[i] = fmt.Sprintf("[%s]:%v", hostname, hostport)
	}

	return ""
}
