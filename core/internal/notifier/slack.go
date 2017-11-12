/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"text/template"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
)

type SlackNotifier struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name           string
	threshold      int
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	extras         map[string]string
	templateOpen   *template.Template
	templateClose  *template.Template
	token          string
	channel        string
	username       string
	iconUrl        string
	iconEmoji      string

	HttpClient *http.Client
	postURL    string
}

func (module *SlackNotifier) Configure(name string, configRoot string) {
	module.name = name

	// Set the Slack chat.postMessage URL (unless it's been set for testing)
	if module.postURL == "" {
		module.postURL = "https://slack.com/api/chat.postMessage"
	}

	module.token = viper.GetString(configRoot + ".token")
	if module.token == "" {
		module.Log.Panic("missing auth token")
		panic(errors.New("configuration error"))
	}

	module.channel = viper.GetString(configRoot + ".channel")
	if module.channel == "" {
		module.Log.Panic("missing channel")
		panic(errors.New("configuration error"))
	}

	module.username = viper.GetString(configRoot + ".username")
	module.iconUrl = viper.GetString(configRoot + ".icon-url")
	module.iconEmoji = viper.GetString(configRoot + ".icon-emoji")

	// Set defaults for module-specific configs if needed
	viper.SetDefault(configRoot+".timeout", 5)
	viper.SetDefault(configRoot+".keepalive", 300)

	// Set up HTTP client
	module.HttpClient = &http.Client{
		Timeout: viper.GetDuration(configRoot+".timeout") * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				KeepAlive: viper.GetDuration(configRoot+".keepalive") * time.Second,
			}).Dial,
			Proxy: http.ProxyFromEnvironment,
		},
	}
}

func (module *SlackNotifier) Start() error {
	// Slack notifier does not have a running component - no start needed
	return nil
}

func (module *SlackNotifier) Stop() error {
	// Slack notifier does not have a running component - no start needed
	return nil
}

func (module *SlackNotifier) GetName() string {
	return module.name
}

func (module *SlackNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *SlackNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
}

func (module *SlackNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *SlackNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

type SlackMessage struct {
	Channel   string `json:"channel"`
	Username  string `json:"username,omitempty"`
	IconUrl   string `json:"icon-url,omitempty"`
	IconEmoji string `json:"icon-emoji,omitempty"`
	Text      string `json:"text,omitempty"`
}

func (module *SlackNotifier) Notify(status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
	logger := module.Log.With(
		zap.String("cluster", status.Cluster),
		zap.String("group", status.Group),
		zap.String("id", eventId),
		zap.String("status", status.Status.String()),
	)

	var tmpl *template.Template

	if stateGood {
		tmpl = module.templateClose
	} else {
		tmpl = module.templateOpen
	}

	messageBytes, err := ExecuteTemplate(tmpl, module.extras, status, eventId, startTime)
	if err != nil {
		logger.Error("failed to assemble message", zap.Error(err))
		return
	}

	// Encode JSON payload
	data, err := json.Marshal(SlackMessage{
		Channel:   module.channel,
		Username:  module.username,
		IconUrl:   module.iconUrl,
		IconEmoji: module.iconEmoji,
		Text:      messageBytes.String(),
	})
	if err != nil {
		logger.Error("failed to encode", zap.Error(err))
		return
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest("POST", module.postURL, bytes.NewBuffer(data))
	if err != nil {
		logger.Error("failed to create request", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+module.token)

	resp, err := module.HttpClient.Do(req)
	if err != nil {
		logger.Error("failed to send", zap.Error(err))
		return
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
		logger.Debug("sent")
	} else {
		logger.Error("failed to send", zap.Int("response", resp.StatusCode))
	}
}
