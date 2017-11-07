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
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"text/template"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type HttpNotifier struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name            string
	myConfiguration *configuration.NotifierConfig
	profile         *configuration.HttpNotifierProfile

	groupWhitelist *regexp.Regexp
	extras         map[string]string
	templateOpen   *template.Template
	templateClose  *template.Template

	HttpClient *http.Client
}

func (module *HttpNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]

	if profile, ok := module.App.Configuration.HttpNotifierProfile[module.myConfiguration.Profile]; ok {
		module.profile = profile
	} else {
		module.Log.Panic("unknown HTTP notifier profile")
		panic(errors.New("configuration error"))
	}

	// Validate and set defaults for profile configs
	if module.profile.UrlOpen == "" {
		module.Log.Panic("no url-open specified")
		panic(errors.New("configuration error"))
	}
	if module.profile.MethodOpen == "" {
		module.profile.MethodOpen = "POST"
	}
	if module.myConfiguration.SendClose {
		if module.profile.UrlClose == "" {
			module.Log.Panic("no url-close specified")
			panic(errors.New("configuration error"))
		}
		if module.profile.MethodClose == "" {
			module.profile.MethodClose = "POST"
		}
	}

	// Set defaults for module-specific configs if needed
	if module.App.Configuration.Notifier[module.name].Timeout == 0 {
		module.App.Configuration.Notifier[module.name].Timeout = 5
	}
	if module.App.Configuration.Notifier[module.name].Keepalive == 0 {
		module.App.Configuration.Notifier[module.name].Keepalive = 300
	}

	// Set up HTTP client
	module.HttpClient = &http.Client{
		Timeout: time.Duration(module.myConfiguration.Timeout) * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				KeepAlive: time.Duration(module.myConfiguration.Keepalive) * time.Second,
			}).Dial,
			Proxy: http.ProxyFromEnvironment,
		},
	}
}

func (module *HttpNotifier) Start() error {
	// HTTP notifier does not have a running component - no start needed
	return nil
}

func (module *HttpNotifier) Stop() error {
	// HTTP notifier does not have a running component - no stop needed
	return nil
}

func (module *HttpNotifier) GetName() string {
	return module.name
}

func (module *HttpNotifier) GetConfig() *configuration.NotifierConfig {
	return module.myConfiguration
}

func (module *HttpNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *HttpNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *HttpNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

func (module *HttpNotifier) Notify(status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
	logger := module.Log.With(
		zap.String("cluster", status.Cluster),
		zap.String("group", status.Group),
		zap.String("id", eventId),
		zap.String("status", status.Status.String()),
	)

	var tmpl *template.Template
	var method string
	var url string

	if stateGood {
		tmpl = module.templateClose
		method = module.profile.MethodClose
		url = module.profile.UrlClose
	} else {
		tmpl = module.templateOpen
		method = module.profile.MethodOpen
		url = module.profile.UrlOpen
	}

	bytesToSend, err := ExecuteTemplate(tmpl, module.extras, status, eventId, startTime)
	if err != nil {
		logger.Error("failed to assemble message", zap.Error(err))
		return
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest(method, url, bytesToSend)
	req.Header.Set("Content-Type", "application/json")

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
