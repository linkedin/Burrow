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

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/burrow/core/protocol"
)

type HttpNotifier struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name           string
	threshold      int
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	extras         map[string]string
	urlOpen        string
	urlClose       string
	methodOpen     string
	methodClose    string
	templateOpen   *template.Template
	templateClose  *template.Template
	sendClose      bool

	HttpClient *http.Client
}

func (module *HttpNotifier) Configure(name string, configRoot string) {
	module.name = name

	// Validate and set defaults for profile configs
	module.urlOpen = viper.GetString(configRoot + ".url-open")
	if module.urlOpen == "" {
		module.Log.Panic("no url-open specified")
		panic(errors.New("configuration error"))
	}
	viper.SetDefault(configRoot+".method-open", "POST")
	module.methodOpen = viper.GetString(configRoot + ".method-open")

	module.sendClose = viper.GetBool(configRoot + ".send-close")
	if module.sendClose {
		module.urlClose = viper.GetString(configRoot + ".url-close")
		if module.urlClose == "" {
			module.Log.Panic("no url-close specified")
			panic(errors.New("configuration error"))
		}
		viper.SetDefault(configRoot+".method-close", "POST")
		module.methodClose = viper.GetString(configRoot + ".method-close")
	}

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

func (module *HttpNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *HttpNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
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
		method = module.methodClose
		url = module.urlClose
	} else {
		tmpl = module.templateOpen
		method = module.methodOpen
		url = module.urlOpen
	}

	bytesToSend, err := ExecuteTemplate(tmpl, module.extras, status, eventId, startTime)
	if err != nil {
		logger.Error("failed to assemble message", zap.Error(err))
		return
	}

	// Send request to HTTP endpoint
	req, err := http.NewRequest(method, url, bytesToSend)
	if err != nil {
		logger.Error("failed to create request", zap.Error(err))
		return
	}
	username := viper.GetString("notifier." + module.name + ".username")
	if username != "" {
		// Add basic auth using the provided username and password
		req.SetBasicAuth(viper.GetString("notifier."+module.name+".username"), viper.GetString("notifier."+module.name+".password"))
	}
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
