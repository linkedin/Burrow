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

	"crypto/tls"

	"github.com/linkedin/Burrow/core/protocol"
)

// HTTPNotifier is a module which can be used to send notifications of consumer group status via outbound HTTP calls to
// another server. This is useful for informing another system, such as an alert system, when there is a problem. One
// HTTP call is made for each consumer group that matches the whitelist/blacklist and the status threshold (though
// keepalive connections will be used if configured).
type HTTPNotifier struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name           string
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

	httpClient *http.Client
}

// Configure validates the configuration of the http notifier. At minimum, there must be a url-open specified, and if
// send-close is set to true there must also be a url-close. If these are missing or incorrect, this func will panic
// with an explanatory message. It is also possible to configure a specific method (such as POST or DELETE) to be used
// with these URLs, as well as a timeout and keepalive for the HTTP smtpClient.
func (module *HTTPNotifier) Configure(name string, configRoot string) {
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

	tlsConfig := buildHTTPTLSConfig(viper.GetString(configRoot+".extra-ca"), viper.GetBool(configRoot+".noverify"))

	module.httpClient = &http.Client{
		Timeout: viper.GetDuration(configRoot+".timeout") * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				KeepAlive: viper.GetDuration(configRoot+".keepalive") * time.Second,
			}).Dial,
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		},
	}
}

func buildHTTPTLSConfig(extraCaFile string, noVerify bool) *tls.Config {
	rootCAs := buildRootCAs(extraCaFile, noVerify)

	return &tls.Config{
		InsecureSkipVerify: noVerify,
		RootCAs:            rootCAs,
	}
}

// Start is a no-op for the http notifier. It always returns no error
func (module *HTTPNotifier) Start() error {
	return nil
}

// Stop is a no-op for the http notifier. It always returns no error
func (module *HTTPNotifier) Stop() error {
	return nil
}

// GetName returns the configured name of this module
func (module *HTTPNotifier) GetName() string {
	return module.name
}

// GetGroupWhitelist returns the compiled group whitelist (or nil, if there is not one)
func (module *HTTPNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

// GetGroupBlacklist returns the compiled group blacklist (or nil, if there is not one)
func (module *HTTPNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
}

// GetLogger returns the configured zap.Logger for this notifier
func (module *HTTPNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// AcceptConsumerGroup has no additional function for the http notifier, and so always returns true
func (module *HTTPNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

// Notify makes a single outbound HTTP request. The status, eventID, and startTime are all passed to the template for
// compiling the request body. If stateGood is true, the "close" template and URL are used. Otherwise, the "open"
// template and URL are used.
func (module *HTTPNotifier) Notify(status *protocol.ConsumerGroupStatus, eventID string, startTime time.Time, stateGood bool) {
	logger := module.Log.With(
		zap.String("cluster", status.Cluster),
		zap.String("group", status.Group),
		zap.String("id", eventID),
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

	bytesToSend, err := executeTemplate(tmpl, module.extras, status, eventID, startTime)
	if err != nil {
		logger.Error("failed to assemble message", zap.Error(err))
		return
	}

	urlTmpl, err := template.New("url").Parse(url)
	if err != nil {
		logger.Error("failed to parse url", zap.Error(err))
		return
	}

	urlToSend, err := executeTemplate(urlTmpl, module.extras, status, eventID, startTime)
	if err != nil {
		logger.Error("failed to assemble url", zap.Error(err))
		return
	}

	// Send request to HTTP endpoint
	req, err := http.NewRequest(method, urlToSend.String(), bytesToSend)
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

	for header, value := range viper.GetStringMapString("notifier." + module.name + ".headers") {
		req.Header.Set(header, value)
	}

	resp, err := module.httpClient.Do(req)
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
