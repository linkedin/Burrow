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
	"errors"
	"fmt"
	"net/smtp"
	"regexp"
	"strings"
	"text/template"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/spf13/viper"
)

// EmailNotifier is a module which can be used to send notifications of consumer group status via email messages. One
// email is sent for each consumer group that matches the whitelist/blacklist and the status threshold.
type EmailNotifier struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name           string
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	extras         map[string]string
	templateOpen   *template.Template
	templateClose  *template.Template
	from           string
	to             string

	auth           smtp.Auth
	serverWithPort string
	sendMailFunc   func(string, smtp.Auth, string, []string, []byte) error
}

// Configure validates the configuration of the email notifier. At minimum, there must be a valid server, port, from
// address, and to address. If any of these are missing or incorrect, this func will panic with an explanatory message.
// It is also possible to specify an auth-type of either "plain" or "crammd5", along with a username and password.
func (module *EmailNotifier) Configure(name string, configRoot string) {
	module.name = name

	// Abstract the SendMail call so we can test
	if module.sendMailFunc == nil {
		module.sendMailFunc = smtp.SendMail
	}

	module.serverWithPort = fmt.Sprintf("%s:%v", viper.GetString(configRoot+".server"), viper.GetString(configRoot+".port"))
	if !helpers.ValidateHostList([]string{module.serverWithPort}) {
		module.Log.Panic("bad server or port")
		panic(errors.New("configuration error"))
	}

	module.from = viper.GetString(configRoot + ".from")
	if module.from == "" {
		module.Log.Panic("missing from address")
		panic(errors.New("configuration error"))
	}

	module.to = viper.GetString(configRoot + ".to")
	if module.to == "" {
		module.Log.Panic("missing to address")
		panic(errors.New("configuration error"))
	}

	// Set up SMTP authentication
	switch strings.ToLower(viper.GetString(configRoot + ".auth-type")) {
	case "plain":
		module.auth = smtp.PlainAuth("", viper.GetString(configRoot+".username"), viper.GetString(configRoot+".password"), viper.GetString(configRoot+".server"))
	case "crammd5":
		module.auth = smtp.CRAMMD5Auth(viper.GetString(configRoot+".username"), viper.GetString(configRoot+".password"))
	case "":
		module.auth = nil
	default:
		module.Log.Panic("unknown auth type")
		panic(errors.New("configuration error"))
	}
}

// Start is a no-op for the email notifier. It always returns no error
func (module *EmailNotifier) Start() error {
	return nil
}

// Stop is a no-op for the email notifier. It always returns no error
func (module *EmailNotifier) Stop() error {
	return nil
}

// GetName returns the configured name of this module
func (module *EmailNotifier) GetName() string {
	return module.name
}

// GetGroupWhitelist returns the compiled group whitelist (or nil, if there is not one)
func (module *EmailNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

// GetGroupBlacklist returns the compiled group blacklist (or nil, if there is not one)
func (module *EmailNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
}

// GetLogger returns the configured zap.Logger for this notifier
func (module *EmailNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// AcceptConsumerGroup has no additional function for the email notifier, and so always returns true
func (module *EmailNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

// Notify sends a single email message, with the from and to set to the configured addresses for the notifier. The
// status, eventID, and startTime are all passed to the template for compiling the message. If stateGood is true, the
// "close" template is used. Otherwise, the "open" template is used.
func (module *EmailNotifier) Notify(status *protocol.ConsumerGroupStatus, eventID string, startTime time.Time, stateGood bool) {
	logger := module.Log.With(
		zap.String("cluster", status.Cluster),
		zap.String("group", status.Group),
		zap.String("id", eventID),
		zap.String("status", status.Status.String()),
	)

	var tmpl *template.Template
	if stateGood {
		tmpl = module.templateClose
	} else {
		tmpl = module.templateOpen
	}

	// Put the from and to lines in without the template. Template should set the subject line, followed by a blank line
	bytesToSend := bytes.NewBufferString("From: " + module.from + "\nTo: " + module.to + "\n")
	messageBody, err := executeTemplate(tmpl, module.extras, status, eventID, startTime)
	if err != nil {
		logger.Error("failed to assemble", zap.Error(err))
		return
	}
	bytesToSend.Write(messageBody.Bytes())

	err = module.sendMailFunc(module.serverWithPort, module.auth, module.from, []string{module.to}, bytesToSend.Bytes())
	if err != nil {
		logger.Error("failed to send", zap.Error(err))
	}
}
