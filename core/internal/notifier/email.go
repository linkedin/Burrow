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

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type EmailNotifier struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	groupWhitelist  *regexp.Regexp
	extras          map[string]string
	templateOpen    *template.Template
	templateClose   *template.Template

	name            string
	myConfiguration *configuration.NotifierConfig
	profile         *configuration.EmailNotifierProfile

	auth            smtp.Auth
	serverWithPort  string
	sendMailFunc    func (string, smtp.Auth, string, []string, []byte) error
}

func (module *EmailNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]

	// Abstract the SendMail call so we can test
	if module.sendMailFunc == nil {
		module.sendMailFunc = smtp.SendMail
	}

	if profile, ok := module.App.Configuration.EmailNotifierProfile[module.myConfiguration.Profile]; ok {
		module.profile = profile
	} else {
		module.Log.Panic("unknown Email notifier profile")
		panic(errors.New("configuration error"))
	}

	module.serverWithPort = fmt.Sprintf("%s:%v", module.profile.Server, module.profile.Port)
	if ! configuration.ValidateHostList([]string{module.serverWithPort}) {
		module.Log.Panic("bad server or port")
		panic(errors.New("configuration error"))
	}

	if module.profile.From == "" {
		module.Log.Panic("missing from address")
		panic(errors.New("configuration error"))
	}

	if module.profile.To == "" {
		module.Log.Panic("missing to address")
		panic(errors.New("configuration error"))
	}

	// Set up SMTP authentication
	switch strings.ToLower(module.profile.AuthType) {
	case "plain":
		module.auth = smtp.PlainAuth("", module.profile.Username, module.profile.Password, module.profile.Server)
	case "crammd5":
		module.auth = smtp.CRAMMD5Auth(module.profile.Username, module.profile.Password)
	case "":
		module.auth = nil
	default:
		module.Log.Panic("unknown auth type")
		panic(errors.New("configuration error"))
	}
}

func (module *EmailNotifier) Start() error {
	// Email notifier does not have a running component - no start needed
	return nil
}

func (module *EmailNotifier) Stop() error {
	// Email notifier does not have a running component - no stop needed
	return nil
}

func (module *EmailNotifier) GetName() string {
	return module.name
}

func (module *EmailNotifier) GetConfig() *configuration.NotifierConfig {
	return module.myConfiguration
}

func (module *EmailNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *EmailNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *EmailNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

func (module *EmailNotifier) Notify (status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
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

	// Put the from and to lines in without the template. Template should set the subject line, followed by a blank line
	bytesToSend := bytes.NewBufferString("From: " + module.profile.From + "\nTo: " + module.profile.To + "\n")
	messageBody, err := ExecuteTemplate(tmpl, module.extras, status, eventId, startTime)
	if err != nil {
		logger.Error("failed to assemble", zap.Error(err))
		return
	}
	bytesToSend.Write(messageBody.Bytes())

	err = module.sendMailFunc(module.serverWithPort, module.auth, module.profile.From, []string{module.profile.To}, bytesToSend.Bytes())
	if err != nil {
		logger.Error("failed to send", zap.Error(err))
	}
}