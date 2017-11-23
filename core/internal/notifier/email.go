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

	"github.com/linkedin/burrow/core/internal/helpers"
	"github.com/linkedin/burrow/core/protocol"
	"github.com/spf13/viper"
)

type EmailNotifier struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name           string
	threshold      int
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

func (module *EmailNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *EmailNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
}

func (module *EmailNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *EmailNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

func (module *EmailNotifier) Notify(status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
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
	bytesToSend := bytes.NewBufferString("From: " + module.from + "\nTo: " + module.to + "\n")
	messageBody, err := ExecuteTemplate(tmpl, module.extras, status, eventId, startTime)
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
