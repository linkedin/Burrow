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
	"crypto/tls"
	"errors"
	"fmt"
	"net/smtp"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
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

	to   string
	from string

	smtpDialer   *gomail.Dialer
	sendMailFunc func(message *gomail.Message) error
}

// Configure validates the configuration of the email notifier. At minimum, there must be a valid server, port, from
// address, and to address. If any of these are missing or incorrect, this func will panic with an explanatory message.
// It is also possible to specify an auth-type of either "plain" or "crammd5", along with a username and password.
func (module *EmailNotifier) Configure(name string, configRoot string) {
	module.name = name

	// Abstract the SendMail call so we can test
	if module.sendMailFunc == nil {
		module.sendMailFunc = module.sendEmail
	}

	host := viper.GetString(configRoot + ".server")
	port := viper.GetInt(configRoot + ".port")

	serverWithPort := fmt.Sprintf("%s:%v", host, port)

	if !helpers.ValidateHostList([]string{serverWithPort}) {
		module.Log.Panic("bad server or port")
		panic(errors.New("configuration error"))
	}

	module.from = viper.GetString(configRoot + ".from")
	if module.from == "" {
		module.Log.Panic("missing 	from address")
		panic(errors.New("configuration error"))
	}

	module.to = viper.GetString(configRoot + ".to")
	if module.to == "" {
		module.Log.Panic("missing to address")
		panic(errors.New("configuration error"))
	}

	// Set up dialer and extra TLS configuration
	extraCa := viper.GetString(configRoot + ".extra-ca")
	noVerify := viper.GetBool(configRoot + ".noverify")

	d := gomail.NewDialer(host, port, "", "")
	d.Auth = module.getSMTPAuth(configRoot)
	d.TLSConfig = buildEmailTLSConfig(extraCa, noVerify, host)

	module.smtpDialer = d
}

func buildEmailTLSConfig(extraCaFile string, noVerify bool, smtpHost string) *tls.Config {
	rootCAs := buildRootCAs(extraCaFile, noVerify)

	return &tls.Config{
		InsecureSkipVerify: noVerify,
		ServerName:         smtpHost,
		RootCAs:            rootCAs,
	}
}

// Builds authentication profile for smtp client
func (module *EmailNotifier) getSMTPAuth(configRoot string) smtp.Auth {
	var auth smtp.Auth
	// Set up SMTP authentication
	switch strings.ToLower(viper.GetString(configRoot + ".auth-type")) {
	case "plain":
		auth = smtp.PlainAuth("", viper.GetString(configRoot+".username"), viper.GetString(configRoot+".password"), viper.GetString(configRoot+".server"))
	case "crammd5":
		auth = smtp.CRAMMD5Auth(viper.GetString(configRoot+".username"), viper.GetString(configRoot+".password"))
	case "":
		auth = nil
	default:
		module.Log.Panic("unknown auth type")
		panic(errors.New("configuration error"))
	}

	return auth
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
	messageContent, err := executeTemplate(tmpl, module.extras, status, eventID, startTime)

	if err != nil {
		logger.Error("failed to assemble", zap.Error(err))
		return
	}

	// Process template headers and send email
	if m, err := module.createMessage(messageContent.String()); err == nil {
		if err := module.sendMailFunc(m); err != nil {
			logger.Error("failed to send", zap.Error(err))
		}
	} else {
		logger.Error("failed to send", zap.Error(err))
	}
}

// sendEmail uses the gomail smtpDialer to send a constructed message. This function is mocked for testing purposes
func (module *EmailNotifier) sendEmail(m *gomail.Message) error {
	if err := module.smtpDialer.DialAndSend(m); err != nil {
		return err
	}

	return nil
}

// createMessage organizes all relevant email message content into a structure for easy use
func (module *EmailNotifier) createMessage(messageContent string) (*gomail.Message, error) {
	m := gomail.NewMessage()
	var subject string
	var mimeVersion string

	contentType := "text/plain"

	subjectDelimiter := "Subject: "
	contentTypeDelimiter := "Content-Type: "
	mimeVersionDelimiter := "MIME-version: "

	if !strings.HasPrefix(messageContent, subjectDelimiter) {
		return nil, errors.New("no subject line detected. Please make sure" +
			" \"Subject: my_subject_line\" is included in your template")
	}

	var body string

	// Go doesn't support regex lookaheads yet
	for _, line := range strings.Split(messageContent, "\n") {
		if strings.HasPrefix(line, subjectDelimiter) && subject == "" {
			subject = getKeywordContent(line, subjectDelimiter)
		} else if strings.HasPrefix(line, contentTypeDelimiter) {
			contentType = strings.Replace(getKeywordContent(line, contentTypeDelimiter), ";", "", -1)
		} else if strings.HasPrefix(line, mimeVersionDelimiter) {
			mimeVersion = strings.Replace(getKeywordContent(line, mimeVersionDelimiter), ";", "", -1)
		} else {
			body = body + line + "\n"
		}
	}

	recipients := strings.Split(module.to, ",")
	m.SetHeader("To", recipients...)
	m.SetHeader("From", module.from)
	m.SetHeader("Subject", subject)

	if mimeVersion != "" {
		m.SetHeader("MIME-version", mimeVersion)
	}

	m.SetBody(contentType, body)

	return m, nil
}

func getKeywordContent(header string, subjectDelimiter string) string {
	return strings.Split(header, subjectDelimiter)[1]
}
