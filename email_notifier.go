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
	"bytes"
	"fmt"
	log "github.com/cihub/seelog"
	"net/smtp"
	"text/template"
)

type EmailNotifier struct {
	template  *template.Template
	auth      smtp.Auth
	server    string
	port      int
	interval  int64
	threshold int
	username  string
	password  string
	from      string
	to        string
	groups    []string
}

func (emailer *EmailNotifier) NotifierName() string {
	return "email-notify"
}

func (emailer *EmailNotifier) Notify(msg Message) error {
	switch msg.(type) {
	case *ConsumerGroupStatus:
		result, _ := msg.(*ConsumerGroupStatus)
		return emailer.sendConsumerGroupStatusNotify(result)
	}
	return nil
}

func (emailer *EmailNotifier) Ignore(msg Message) bool {
	switch msg.(type) {
	case *ConsumerGroupStatus:
		result, _ := msg.(*ConsumerGroupStatus)
		return int(result.Status) < emailer.threshold
	}
	return false
}

func NewEmailNotifier(app *ApplicationContext) ([]*EmailNotifier, error) {
	log.Info("Start email notify")
	template, err := template.ParseFiles(app.Config.Smtp.Template)
	if err != nil {
		log.Critical("Cannot parse email template: %v", err)
		return nil, err
	}

	var auth smtp.Auth
	switch app.Config.Smtp.AuthType {
	case "plain":
		auth = smtp.PlainAuth("", app.Config.Smtp.Username, app.Config.Smtp.Password, app.Config.Smtp.Server)
	case "crammd5":
		auth = smtp.CRAMMD5Auth(app.Config.Smtp.Username, app.Config.Smtp.Password)
	}

	emailers := []*EmailNotifier{}
	for to, cfg := range app.Config.Emailnotifier {
		if cfg.Enable {
			emailer := &EmailNotifier{
				threshold: cfg.Threshold,
				template:  template,
				server:    app.Config.Smtp.Server,
				port:      app.Config.Smtp.Port,
				username:  app.Config.Smtp.Username,
				password:  app.Config.Smtp.Password,
				auth:      auth,
				groups:    cfg.Groups,
				interval:  cfg.Interval,
				from:      app.Config.Smtp.From,
				to:        to,
			}
			emailers = append(emailers, emailer)
		}
	}

	return emailers, nil
}

func (emailer *EmailNotifier) sendConsumerGroupStatusNotify(result *ConsumerGroupStatus) error {
	var bytesToSend bytes.Buffer

	err := emailer.template.Execute(&bytesToSend, struct {
		From    string
		To      string
		Results []*ConsumerGroupStatus
	}{
		From:    emailer.from,
		To:      emailer.to,
		Results: []*ConsumerGroupStatus{result},
	})
	if err != nil {
		log.Error("Failed to assemble email:", err)
		return err
	}

	err = smtp.SendMail(fmt.Sprintf("%s:%v", emailer.server, emailer.port),
		emailer.auth, emailer.from, []string{emailer.to}, bytesToSend.Bytes())
	if err != nil {
		log.Error("Failed to send email message:", err)
		return err
	}
	return nil
}
