/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
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
	"fmt"
	log "github.com/cihub/seelog"
	"net/smtp"
	"text/template"
)

type EmailNotifier struct {
	TemplateFile string
	Server       string
	Port         int
	Interval     int64
	Threshold    int
	Username     string
	Password     string
	AuthType     string
	From         string
	To           string
	Groups       []string
	auth         smtp.Auth
	template     *template.Template
	groupMsgs    map[string]Message
}

func (emailer *EmailNotifier) NotifierName() string {
	return "email-notify"
}

func (emailer *EmailNotifier) Notify(msg Message) error {
	if emailer.Ignore(msg) {
		return nil
	}

	if emailer.auth == nil {
		switch emailer.AuthType {
		case "plain":
			emailer.auth = smtp.PlainAuth("", emailer.Username, emailer.Password, emailer.Server)
		case "crammd5":
			emailer.auth = smtp.CRAMMD5Auth(emailer.Username, emailer.Password)
		}
	}

	if emailer.template == nil {
		template, err := template.ParseFiles(emailer.TemplateFile)
		if err != nil {
			log.Critical("Cannot parse email template: %v", err)
			return err
		}
		emailer.template = template
	}

	if emailer.groupMsgs == nil {
		emailer.groupMsgs = make(map[string]Message)
	}

	for _, group := range emailer.Groups {
		clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
		if clusterGroup == group {
			emailer.groupMsgs[clusterGroup] = msg
		}
	}
	if len(emailer.Groups) == len(emailer.groupMsgs) {
		return emailer.sendConsumerGroupStatusNotify()
	}
	return nil
}

func (emailer *EmailNotifier) Ignore(msg Message) bool {
	return int(msg.Status) < emailer.Threshold
}

func (emailer *EmailNotifier) sendConsumerGroupStatusNotify() error {
	var bytesToSend bytes.Buffer
	log.Debug("send email")

	msgs := make([]Message, len(emailer.Groups))
	i := 0
	for group, msg := range emailer.groupMsgs {
		msgs[i] = msg
		delete(emailer.groupMsgs, group)
		i++
	}

	err := emailer.template.Execute(&bytesToSend, struct {
		From    string
		To      string
		Results []Message
	}{
		From:    emailer.From,
		To:      emailer.To,
		Results: msgs,
	})
	if err != nil {
		log.Error("Failed to assemble email:", err)
		return err
	}

	err = smtp.SendMail(fmt.Sprintf("%s:%v", emailer.Server, emailer.Port),
		emailer.auth, emailer.From, []string{emailer.To}, bytesToSend.Bytes())
	if err != nil {
		log.Error("Failed to send email message:", err)
		return err
	}
	return nil
}
