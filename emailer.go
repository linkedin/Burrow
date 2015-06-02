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
	"os"
	"strings"
	"text/template"
	"time"
)

type Emailer struct {
	app       *ApplicationContext
	template  *template.Template
	Tickers   map[string]*time.Ticker
	quitSends chan struct{}
	auth      smtp.Auth
}

func NewEmailer(app *ApplicationContext) (*Emailer, error) {
	template, err := template.ParseFiles(app.Config.Smtp.Template)
	if err != nil {
		log.Critical("Cannot parse email template: %v", err)
		os.Exit(1)
	}

	var auth smtp.Auth
	switch app.Config.Smtp.AuthType {
	case "plain":
		auth = smtp.PlainAuth("", app.Config.Smtp.Username, app.Config.Smtp.Password, app.Config.Smtp.Server)
	case "crammd5":
		auth = smtp.CRAMMD5Auth(app.Config.Smtp.Username, app.Config.Smtp.Password)
	}

	return &Emailer{
		app:       app,
		template:  template,
		Tickers:   make(map[string]*time.Ticker),
		quitSends: make(chan struct{}),
		auth:      auth,
	}, nil
}

func (emailer *Emailer) Start() {
	for email, cfg := range emailer.app.Config.Email {
		emailer.Tickers[email] = time.NewTicker(time.Duration(cfg.Interval) * time.Second)
		go emailer.sendEmailNotifications(email, cfg.Threshold, cfg.Groups, emailer.Tickers[email].C)
	}
}

func (emailer *Emailer) Stop() {
	close(emailer.quitSends)
}

func (emailer *Emailer) sendEmail(to string, results []*ConsumerGroupStatus) {
	var bytesToSend bytes.Buffer

	err := emailer.template.Execute(&bytesToSend, struct {
		From    string
		To      string
		Results []*ConsumerGroupStatus
	}{
		From:    emailer.app.Config.Smtp.From,
		To:      to,
		Results: results,
	})
	if err != nil {
		log.Error("Failed to assemble email:", err)
	}

	err = smtp.SendMail(fmt.Sprintf("%s:%v", emailer.app.Config.Smtp.Server, emailer.app.Config.Smtp.Port),
		emailer.auth, emailer.app.Config.Smtp.From, []string{to}, bytesToSend.Bytes())
	if err != nil {
		log.Error("Failed to send email message:", err)
	}
}

func (emailer *Emailer) sendEmailNotifications(email string, threshold string, groups []string, ticker <-chan time.Time) {
	// Convert the config threshold string into a value
	thresholdVal := StatusWarning
	if threshold == "ERROR" {
		thresholdVal = StatusError
	}

OUTERLOOP:
	for {
		select {
		case <-emailer.quitSends:
			for _, ticker := range emailer.Tickers {
				ticker.Stop()
			}
			break OUTERLOOP
		case <-ticker:
			results := make([]*ConsumerGroupStatus, 0, len(groups))
			resultChannel := make(chan *ConsumerGroupStatus)

			for _, group := range groups {
				groupParts := strings.Split(group, ",")
				storageRequest := &RequestConsumerStatus{Result: resultChannel, Cluster: groupParts[0], Group: groupParts[1]}
				emailer.app.Storage.requestChannel <- storageRequest
			}

			for {
				result := <-resultChannel
				results = append(results, result)
				if len(results) == len(groups) {
					break
				}
			}

			// Send an email if any of the resoults breaches the threshold
			for _, result := range results {
				if result.Status >= thresholdVal {
					emailer.sendEmail(email, results)
					break
				}
			}
		}
	}
}
