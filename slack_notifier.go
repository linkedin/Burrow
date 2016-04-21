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
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type SlackNotifier struct {
	refreshTicker *time.Ticker
	url           string
	channel       string
	username      string
	iconUrl       string
	iconEmoji     string
	threshold     int
	httpClient    *http.Client
	groups        []string
	groupResults  map[string]Message
}

type SlackMessage struct {
	Channel     string       `json:"channel"`
	Username    string       `json:"username"`
	IconUrl     string       `json:"icon_url"`
	IconEmoji   string       `json:"icon_emoji"`
	Text        string       `json:"text,omitempty"`
	Attachments []attachment `json:"attachments,omitempty"`
}

type attachment struct {
	Color    string   `json:"color"`
	Title    string   `json:"title"`
	Pretext  string   `json:"pretext"`
	Fallback string   `json:"fallback"`
	Text     string   `json:"text"`
	MrkdwnIn []string `json:"mrkdwn_in"`
}

func (slack *SlackNotifier) NotifierName() string {
	return "slack-notify"
}

func (slack *SlackNotifier) Ignore(msg Message) bool {
	switch msg.(type) {
	case *ConsumerGroupStatus:
		result, _ := msg.(*ConsumerGroupStatus)

		return int(result.Status) < slack.threshold
	}
	return true
}

func (slack *SlackNotifier) Notify(msg Message) error {
	switch msg.(type) {
	case *ConsumerGroupStatus:
		result, _ := msg.(*ConsumerGroupStatus)
		for _, group := range slack.groups {
			clusterGroup := fmt.Sprintf("%s,%s", result.Cluster, result.Group)
			if clusterGroup == group {
				slack.groupResults[clusterGroup] = msg
			}
		}
		if len(slack.groups) == len(slack.groupResults) {
			return slack.sendConsumerGroupStatusNotify()
		}

	default:
		log.Infof("msg type is not ConsumerGroupStatus")
	}
	return nil
}

func NewSlackNotifier(app *ApplicationContext) (*SlackNotifier, error) {
	log.Info("Start Slack Notify")

	return &SlackNotifier{
		url:          app.Config.Slacknotifier.Url,
		groups:       app.Config.Slacknotifier.Groups,
		groupResults: make(map[string]Message),
		threshold:    app.Config.Slacknotifier.Threshold,
		channel:      app.Config.Slacknotifier.Channel,
		username:     app.Config.Slacknotifier.Username,
		iconUrl:      app.Config.Slacknotifier.IconUrl,
		iconEmoji:    app.Config.Slacknotifier.IconEmoji,
		httpClient: &http.Client{
			Timeout: time.Duration(app.Config.Slacknotifier.Timeout) * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					KeepAlive: time.Duration(app.Config.Slacknotifier.Keepalive) * time.Second,
				}).Dial,
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}, nil

}

func (slack *SlackNotifier) sendConsumerGroupStatusNotify() error {
	results := make([]attachment, len(slack.groups))
	i := 0
	for _, groupResult := range slack.groupResults {
		result, _ := groupResult.(*ConsumerGroupStatus)

		var emoji, color string
		switch result.Status {
		case StatusOK:
			emoji = ":white_check_mark:"
			color = "good"
		case StatusNotFound, StatusWarning:
			emoji = ":question:"
			color = "warning"
		default:
			emoji = ":x:"
			color = "danger"
		}

		title := "Burrow monitoring report"
		fallback := fmt.Sprintf("%s is %s", result.Group, result.Status)
		pretext := fmt.Sprintf("%s Group `%s` in Cluster `%s` is *%s*", emoji, result.Group, result.Cluster, result.Status)

		detailedBody := fmt.Sprintf("*Detail:* Total Partition = `%d` Fail Partition = `%d`\n",
			result.TotalPartitions, len(result.Partitions))

		for _, p := range result.Partitions {
			detailedBody += fmt.Sprintf("*%s* *[%s:%d]* (%d, %d) -> (%d, %d)\n",
				p.Status.String(), p.Topic, p.Partition, p.Start.Offset, p.Start.Lag, p.End.Offset, p.End.Lag)
		}

		results[i] = attachment{
			Color:    color,
			Title:    title,
			Fallback: fallback,
			Pretext:  pretext,
			Text:     detailedBody,
			MrkdwnIn: []string{"text", "pretext"},
		}

		i++
	}
	slack.groupResults = make(map[string]Message)

	slackMessage := &SlackMessage{
		Channel:     slack.channel,
		Username:    slack.username,
		IconUrl:     slack.iconUrl,
		IconEmoji:   slack.iconEmoji,
		Attachments: results,
	}

	return slack.postToSlack(slackMessage)
}

func (slack *SlackNotifier) postToSlack(slackMessage *SlackMessage) error {
	data, err := json.Marshal(slackMessage)
	if err != nil {
		log.Errorf("Unable to marshal slack payload:", err)
		return err
	}
	log.Debugf("struct = %+v, json = %s", slackMessage, string(data))

	b := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", slack.url, b)
	req.Header.Set("Content-Type", "application/json")

	if res, err := slack.httpClient.Do(req); err != nil {
		log.Errorf("Unable to send data to slack:%+v", err)
		return err
	} else {
		defer res.Body.Close()
		statusCode := res.StatusCode
		if statusCode != 200 {
			body, _ := ioutil.ReadAll(res.Body)
			log.Errorf("Unable to notify slack:", string(body))
			return errors.New("Send to Slack failed")
		} else {
			log.Debug("Slack notification sent")
			return nil
		}
	}
}
