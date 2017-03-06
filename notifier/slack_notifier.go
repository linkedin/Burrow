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
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/linkedin/Burrow/protocol"
	"io/ioutil"
	"net/http"
	"time"
)

type SlackNotifier struct {
	RefreshTicker *time.Ticker
	Url           string
	Channel       string
	Username      string
	IconUrl       string
	IconEmoji     string
	Threshold     int
	HttpClient    *http.Client
	Groups        []string
	groupMsgs     map[string]Message
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
	return int(msg.Status) < slack.Threshold
}

func (slack *SlackNotifier) Notify(msg Message) error {
	if slack.Ignore(msg) {
		return nil
	}

	if slack.groupMsgs == nil {
		slack.groupMsgs = make(map[string]Message)
	}

	for _, group := range slack.Groups {
		clusterGroup := fmt.Sprintf("%s,%s", msg.Cluster, msg.Group)
		if clusterGroup == group {
			slack.groupMsgs[clusterGroup] = msg
		}
	}
	if len(slack.Groups) == len(slack.groupMsgs) {
		return slack.sendConsumerGroupStatusNotify()
	}
	return nil
}

func (slack *SlackNotifier) sendConsumerGroupStatusNotify() error {
	msgs := make([]attachment, len(slack.Groups))
	i := 0
	for _, msg := range slack.groupMsgs {

		var emoji, color string
		switch msg.Status {
		case protocol.StatusOK:
			emoji = ":white_check_mark:"
			color = "good"
		case protocol.StatusNotFound, protocol.StatusWarning:
			emoji = ":question:"
			color = "warning"
		default:
			emoji = ":x:"
			color = "danger"
		}

		title := "Burrow monitoring report"
		fallback := fmt.Sprintf("%s is %s", msg.Group, msg.Status)
		pretext := fmt.Sprintf("%s Group `%s` in Cluster `%s` is *%s*", emoji, msg.Group, msg.Cluster, msg.Status)

		detailedBody := fmt.Sprintf("*Detail:* Total Partition = `%d` Fail Partition = `%d`\n",
			msg.TotalPartitions, len(msg.Partitions))

		for _, p := range msg.Partitions {
			detailedBody += fmt.Sprintf("*%s* *[%s:%d]* (%d, %d) -> (%d, %d)\n",
				p.Status.String(), p.Topic, p.Partition, p.Start.Offset, p.Start.Lag, p.End.Offset, p.End.Lag)
		}

		msgs[i] = attachment{
			Color:    color,
			Title:    title,
			Fallback: fallback,
			Pretext:  pretext,
			Text:     detailedBody,
			MrkdwnIn: []string{"text", "pretext"},
		}

		i++
	}
	slack.groupMsgs = make(map[string]Message)

	slackMessage := &SlackMessage{
		Channel:     slack.Channel,
		Username:    slack.Username,
		IconUrl:     slack.IconUrl,
		IconEmoji:   slack.IconEmoji,
		Attachments: msgs,
	}

	return slack.postToSlack(slackMessage)
}

func (slack *SlackNotifier) postToSlack(slackMessage *SlackMessage) error {
	data, err := json.Marshal(slackMessage)
	if err != nil {
		log.Errorf("Unable to marshal slack payload:%+v", err)
		return err
	}
	log.Debugf("struct = %+v, json = %s", slackMessage, string(data))

	b := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", slack.Url, b)
	req.Header.Set("Content-Type", "application/json")

	if res, err := slack.HttpClient.Do(req); err != nil {
		log.Errorf("Unable to send data to slack:%+v", err)
		return err
	} else {
		defer res.Body.Close()
		statusCode := res.StatusCode
		if statusCode != 200 {
			body, _ := ioutil.ReadAll(res.Body)
			log.Errorf("Unable to notify slack:%s", string(body))
			return errors.New("Send to Slack failed")
		} else {
			log.Debug("Slack notification sent")
			return nil
		}
	}
}
