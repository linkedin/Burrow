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
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"gopkg.in/gomail.v2"
	"net"
	"strconv"
)

func fixtureEmailNotifier() *EmailNotifier {
	module := EmailNotifier{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{}

	viper.Reset()
	viper.Set("notifier.test.class-name", "email")
	viper.Set("notifier.test.template-open", "template_open")
	viper.Set("notifier.test.template-close", "template_close")
	viper.Set("notifier.test.send-close", false)
	viper.Set("notifier.test.server", "test.example.com")
	viper.Set("notifier.test.port", 587)
	viper.Set("notifier.test.from", "sender@example.com")
	viper.Set("notifier.test.to", "receiver@example.com")
	viper.Set("notifier.test.noverify", true)

	return &module
}

func TestEmailNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(EmailNotifier))
	assert.Implements(t, (*Module)(nil), new(EmailNotifier))
}

func TestEmailNotifier_Configure(t *testing.T) {
	module := fixtureEmailNotifier()

	module.Configure("test", "notifier.test")
	assert.NotNil(t, module.smtpDialer, "Expected smtpDialer")
}

func TestEmailNotifier_Configure_BasicAuth(t *testing.T) {
	module := fixtureEmailNotifier()
	viper.Set("notifier.test.auth-type", "plain")
	viper.Set("notifier.test.username", "user")
	viper.Set("notifier.test.password", "pass")

	module.Configure("test", "notifier.test")
}

func TestEmailNotifier_Configure_CramMD5(t *testing.T) {
	module := fixtureEmailNotifier()
	viper.Set("notifier.test.auth-type", "CramMD5")
	viper.Set("notifier.test.username", "user")
	viper.Set("notifier.test.password", "pass")

	module.Configure("test", "notifier.test")
}

func TestEmailNotifier_StartStop(t *testing.T) {
	module := fixtureEmailNotifier()
	module.Configure("test", "notifier.test")

	err := module.Start()
	assert.Nil(t, err, "Expected Start to return no error")
	err = module.Stop()
	assert.Nil(t, err, "Expected Stop to return no error")
}

func TestEmailNotifier_AcceptConsumerGroup(t *testing.T) {
	module := fixtureEmailNotifier()
	module.Configure("test", "notifier.test")

	// Should always return true
	assert.True(t, module.AcceptConsumerGroup(&protocol.ConsumerGroupStatus{}), "Expected any status to return True")
}

func TestEmailNotifier_Notify_Open(t *testing.T) {
	module := fixtureEmailNotifier()
	viper.Set("notifier.test.auth-type", "plain")
	viper.Set("notifier.test.username", "user")
	viper.Set("notifier.test.password", "pass")

	module.sendMailFunc = func(m *gomail.Message) error {
		d := module.smtpDialer
		serverWithPort := net.JoinHostPort(d.Host, strconv.Itoa(d.Port))
		assert.Equalf(t, "test.example.com:587", serverWithPort, "Expected server to be test.example.com:587, not %v", serverWithPort)
		assert.NotNil(t, d.Auth, "Expected auth to not be nil")
		assert.Equalf(t, "sender@example.com", module.from, "Expected from to be sender@example.com, not %v", module.from)
		assert.Lenf(t, []string{module.to}, 1, "Expected one to address, not %v", len([]string{module.to}))
		assert.Equalf(t, "receiver@example.com", []string{module.to}[0], "Expected to to be receiver@example.com, not %v", []string{module.to}[0])

		mimeHeader := m.GetHeader("MIME-version")
		subHeader := m.GetHeader("Subject")

		assert.Equalf(t, []string([]string{"[Burrow] Kafka Consumer Lag Alert"}), subHeader, "Expected subject to be [Burrow] Kafka Consumer Lag Alert, not %v", subHeader)
		assert.Equalf(t, []string([]string{"1.0"}), mimeHeader, "Expected MimeVersion of 1.0", mimeHeader)
		assert.NotNil(t, m, "Expected auth to not be nil")
		assert.True(t, d.TLSConfig.InsecureSkipVerify)

		return nil
	}

	// Template for testing
	module.templateOpen, _ = template.New("test").Parse("Subject: [Burrow] Kafka Consumer Lag Alert\n\n" +
		"MIME-version: 1.0\n" +
		"The Kafka consumer groups you are monitoring are currently showing problems. The following groups are in a problem state (groups not listed are OK):\n\n" +
		"Cluster:  {{.Result.Cluster}}\n" +
		"Group:    {{.Result.Group}}\n" +
		"Status:   {{.Result.Status.String}}\n" +
		"Complete: {{.Result.Complete}}\n" +
		"Errors:   {{len .Result.Partitions}} partitions have problems\n" +
		"{{range .Result.Partitions}}          {{.Status.String}} {{.Topic}}:{{.Partition}} ({{.Start.Timestamp}}, {{.Start.Offset}}, {{.Start.Lag}}) -> ({{.End.Timestamp}}, {{.End.Offset}}, {{.End.Lag}})\n" +
		"{{end}}")

	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), false)
}

func TestEmailNotifier_Notify_Close(t *testing.T) {
	module := fixtureEmailNotifier()

	module.sendMailFunc = func(m *gomail.Message) error {
		d := module.smtpDialer
		serverWithPort := net.JoinHostPort(d.Host, strconv.Itoa(d.Port))

		assert.Equalf(t, "test.example.com:587", serverWithPort, "Expected server to be test.example.com:587, not %v", serverWithPort)
		assert.Nil(t, d.Auth, "Expected auth to be nil")
		assert.Equalf(t, "sender@example.com", module.from, "Expected from to be sender@example.com, not %v", module.from)
		assert.Lenf(t, []string{module.to}, 1, "Expected one to address, not %v", len([]string{module.to}))
		assert.Equalf(t, "receiver@example.com", []string{module.to}[0], "Expected to to be receiver@example.com, not %v", []string{module.to}[0])

		mimeHeader := m.GetHeader("MIME-version")
		subHeader := m.GetHeader("Subject")

		assert.Equalf(t, []string([]string{"[Burrow] Kafka Consumer Healthy"}), subHeader, "Expected subject to be [Burrow] Kafka Consumer Healthy, not %v", subHeader)
		assert.Equalf(t, []string([]string(nil)), mimeHeader, "Expected empty MimeVersion, not %v", mimeHeader)
		assert.NotNil(t, m, "Expected auth to not be nil")
		assert.True(t, d.TLSConfig.InsecureSkipVerify)

		return nil
	}

	// Template for testing
	module.templateClose, _ = template.New("test").Parse("Subject: [Burrow] Kafka Consumer Healthy\n\n" +
		"Content-Type: text/html\n" +
		"Consumer is now in a healthy state" +
		"Cluster:  {{.Result.Cluster}}\n" +
		"Group:    {{.Result.Group}}\n" +
		"Status:   {{.Result.Status.String}}\n" +
		"Complete: {{.Result.Complete}}\n" +
		"{{range .Result.Partitions}}          {{.Status.String}} {{.Topic}}:{{.Partition}} ({{.Start.Timestamp}}, {{.Start.Offset}}, {{.Start.Lag}}) -> ({{.End.Timestamp}}, {{.End.Offset}}, {{.End.Lag}})\n" +
		"{{end}}")

	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	// Test appending file that doesn't exist
	assert.Panics(t, func() { buildRootCAs("/etc/no/file", false) }, "The code did not panic")

	module.Notify(status, "testidstring", time.Now(), true)
}
