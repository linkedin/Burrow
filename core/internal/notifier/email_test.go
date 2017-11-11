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
	"net/smtp"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
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

	return &module
}

func TestEmailNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(EmailNotifier))
	assert.Implements(t, (*Module)(nil), new(EmailNotifier))
}

func TestEmailNotifier_Configure(t *testing.T) {
	module := fixtureEmailNotifier()

	module.Configure("test", "notifier.test")
	assert.Equalf(t, "test.example.com:587", module.serverWithPort, "Expected serverWithPort to be test.example.com:587, not %v", module.serverWithPort)
	assert.NotNil(t, module.sendMailFunc, "Expected sendMailFunc to get set to smtp.SendMail")
	assert.Nil(t, module.auth, "Expected auth to be set to nil")
}

func TestEmailNotifier_Configure_BasicAuth(t *testing.T) {
	module := fixtureEmailNotifier()
	viper.Set("notifier.test.auth-type", "plain")
	viper.Set("notifier.test.username", "user")
	viper.Set("notifier.test.password", "pass")

	module.Configure("test", "notifier.test")
	assert.NotNil(t, module.auth, "Expected auth to be set")
}

func TestEmailNotifier_Configure_CramMD5(t *testing.T) {
	module := fixtureEmailNotifier()
	viper.Set("notifier.test.auth-type", "CramMD5")
	viper.Set("notifier.test.username", "user")
	viper.Set("notifier.test.password", "pass")

	module.Configure("test", "notifier.test")
	assert.NotNil(t, module.auth, "Expected auth to be set")
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

	module.sendMailFunc = func(server string, auth smtp.Auth, from string, to []string, bytesToSend []byte) error {
		assert.Equalf(t, "test.example.com:587", server, "Expected server to be test.example.com:587, not %v", server)
		assert.NotNil(t, auth, "Expected auth to not be nil")
		assert.Equalf(t, "sender@example.com", from, "Expected from to be sender@example.com, not %v", from)
		assert.Lenf(t, to, 1, "Expected one to address, not %v", len(to))
		assert.Equalf(t, "receiver@example.com", to[0], "Expected to to be receiver@example.com, not %v", to[0])
		assert.Equalf(t, []byte("From: sender@example.com\nTo: receiver@example.com\ntestidstring testcluster testgroup WARN"), bytesToSend, "Unexpected bytes, got: %v", bytesToSend)
		return nil
	}

	// Template for testing
	module.templateOpen, _ = template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

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

	module.sendMailFunc = func(server string, auth smtp.Auth, from string, to []string, bytesToSend []byte) error {
		assert.Equalf(t, "test.example.com:587", server, "Expected server to be test.example.com:587, not %v", server)
		assert.Nil(t, auth, "Expected auth to be nil")
		assert.Equalf(t, "sender@example.com", from, "Expected from to be sender@example.com, not %v", from)
		assert.Lenf(t, to, 1, "Expected one to address, not %v", len(to))
		assert.Equalf(t, "receiver@example.com", to[0], "Expected to to be receiver@example.com, not %v", to[0])
		assert.Equalf(t, []byte("From: sender@example.com\nTo: receiver@example.com\ntestidstring testcluster testgroup OK"), bytesToSend, "Unexpected bytes, got: %v", bytesToSend)
		return nil
	}

	// Template for testing
	module.templateClose, _ = template.New("test").Parse("{{.Id}} {{.Cluster}} {{.Group}} {{.Result.Status}}")

	module.Configure("test", "notifier.test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), true)
}
