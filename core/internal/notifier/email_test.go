package notifier

import (
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
	"net/smtp"
)

func fixtureEmailNotifier() *EmailNotifier {
	module := EmailNotifier{
		Log: zap.NewNop(),
	}
	module.App = &protocol.ApplicationContext{
		Configuration:  &configuration.Configuration{},
	}

	module.App.Configuration.EmailNotifierProfile = make(map[string]*configuration.EmailNotifierProfile)
	module.App.Configuration.EmailNotifierProfile["test_email_profile"] = &configuration.EmailNotifierProfile{
		Server:   "test.example.com",
		Port:     587,
		AuthType: "",
		Username: "",
		Password: "",
		From:     "sender@example.com",
		To:       "receiver@example.com",
	}

	module.App.Configuration.Notifier = make(map[string]*configuration.NotifierConfig)
	module.App.Configuration.Notifier["test"] = &configuration.NotifierConfig{
		ClassName:      "email",
		Profile:        "test_email_profile",
		Timeout:        2,
		Keepalive:      10,
		TemplateOpen:   "template_open",
		TemplateClose:  "template_close",
		SendClose:      false,
	}

	return &module
}

func TestEmailNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(EmailNotifier))
	assert.Implements(t, (*Module)(nil), new(EmailNotifier))
}

func TestEmailNotifier_Configure(t *testing.T) {
	module := fixtureEmailNotifier()

	module.Configure("test")
	assert.Equalf(t, "test.example.com:587", module.serverWithPort, "Expected serverWithPort to be test.example.com:587, not %v", module.serverWithPort)
	assert.NotNil(t, module.sendMailFunc, "Expected sendMailFunc to get set to smtp.SendMail")
	assert.Nil(t, module.auth, "Expected auth to be set to nil")
}

func TestEmailNotifier_Configure_BasicAuth(t *testing.T) {
	module := fixtureEmailNotifier()
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].AuthType = "plain"
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].Username = "user"
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].Password = "pass"

	module.Configure("test")
	assert.NotNil(t, module.auth, "Expected auth to be set")
}

func TestEmailNotifier_Configure_CramMD5(t *testing.T) {
	module := fixtureEmailNotifier()
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].AuthType = "CramMD5"
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].Username = "user"
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].Password = "pass"

	module.Configure("test")
	assert.NotNil(t, module.auth, "Expected auth to be set")
}

func TestEmailNotifier_StartStop(t *testing.T) {
	module := fixtureEmailNotifier()
	module.Configure("test")

	err := module.Start()
	assert.Nil(t, err, "Expected Start to return no error")
	err = module.Stop()
	assert.Nil(t, err, "Expected Stop to return no error")
}

func TestEmailNotifier_AcceptConsumerGroup(t *testing.T) {
	module := fixtureEmailNotifier()
	module.Configure("test")

	// Should always return true
	assert.True(t, module.AcceptConsumerGroup(&protocol.ConsumerGroupStatus{}), "Expected any status to return True")
}

func TestEmailNotifier_Notify_Open(t *testing.T) {
	module := fixtureEmailNotifier()
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].AuthType = "plain"
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].Username = "user"
	module.App.Configuration.EmailNotifierProfile["test_email_profile"].Password = "pass"

	module.sendMailFunc = func (server string, auth smtp.Auth, from string, to []string, bytesToSend []byte) error {
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

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusWarning,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), false)
}

func TestEmailNotifier_Notify_Close(t *testing.T) {
	module := fixtureEmailNotifier()

	module.sendMailFunc = func (server string, auth smtp.Auth, from string, to []string, bytesToSend []byte) error {
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

	module.Configure("test")

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
	}

	module.Notify(status, "testidstring", time.Now(), true)
}