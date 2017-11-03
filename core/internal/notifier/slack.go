package notifier

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"text/template"
	"time"

	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type SlackNotifier struct {
	App             *protocol.ApplicationContext
	Log             *zap.Logger

	groupWhitelist  *regexp.Regexp
	extras          map[string]string
	templateOpen    *template.Template
	templateClose   *template.Template

	name            string
	myConfiguration *configuration.NotifierConfig
	profile         *configuration.SlackNotifierProfile

	HttpClient      *http.Client
	postURL         string
}

func (module *SlackNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]

	// Set the Slack chat.postMessage URL (unless it's been set for testing)
	if module.postURL == "" {
		module.postURL = "https://slack.com/api/chat.postMessage"
	}
	if profile, ok := module.App.Configuration.SlackNotifierProfile[module.myConfiguration.Profile]; ok {
		module.profile = profile
	} else {
		module.Log.Panic("unknown Slack notifier profile")
		panic(errors.New("configuration error"))
	}

	if module.profile.Token == "" {
		module.Log.Panic("missing auth token")
		panic(errors.New("configuration error"))
	}

	if module.profile.Channel == "" {
		module.Log.Panic("missing channel")
		panic(errors.New("configuration error"))
	}

	// Set defaults for module-specific configs if needed
	if module.App.Configuration.Notifier[module.name].Timeout == 0 {
		module.App.Configuration.Notifier[module.name].Timeout = 5
	}
	if module.App.Configuration.Notifier[module.name].Keepalive == 0 {
		module.App.Configuration.Notifier[module.name].Keepalive = 300
	}

	// Set up HTTP client
	module.HttpClient = &http.Client{
		Timeout: time.Duration(module.myConfiguration.Timeout) * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				KeepAlive: time.Duration(module.myConfiguration.Keepalive) * time.Second,
			}).Dial,
			Proxy: http.ProxyFromEnvironment,
		},
	}
}

func (module *SlackNotifier) Start() error {
	// Slack notifier does not have a running component - no start needed
	return nil
}

func (module *SlackNotifier) Stop() error {
	// Slack notifier does not have a running component - no start needed
	return nil
}

func (module *SlackNotifier) GetName() string {
	return module.name
}

func (module *SlackNotifier) GetConfig() *configuration.NotifierConfig {
	return module.myConfiguration
}

func (module *SlackNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *SlackNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *SlackNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

type SlackMessage struct {
	Channel     string `json:"channel"`
	Username    string `json:"username,omitempty"`
	IconUrl     string `json:"icon-url,omitempty"`
	IconEmoji   string `json:"icon-emoji,omitempty"`
	Text        string `json:"text,omitempty"`
}

func (module *SlackNotifier) Notify (status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {
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

	messageBytes, err := ExecuteTemplate(tmpl, module.extras, status, eventId, startTime)
	if err != nil {
		logger.Error("failed to assemble message", zap.Error(err))
		return
	}

	// Encode JSON payload
	data, err := json.Marshal(SlackMessage{
		Channel:   module.profile.Channel,
		Username:  module.profile.Username,
		IconUrl:   module.profile.IconUrl,
		IconEmoji: module.profile.IconEmoji,
		Text:      messageBytes.String(),
	})
	if err != nil {
		logger.Error("failed to encode", zap.Error(err))
		return
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest("POST", module.postURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer " + module.profile.Token)

	resp, err := module.HttpClient.Do(req)
	if err != nil {
		logger.Error("failed to send", zap.Error(err))
		return
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if (resp.StatusCode >= 200) && (resp.StatusCode <= 299) {
		logger.Debug("sent")
	} else {
		logger.Error("failed to send", zap.Int("response", resp.StatusCode))
	}
}