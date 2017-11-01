package notifier

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strings"
	"text/template"
	"time"

	"go.uber.org/zap"
	"github.com/pborman/uuid"

	"github.com/linkedin/Burrow/core/configuration"
	"github.com/linkedin/Burrow/core/protocol"
)

type HttpNotifier struct {
	App                  *protocol.ApplicationContext
	Log                  *zap.Logger

	name                 string
	myConfiguration      *configuration.NotifierConfig
	profile              *configuration.HttpNotifierProfile
	extras               map[string]string

	groupIds             map[string]map[string]*Event
	groupWhitelist       *regexp.Regexp

	templateParseFunc    func (...string) (*template.Template, error)
	sendNotificationFunc func (*protocol.ConsumerGroupStatus, *Event, bool, *zap.Logger)

	templateOpen         *template.Template
	templateClose        *template.Template

	HttpClient           *http.Client
}

type Event struct {
	Id    string
	Start time.Time
	Last  time.Time
}

func (module *HttpNotifier) Configure(name string) {
	module.name = name
	module.myConfiguration = module.App.Configuration.Notifier[name]
	module.groupIds = make(map[string]map[string]*Event)

	// Set the function for parsing templates (configurable to enable testing)
	if module.templateParseFunc == nil {
		module.templateParseFunc = template.New(name).Funcs(helperFunctionMap).ParseFiles
	}

	// Set the function for parsing templates (configurable to enable testing)
	if module.sendNotificationFunc == nil {
		module.sendNotificationFunc = module.sendNotification
	}

	// Set defaults for configs if needed
	if module.App.Configuration.Notifier[module.name].Interval == 0 {
		module.App.Configuration.Notifier[module.name].Interval = 60
	}
	if module.App.Configuration.Notifier[module.name].Threshold == 0 {
		// protocol.StatusWarning
		module.App.Configuration.Notifier[module.name].Threshold = 2
	}
	if module.App.Configuration.Notifier[module.name].Timeout == 0 {
		module.App.Configuration.Notifier[module.name].Timeout = 5
	}
	if module.App.Configuration.Notifier[module.name].Keepalive == 0 {
		module.App.Configuration.Notifier[module.name].Keepalive = 300
	}

	module.extras = make(map[string]string)
	for _, extra := range module.myConfiguration.Extras {
		parts := strings.Split(extra, "=")
		if len(parts) < 2 {
			module.Log.Panic("extras badly formatted")
			panic(errors.New("configuration error"))
		}
		module.extras[parts[0]] = strings.Join(parts[1:], "=")
	}

	if profile, ok := module.App.Configuration.HttpNotifierProfile[module.myConfiguration.Profile]; ok {
		module.profile = profile
	} else {
		module.Log.Panic("unknown HTTP notifier profile")
		panic(errors.New("configuration error"))
	}

	// Compile the whitelist for the consumer groups to notify for
	if module.App.Configuration.Notifier[module.name].GroupWhitelist != "" {
		re, err := regexp.Compile(module.App.Configuration.Notifier[module.name].GroupWhitelist)
		if err != nil {
			module.Log.Panic("Failed to compile group whitelist")
			panic(err)
		}
		module.groupWhitelist = re
	}

	// Compile the templates
	tmpl, err := module.templateParseFunc(module.myConfiguration.TemplateOpen)
	if err != nil {
		module.Log.Panic("Failed to compile TemplateOpen", zap.Error(err))
		panic(err)
	}
	module.templateOpen = tmpl.Templates()[0]

	if module.myConfiguration.SendClose {
		tmpl, err = module.templateParseFunc(module.myConfiguration.TemplateClose)
		if err != nil {
			module.Log.Panic("Failed to compile TemplateClose", zap.Error(err))
			panic(err)
		}
		module.templateClose = tmpl.Templates()[0]
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

func (module *HttpNotifier) Start() error {
	// HTTP notifier does not have a running component - no start needed
	return nil
}

func (module *HttpNotifier) Stop() error {
	// HTTP notifier does not have a running component - no stop needed
	return nil
}

func (module *HttpNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	if int(status.Status) < module.myConfiguration.Threshold {
		return false
	}

	// No whitelist means everything passes
	if module.groupWhitelist == nil {
		return true
	}
	return module.groupWhitelist.MatchString(status.Group)
}

func (module *HttpNotifier) Notify (status *protocol.ConsumerGroupStatus) {
	currentTime := time.Now()

	if _, ok := module.groupIds[status.Cluster]; !ok {
		// Create the cluster map
		module.groupIds[status.Cluster] = make(map[string]*Event)
	}

	stateGood := (status.Status == protocol.StatusOK) || (int(status.Status) < module.myConfiguration.Threshold)

	if _, ok := module.groupIds[status.Cluster][status.Group]; !ok {
		if stateGood {
			return
		}

		// Create Event and Id
		eventId := uuid.NewRandom()
		module.groupIds[status.Cluster][status.Group] = &Event{
			Id:    eventId.String(),
			Start: currentTime,
		}
	}

	event := module.groupIds[status.Cluster][status.Group]
	logger := module.Log.With(
		zap.String("cluster", status.Cluster),
		zap.String("group", status.Group),
		zap.String("id", event.Id),
		zap.String("status", status.Status.String()),
	)
	if stateGood {
		if module.myConfiguration.SendClose {
			module.sendNotificationFunc(status, event, stateGood, logger)
		}

		// Remove ID for group that is now clear
		delete(module.groupIds[status.Cluster], status.Group)
	} else {
		// Only send the notification if it's been at least our Interval since the last one for this group
		if currentTime.Sub(event.Last) > (time.Duration(module.myConfiguration.Interval) * time.Second) {
			module.sendNotificationFunc(status, event, stateGood, logger)
			event.Last = currentTime
		}
	}
}

func (module *HttpNotifier) sendNotification (status *protocol.ConsumerGroupStatus, event *Event, stateGood bool, logger *zap.Logger) {
	var tmpl *template.Template
	var method string
	var url string

	if stateGood {
		tmpl = module.templateClose
		method = module.profile.MethodClose
		url = module.profile.UrlClose
	} else {
		tmpl = module.templateOpen
		method = module.profile.MethodOpen
		url = module.profile.UrlOpen
	}

	bytesToSend := new(bytes.Buffer)
	err := tmpl.Execute(bytesToSend, struct {
		Cluster    string
		Group      string
		Id         string
		Start      time.Time
		Extras     map[string]string
		Result     protocol.ConsumerGroupStatus
	}{
		Cluster:    status.Cluster,
		Group:      status.Group,
		Id:         event.Id,
		Start:      event.Start,
		Extras:     module.extras,
		Result:     *status,
	})
	if err != nil {
		logger.Error("failed to assemble message", zap.Error(err))
		return
	}

	// Send POST to HTTP endpoint
	req, err := http.NewRequest(method, url, bytesToSend)
	req.Header.Set("Content-Type", "application/json")

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

	return
}
