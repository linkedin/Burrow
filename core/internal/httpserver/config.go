/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package httpserver

import (
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/linkedin/Burrow/core/configuration"
)

func (hc *Coordinator) configMain(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Build JSON structs for config
	configGeneral := HTTPResponseConfigGeneral{
		PIDFile:       hc.App.Configuration.General.PIDFile,
		StdoutLogfile: hc.App.Configuration.General.StdoutLogfile,
	}
	configLogging := HTTPResponseConfigLogging{
		Filename:       hc.App.Configuration.Logging.Filename,
		MaxSize:        hc.App.Configuration.Logging.MaxSize,
		MaxBackups:     hc.App.Configuration.Logging.MaxBackups,
		MaxAge:         hc.App.Configuration.Logging.MaxAge,
		UseLocalTime:   hc.App.Configuration.Logging.UseLocalTime,
		UseCompression: hc.App.Configuration.Logging.UseCompression,
		Level:          hc.App.Configuration.Logging.Level,
	}
	configZookeeper := HTTPResponseConfigZookeeper{
		Servers:  hc.App.Configuration.Zookeeper.Server,
		Timeout:  hc.App.Configuration.Zookeeper.Timeout,
		RootPath: hc.App.Configuration.Zookeeper.RootPath,
	}
	configHttpServer := make(map[string]HTTPResponseConfigHttpServer)
	for name, cfg := range hc.App.Configuration.HttpServer {
		configHttpServer[name] = HTTPResponseConfigHttpServer{
			Address:         cfg.Address,
			Timeout:         cfg.Timeout,
			TLS:             cfg.TLS,
			TLSCAFilePath:   cfg.TLSCAFilePath,
			TLSCertFilePath: cfg.TLSCertFilePath,
			TLSKeyFilePath:  cfg.TLSKeyFilePath,
		}
	}

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, HTTPResponseConfigMain{
		Error:      false,
		Message:    "main config returned",
		General:    configGeneral,
		Logging:    configLogging,
		Zookeeper:  configZookeeper,
		HttpServer: configHttpServer,
		Request:    requestInfo,
	})
}

func (hc *Coordinator) writeModuleListResponse(w http.ResponseWriter, r *http.Request, coordinator string, modules []string) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, HTTPResponseConfigModuleList{
		Error:       false,
		Message:     "module list returned",
		Request:     requestInfo,
		Coordinator: coordinator,
		Modules:     modules,
	})
}

func (hc *Coordinator) configStorageList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := make([]string, len(hc.App.Configuration.Storage))
	i := 0
	for name := range hc.App.Configuration.Storage {
		modules[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "storage", modules)
}

func (hc *Coordinator) configConsumerList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := make([]string, len(hc.App.Configuration.Consumer))
	i := 0
	for name := range hc.App.Configuration.Consumer {
		modules[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "consumer", modules)
}

func (hc *Coordinator) configClusterList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := make([]string, len(hc.App.Configuration.Cluster))
	i := 0
	for name := range hc.App.Configuration.Cluster {
		modules[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "cluster", modules)
}

func (hc *Coordinator) configEvaluatorList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := make([]string, len(hc.App.Configuration.Evaluator))
	i := 0
	for name := range hc.App.Configuration.Evaluator {
		modules[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "evaluator", modules)
}

func (hc *Coordinator) configNotifierList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := make([]string, len(hc.App.Configuration.Notifier))
	i := 0
	for name := range hc.App.Configuration.Notifier {
		modules[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "notifier", modules)
}

func (hc *Coordinator) configStorageDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if cfg, ok := hc.App.Configuration.Storage[params.ByName("name")]; !ok {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "storage module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, HTTPResponseConfigModuleDetail{
			Error:   false,
			Message: "storage module detail returned",
			Module: HTTPResponseConfigModuleStorage{
				ClassName:      cfg.ClassName,
				Intervals:      cfg.Intervals,
				MinDistance:    cfg.MinDistance,
				GroupWhitelist: cfg.GroupWhitelist,
				ExpireGroup:    cfg.ExpireGroup,
			},
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) configConsumerDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if cfg, ok := hc.App.Configuration.Consumer[params.ByName("name")]; !ok {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "consumer module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, HTTPResponseConfigModuleDetail{
			Error:   false,
			Message: "consumer module detail returned",
			Module: HTTPResponseConfigModuleConsumer{
				ClassName:        cfg.ClassName,
				Cluster:          cfg.Cluster,
				Servers:          cfg.Servers,
				GroupWhitelist:   cfg.GroupWhitelist,
				ZookeeperPath:    cfg.ZookeeperPath,
				ZookeeperTimeout: cfg.ZookeeperTimeout,
				ClientProfile:    getClientProfile(cfg.ClientProfile, hc.App.Configuration.ClientProfile[cfg.ClientProfile]),
				OffsetsTopic:     cfg.OffsetsTopic,
				StartLatest:      cfg.StartLatest,
			},
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) configEvaluatorDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if cfg, ok := hc.App.Configuration.Evaluator[params.ByName("name")]; !ok {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "evaluator module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, HTTPResponseConfigModuleDetail{
			Error:   false,
			Message: "evaluator module detail returned",
			Module: HTTPResponseConfigModuleEvaluator{
				ClassName:   cfg.ClassName,
				ExpireCache: cfg.ExpireCache,
			},
			Request: requestInfo,
		})
	}
}

func getHttpNotifierProfile(name string, profile *configuration.HttpNotifierProfile) HTTPResponseConfigHttpNotifierProfile {
	return HTTPResponseConfigHttpNotifierProfile{
		Name:        name,
		UrlOpen:     profile.UrlOpen,
		UrlClose:    profile.UrlClose,
		MethodOpen:  profile.MethodOpen,
		MethodClose: profile.MethodClose,
	}
}

func getEmailNotifierProfile(name string, profile *configuration.EmailNotifierProfile) HTTPResponseConfigEmailNotifierProfile {
	return HTTPResponseConfigEmailNotifierProfile{
		Name:     name,
		Server:   profile.Server,
		Port:     profile.Port,
		AuthType: profile.AuthType,
		Username: profile.Username,
		From:     profile.From,
		To:       profile.To,
	}
}

func getSlackNotifierProfile(name string, profile *configuration.SlackNotifierProfile) HTTPResponseConfigSlackNotifierProfile {
	return HTTPResponseConfigSlackNotifierProfile{
		Name:      name,
		Channel:   profile.Channel,
		Username:  profile.Username,
		IconUrl:   profile.IconUrl,
		IconEmoji: profile.IconEmoji,
	}
}

func (hc *Coordinator) configNotifierDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if cfg, ok := hc.App.Configuration.Notifier[params.ByName("name")]; !ok {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "notifier module not found")
	} else {
		// Get the right profile structure
		var profile interface{}
		switch cfg.ClassName {
		case "http":
			profile = getHttpNotifierProfile(cfg.Profile, hc.App.Configuration.HttpNotifierProfile[cfg.Profile])
		case "email":
			profile = getEmailNotifierProfile(cfg.Profile, hc.App.Configuration.EmailNotifierProfile[cfg.Profile])
		case "slack":
			profile = getSlackNotifierProfile(cfg.Profile, hc.App.Configuration.SlackNotifierProfile[cfg.Profile])
		}
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, HTTPResponseConfigModuleDetail{
			Error:   false,
			Message: "notifier module detail returned",
			Module: HTTPResponseConfigModuleNotifier{
				ClassName:      cfg.ClassName,
				GroupWhitelist: cfg.GroupWhitelist,
				Interval:       cfg.Interval,
				Threshold:      cfg.Threshold,
				Timeout:        cfg.Timeout,
				Keepalive:      cfg.Keepalive,
				TemplateOpen:   cfg.TemplateOpen,
				TemplateClose:  cfg.TemplateClose,
				Extras:         cfg.Extras,
				SendClose:      cfg.SendClose,
				Profile:        profile,
			},
			Request: requestInfo,
		})
	}
}
