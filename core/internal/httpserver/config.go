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
	"github.com/spf13/viper"
)

func (hc *Coordinator) configMain(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Build JSON structs for config
	configGeneral := httpResponseConfigGeneral{
		PIDFile:                  viper.GetString("general.pidfile"),
		StdoutLogfile:            viper.GetString("general.stdout-logfile"),
		AccessControlAllowOrigin: viper.GetString("general.access-control-allow-origin"),
	}
	configLogging := httpResponseConfigLogging{
		Filename:       viper.GetString("logging.filename"),
		MaxSize:        viper.GetInt("logging.maxsize"),
		MaxBackups:     viper.GetInt("logging.maxbackups"),
		MaxAge:         viper.GetInt("logging.maxage"),
		UseLocalTime:   viper.GetBool("logging.use-localtime"),
		UseCompression: viper.GetBool("logging.use-compression"),
		Level:          viper.GetString("logging.level"),
	}
	configZookeeper := httpResponseConfigZookeeper{
		Servers:  viper.GetStringSlice("zookeeper.servers"),
		Timeout:  viper.GetInt("zookeeper.timeout"),
		RootPath: viper.GetString("zookeeper.root-path"),
	}

	servers := viper.GetStringMap("httpserver")
	configHTTPServer := make(map[string]httpResponseConfigHTTPServer)
	for name := range servers {
		configRoot := "httpserver." + name
		configHTTPServer[name] = httpResponseConfigHTTPServer{
			Address: viper.GetString(configRoot + ".address"),
			Timeout: viper.GetInt(configRoot + ".timeout"),
			TLS:     viper.GetString(configRoot + ".tls"),
		}
	}

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseConfigMain{
		Error:      false,
		Message:    "main config returned",
		General:    configGeneral,
		Logging:    configLogging,
		Zookeeper:  configZookeeper,
		HTTPServer: configHTTPServer,
		Request:    requestInfo,
	})
}

func (hc *Coordinator) writeModuleListResponse(w http.ResponseWriter, r *http.Request, coordinator string, modules []string) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleList{
		Error:       false,
		Message:     "module list returned",
		Request:     requestInfo,
		Coordinator: coordinator,
		Modules:     modules,
	})
}

func (hc *Coordinator) configStorageList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := viper.GetStringMap("storage")
	moduleList := make([]string, len(modules))
	i := 0
	for name := range modules {
		moduleList[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "storage", moduleList)
}

func (hc *Coordinator) configConsumerList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := viper.GetStringMap("consumer")
	moduleList := make([]string, len(modules))
	i := 0
	for name := range modules {
		moduleList[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "consumer", moduleList)
}

func (hc *Coordinator) configClusterList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := viper.GetStringMap("cluster")
	moduleList := make([]string, len(modules))
	i := 0
	for name := range modules {
		moduleList[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "cluster", moduleList)
}

func (hc *Coordinator) configEvaluatorList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := viper.GetStringMap("evaluator")
	moduleList := make([]string, len(modules))
	i := 0
	for name := range modules {
		moduleList[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "evaluator", moduleList)
}

func (hc *Coordinator) configNotifierList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	modules := viper.GetStringMap("notifier")
	moduleList := make([]string, len(modules))
	i := 0
	for name := range modules {
		moduleList[i] = name
		i++
	}
	hc.writeModuleListResponse(w, r, "notifier", moduleList)
}

func (hc *Coordinator) configStorageDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	configRoot := "storage." + params.ByName("name")
	if !viper.IsSet(configRoot) {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "storage module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
			Error:   false,
			Message: "storage module detail returned",
			Module: httpResponseConfigModuleStorage{
				ClassName:      viper.GetString(configRoot + ".class-name"),
				Intervals:      viper.GetInt(configRoot + ".intervals"),
				MinDistance:    viper.GetInt64(configRoot + ".min-distance"),
				GroupWhitelist: viper.GetString(configRoot + ".group-whitelist"),
				ExpireGroup:    viper.GetInt64(configRoot + ".expire-group"),
			},
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) configConsumerDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	configRoot := "consumer." + params.ByName("name")
	if !viper.IsSet(configRoot) {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "consumer module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
			Error:   false,
			Message: "consumer module detail returned",
			Module: httpResponseConfigModuleConsumer{
				ClassName:        viper.GetString(configRoot + ".class-name"),
				Cluster:          viper.GetString(configRoot + ".cluster"),
				Servers:          viper.GetStringSlice(configRoot + ".servers"),
				GroupWhitelist:   viper.GetString(configRoot + ".group-whitelist"),
				ZookeeperPath:    viper.GetString(configRoot + ".zookeeper-path"),
				ZookeeperTimeout: int32(viper.GetInt64(configRoot + ".zookeeper-timeout")),
				ClientProfile:    getClientProfile(viper.GetString(configRoot + ".client-profile")),
				OffsetsTopic:     viper.GetString(configRoot + ".offsets-topic"),
				StartLatest:      viper.GetBool(configRoot + ".start-latest"),
			},
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) configEvaluatorDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	configRoot := "evaluator." + params.ByName("name")
	if !viper.IsSet(configRoot) {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "evaluator module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
			Error:   false,
			Message: "evaluator module detail returned",
			Module: httpResponseConfigModuleEvaluator{
				ClassName:   viper.GetString(configRoot + ".class-name"),
				ExpireCache: viper.GetInt64(configRoot + ".expire-cache"),
			},
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) configNotifierHTTP(w http.ResponseWriter, r *http.Request, configRoot string) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
		Error:   false,
		Message: "notifier module detail returned",
		Module: httpResponseConfigModuleNotifierHTTP{
			ClassName:      viper.GetString(configRoot + ".class-name"),
			GroupWhitelist: viper.GetString(configRoot + ".group-whitelist"),
			Interval:       viper.GetInt64(configRoot + ".interval"),
			Threshold:      viper.GetInt(configRoot + ".threshold"),
			Timeout:        viper.GetInt(configRoot + ".timeout"),
			Keepalive:      viper.GetInt(configRoot + ".keepalive"),
			URLOpen:        viper.GetString(configRoot + ".url-open"),
			URLClose:       viper.GetString(configRoot + ".url-close"),
			MethodOpen:     viper.GetString(configRoot + ".method-open"),
			MethodClose:    viper.GetString(configRoot + ".method-close"),
			TemplateOpen:   viper.GetString(configRoot + ".template-open"),
			TemplateClose:  viper.GetString(configRoot + ".template-close"),
			Extras:         viper.GetStringMapString(configRoot + ".extras"),
			SendClose:      viper.GetBool(configRoot + ".send-close"),
			ExtraCa:        viper.GetString(configRoot + ".extra-ca"),
			NoVerify:       viper.GetString(configRoot + ".noverify"),
		},
		Request: requestInfo,
	})
}

func (hc *Coordinator) configNotifierSlack(w http.ResponseWriter, r *http.Request, configRoot string) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
		Error:   false,
		Message: "notifier module detail returned",
		Module: httpResponseConfigModuleNotifierSlack{
			ClassName:      viper.GetString(configRoot + ".class-name"),
			GroupWhitelist: viper.GetString(configRoot + ".group-whitelist"),
			Interval:       viper.GetInt64(configRoot + ".interval"),
			Threshold:      viper.GetInt(configRoot + ".threshold"),
			Timeout:        viper.GetInt(configRoot + ".timeout"),
			Keepalive:      viper.GetInt(configRoot + ".keepalive"),
			TemplateOpen:   viper.GetString(configRoot + ".template-open"),
			TemplateClose:  viper.GetString(configRoot + ".template-close"),
			Extras:         viper.GetStringMapString(configRoot + ".extras"),
			SendClose:      viper.GetBool(configRoot + ".send-close"),
			Channel:        viper.GetString(configRoot + ".channel"),
			Username:       viper.GetString(configRoot + ".username"),
			IconURL:        viper.GetString(configRoot + ".icon-url"),
			IconEmoji:      viper.GetString(configRoot + ".icon-emoji"),
		},
		Request: requestInfo,
	})
}

func (hc *Coordinator) configNotifierEmail(w http.ResponseWriter, r *http.Request, configRoot string) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
		Error:   false,
		Message: "notifier module detail returned",
		Module: httpResponseConfigModuleNotifierEmail{
			ClassName:      viper.GetString(configRoot + ".class-name"),
			GroupWhitelist: viper.GetString(configRoot + ".group-whitelist"),
			Interval:       viper.GetInt64(configRoot + ".interval"),
			Threshold:      viper.GetInt(configRoot + ".threshold"),
			TemplateOpen:   viper.GetString(configRoot + ".template-open"),
			TemplateClose:  viper.GetString(configRoot + ".template-close"),
			Extras:         viper.GetStringMapString(configRoot + ".extras"),
			SendClose:      viper.GetBool(configRoot + ".send-close"),
			Server:         viper.GetString(configRoot + ".server"),
			Port:           viper.GetInt(configRoot + ".port"),
			AuthType:       viper.GetString(configRoot + ".auth-type"),
			Username:       viper.GetString(configRoot + ".username"),
			From:           viper.GetString(configRoot + ".from"),
			To:             viper.GetString(configRoot + ".to"),
			ExtraCa:        viper.GetString(configRoot + ".extra-ca"),
			NoVerify:       viper.GetString(configRoot + ".noverify"),
		},
		Request: requestInfo,
	})
}

func (hc *Coordinator) configNotifierNull(w http.ResponseWriter, r *http.Request, configRoot string) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
		Error:   false,
		Message: "notifier module detail returned",
		Module: httpResponseConfigModuleNotifierNull{
			ClassName:      viper.GetString(configRoot + ".class-name"),
			GroupWhitelist: viper.GetString(configRoot + ".group-whitelist"),
			Interval:       viper.GetInt64(configRoot + ".interval"),
			Threshold:      viper.GetInt(configRoot + ".threshold"),
			TemplateOpen:   viper.GetString(configRoot + ".template-open"),
			TemplateClose:  viper.GetString(configRoot + ".template-close"),
			Extras:         viper.GetStringMapString(configRoot + ".extras"),
			SendClose:      viper.GetBool(configRoot + ".send-close"),
		},
		Request: requestInfo,
	})
}

func (hc *Coordinator) configNotifierDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	configRoot := "notifier." + params.ByName("name")
	if !viper.IsSet(configRoot) {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "notifier module not found")
	} else {
		// Return the right profile structure
		switch viper.GetString(configRoot + ".class-name") {
		case "http":
			hc.configNotifierHTTP(w, r, configRoot)
		case "email":
			hc.configNotifierEmail(w, r, configRoot)
		case "slack":
			hc.configNotifierSlack(w, r, configRoot)
		case "null":
			hc.configNotifierNull(w, r, configRoot)
		}
	}
}
