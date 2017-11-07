/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"github.com/julienschmidt/httprouter"

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
	"go.uber.org/zap/zapcore"
)

type Coordinator struct {
	App         *protocol.ApplicationContext
	Log         *zap.Logger

	router      *httprouter.Router
	servers     map[string]*http.Server
}

func (hc *Coordinator) Configure() {
	hc.router = httprouter.New()

	// If no HTTP server configured, add a default HTTP server that listens on a random port
	if len(hc.App.Configuration.HttpServer) == 0 {
		hc.App.Configuration.HttpServer["default"] = &configuration.HttpServerConfig{
			Address:   ":0",
			TLS:       false,
			Timeout:   300,
		}
	}

	// Validate provided HTTP server configs
	hc.servers = make(map[string]*http.Server)
	for name, cfg := range hc.App.Configuration.HttpServer {
		server := &http.Server{}

		if ! configuration.ValidateHostPort(cfg.Address, true) {
			panic("invalid HTTP server listener address")
		}
		server.Addr = cfg.Address

		if cfg.Timeout == 0 {
			cfg.Timeout = 300
		}
		server.ReadTimeout = time.Duration(cfg.Timeout) * time.Second
		server.ReadHeaderTimeout = time.Duration(cfg.Timeout) * time.Second
		server.WriteTimeout = time.Duration(cfg.Timeout) * time.Second
		server.IdleTimeout = time.Duration(cfg.Timeout) * time.Second

		if cfg.TLS {
			server.TLSConfig = &tls.Config{}

			if cfg.TLSCAFilePath != "" {
				caCert, err := ioutil.ReadFile(cfg.TLSCAFilePath)
				if err != nil {
					panic("cannot read TLS CA file: " + err.Error())
				}
				server.TLSConfig.RootCAs = x509.NewCertPool()
				server.TLSConfig.RootCAs.AppendCertsFromPEM(caCert)
			}

			if cfg.TLSCertFilePath == "" || cfg.TLSKeyFilePath == "" {
				panic("TLS HTTP server specified with missing certificate or key")
			}
			cert, err := tls.LoadX509KeyPair(cfg.TLSCertFilePath, cfg.TLSKeyFilePath)
			if err != nil {
				panic("cannot read TLS certificate or key file: " + err.Error())
			}
			server.TLSConfig.Certificates = []tls.Certificate{cert}
			server.TLSConfig.BuildNameToCertificate()
		}
		hc.servers[name] = server
	}

    // Configure URL routes here

	// This is a catchall for undefined URLs
	hc.router.NotFound = &DefaultHandler{}

	// This is a healthcheck URL. Please don't change it
	hc.router.GET("/burrow/admin", hc.handleAdmin)

	// All valid paths go here
	hc.router.GET("/v3/kafka", hc.handleClusterList)
	hc.router.GET("/v3/kafka/:cluster", hc.handleClusterDetail)
	hc.router.GET("/v3/kafka/:cluster/topic", hc.handleTopicList)
	hc.router.GET("/v3/kafka/:cluster/topic/:topic", hc.handleTopicDetail)
	hc.router.GET("/v3/kafka/:cluster/consumer", hc.handleConsumerList)
	hc.router.GET("/v3/kafka/:cluster/consumer/:consumer", hc.handleConsumerDetail)
	hc.router.GET("/v3/kafka/:cluster/consumer/:consumer/status", hc.handleConsumerStatus)
	hc.router.GET("/v3/kafka/:cluster/consumer/:consumer/lag", hc.handleConsumerStatusComplete)

	hc.router.GET("/v3/config", hc.configMain)
	hc.router.GET("/v3/config/storage", hc.configStorageList)
	hc.router.GET("/v3/config/storage/:name", hc.configStorageDetail)
	hc.router.GET("/v3/config/evaluator", hc.configEvaluatorList)
	hc.router.GET("/v3/config/evaluator/:name", hc.configEvaluatorDetail)
	hc.router.GET("/v3/config/cluster", hc.configClusterList)
	hc.router.GET("/v3/config/cluster/:cluster", hc.handleClusterDetail)
	hc.router.GET("/v3/config/consumer", hc.configConsumerList)
	hc.router.GET("/v3/config/consumer/:name", hc.configConsumerDetail)
	hc.router.GET("/v3/config/notifier", hc.configNotifierList)
	hc.router.GET("/v3/config/notifier/:name", hc.configNotifierDetail)

	// TODO: This should really have authentication protecting it
	hc.router.DELETE("/v3/kafka/:cluster/consumer/:consumer", hc.handleConsumerDelete)
	hc.router.GET("/v3/admin/loglevel", hc.getLogLevel)
	hc.router.POST("/v3/admin/loglevel", hc.setLogLevel)
}

func (hc *Coordinator) Start() error {
	hc.Log.Info("starting")

	// Start listeners
	listeners := make(map[string]net.Listener)
	for name, server := range hc.servers {
		ln, err := net.Listen("tcp", hc.servers[name].Addr)
		if err != nil {
			hc.Log.Error("failed to listen", zap.String("listener", hc.servers[name].Addr), zap.Error(err))
			for _, listenerToClose := range listeners {
				if listenerToClose != nil {
					closeErr := listenerToClose.Close()
					if closeErr != nil {
						hc.Log.Error("could not close listener: %v", zap.Error(closeErr))
					}
				}
			}
			return err
		}
		listeners[name] = tcpKeepAliveListener{
			Keepalive:   server.IdleTimeout,
			TCPListener: ln.(*net.TCPListener),
		}
	}

	// Start the HTTP server on the listeners
	for name, server := range hc.servers {
		go server.Serve(listeners[name])
	}
	return nil
}

func (hc *Coordinator) Stop() error {
	hc.Log.Info("shutdown")

	// Close all servers
	collectedErrors := make([]zapcore.Field, 0)
	for _, server := range hc.servers {
		err := server.Close()
		if err != nil {
			collectedErrors = append(collectedErrors, zap.Error(err))
		}
	}

	if len(collectedErrors) > 0 {
		hc.Log.Error("errors shutting down", collectedErrors...)
		return errors.New("error shutting down HTTP servers")
	} else {
		return nil
	}
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted connections. It's used by ListenAndServe and
// ListenAndServeTLS so dead TCP connections (e.g. closing laptop mid-download) eventually go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
	Keepalive   time.Duration
}
func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}

	if ln.Keepalive > 0 {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(ln.Keepalive)
	}
	return tc, nil
}

func makeRequestInfo(r *http.Request) HTTPResponseRequestInfo {
	hostname, _ := os.Hostname()
	return HTTPResponseRequestInfo{
		URI:  r.URL.Path,
		Host: hostname,
	}
}

func writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, jsonObj interface{}) {
	jsonBytes, err := json.Marshal(jsonObj)
	if err != nil {
		writeErrorResponse(w, r, http.StatusInternalServerError, "could not encode JSON")
		return
	}

	// Headers go here
	w.WriteHeader(statusCode)
	w.Write(jsonBytes)
}

func writeErrorResponse(w http.ResponseWriter, r *http.Request, errValue int, message string) {
	rv := HTTPResponseError{
		Error:   true,
		Message: message,
		Request: makeRequestInfo(r),
	}

	jsonStr, err := json.Marshal(rv)
	if err != nil {
		http.Error(w, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}", http.StatusInternalServerError)
	} else {
		http.Error(w, string(jsonStr), errValue)
	}
}

// This is a catch-all handler for unknown URLs. It should return a 404
type DefaultHandler struct {}
func (handler *DefaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "{\"error\":true,\"message\":\"invalid request type\",\"result\":{}}", http.StatusNotFound)
}

func (hc *Coordinator) handleAdmin(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("GOOD"))
}

func (hc *Coordinator) getLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	requestInfo := makeRequestInfo(r)
	writeResponse(w, r, http.StatusOK, HTTPResponseLogLevel{
		Error:   false,
		Message: "log level returned",
		Level:   hc.App.LogLevel.Level().String(),
		Request: requestInfo,
	})
}

func (hc *Coordinator) setLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Decode the JSON body
	decoder := json.NewDecoder(r.Body)
	var req LogLevelRequest
	err := decoder.Decode(&req)
	if err != nil {
		writeErrorResponse(w, r, http.StatusBadRequest, "could not decode message body")
		return
	}
	r.Body.Close()

	// Explicitly validate the log level provided
	switch strings.ToLower(req.Level) {
	case "debug", "trace":
		hc.App.LogLevel.SetLevel(zap.DebugLevel)
	case "info":
		hc.App.LogLevel.SetLevel(zap.InfoLevel)
	case "warning", "warn":
		hc.App.LogLevel.SetLevel(zap.WarnLevel)
	case "error":
		hc.App.LogLevel.SetLevel(zap.ErrorLevel)
	case "fatal":
		hc.App.LogLevel.SetLevel(zap.FatalLevel)
	default:
		writeErrorResponse(w, r, http.StatusNotFound, "unknown log level")
		return
	}

	requestInfo := makeRequestInfo(r)
	writeResponse(w, r, http.StatusOK, HTTPResponseError{
		Error:    false,
		Message:  "set log level",
		Request:  requestInfo,
	})
}
