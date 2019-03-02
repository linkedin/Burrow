/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

// Package httpserver - HTTP API endpoint
// The httpserver subsystem provides an HTTP interface to Burrow that can be used to fetch information about the
// clusters and consumers it is monitoring. More documentation on the requests and responses is provided at
// https://github.com/linkedin/Burrow/wiki/HTTP-Endpoint.
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

	"github.com/julienschmidt/httprouter"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

// Coordinator runs the HTTP interface for Burrow, managing all configured listeners.
type Coordinator struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *protocol.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	router  *httprouter.Router
	servers map[string]*http.Server
	theCert map[string]string
	theKey  map[string]string
}

// Configure is called to configure the HTTP server. This includes validating all configurations for each configured
// listener (which are not treated as separate modules, as opposed to other coordinators), as well as setting up the
// request router. Any configuration failure will cause the func to panic with an appropriate error message.
//
// If no listener has been configured, the coordinator will set up a default listener on a random port greater than
// 1024, as selected by the net.Listener call. This listener will be logged so that the port chosen will be known.
func (hc *Coordinator) Configure() {
	hc.Log.Info("configuring")
	hc.router = httprouter.New()

	// If no HTTP server configured, add a default HTTP server that listens on a random port
	servers := viper.GetStringMap("httpserver")
	if len(servers) == 0 {
		viper.Set("httpserver.default.address", ":0")
		servers = viper.GetStringMap("httpserver")
	}

	// Validate provided HTTP server configs
	hc.servers = make(map[string]*http.Server)
	hc.theCert = make(map[string]string)
	hc.theKey = make(map[string]string)
	for name := range servers {
		configRoot := "httpserver." + name
		server := &http.Server{
			Handler: hc.router,
		}

		server.Addr = viper.GetString(configRoot + ".address")
		if !helpers.ValidateHostPort(server.Addr, true) {
			panic("invalid HTTP server listener address")
		}

		viper.SetDefault(configRoot+".timeout", 300)
		timeout := viper.GetInt(configRoot + ".timeout")
		server.ReadTimeout = time.Duration(timeout) * time.Second
		server.ReadHeaderTimeout = time.Duration(timeout) * time.Second
		server.WriteTimeout = time.Duration(timeout) * time.Second
		server.IdleTimeout = time.Duration(timeout) * time.Second
		keyFile := ""
		certFile := ""
		if viper.IsSet(configRoot + ".tls") {
			tlsName := viper.GetString(configRoot + ".tls")
			certFile = viper.GetString("tls." + tlsName + ".certfile")
			keyFile = viper.GetString("tls." + tlsName + ".keyfile")
			caFile := viper.GetString("tls." + tlsName + ".cafile")

			server.TLSConfig = &tls.Config{}

			if caFile != "" {
				caCert, err := ioutil.ReadFile(caFile)
				if err != nil {
					panic("cannot read TLS CA file: " + err.Error())
				}
				server.TLSConfig.RootCAs = x509.NewCertPool()
				server.TLSConfig.RootCAs.AppendCertsFromPEM(caCert)
			}

			if certFile == "" || keyFile == "" {
				panic("TLS HTTP server specified with missing certificate or key")
			}
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				panic("cannot read TLS certificate or key file: " + err.Error())
			}
			server.TLSConfig.Certificates = []tls.Certificate{cert}
			server.TLSConfig.BuildNameToCertificate()
		}
		hc.servers[name] = server
		hc.theCert[name] = certFile
		hc.theKey[name] = keyFile
	}

	// Configure URL routes here

	// This is a catchall for undefined URLs
	hc.router.NotFound = &defaultHandler{}

	// This is a healthcheck URL. Please don't change it
	hc.router.GET("/burrow/admin", hc.handleAdmin)

	// All valid paths go here
	hc.router.GET("/v3/kafka", hc.handleClusterList)
	hc.router.GET("/v3/kafka/:cluster", hc.handleClusterDetail)
	hc.router.GET("/v3/kafka/:cluster/topic", hc.handleTopicList)
	hc.router.GET("/v3/kafka/:cluster/topic/:topic", hc.handleTopicDetail)
	hc.router.GET("/v3/kafka/:cluster/topic/:topic/consumers", hc.handleTopicConsumerList)
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

// Start is responsible for starting the listener on each configured address. If any listener fails to start, the error
// is logged, and the listeners that have already been started are stopped. The func then returns the error encountered
// to the caller. Once the listeners are all started, the HTTP server itself is started on each listener to respond to
// requests.
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
		hc.Log.Info("started listener", zap.String("listener", ln.Addr().String()))
		listeners[name] = tcpKeepAliveListener{
			Keepalive:   server.IdleTimeout,
			TCPListener: ln.(*net.TCPListener),
		}
	}

	// Start the HTTP server on the listeners
	for name, server := range hc.servers {
		if hc.theCert[name] != "" || hc.theKey[name] != "" {
			go server.ServeTLS(listeners[name], hc.theCert[name], hc.theKey[name])
		} else {
			go server.Serve(listeners[name])
		}

	}
	return nil
}

// Stop calls the Close func for each configured HTTP server listener. This stops the underlying HTTP server without
// waiting for client calls to complete. If there are any errors while shutting down the listeners, this does not stop
// other listeners from being closed. A generic error will be returned to the caller in this case.
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
	}
	return nil
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted connections. It's used by ListenAndServe and
// ListenAndServeTLS so dead TCP connections (e.g. closing laptop mid-download) eventually go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
	Keepalive time.Duration
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

func makeRequestInfo(r *http.Request) httpResponseRequestInfo {
	hostname, _ := os.Hostname()
	return httpResponseRequestInfo{
		URI:  r.URL.Path,
		Host: hostname,
	}
}

func (hc *Coordinator) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, jsonObj interface{}) {
	// Add CORS header, if configured
	corsHeader := viper.GetString("general.access-control-allow-origin")
	if corsHeader != "" {
		w.Header().Set("Access-Control-Allow-Origin", corsHeader)
	}

	w.Header().Set("Content-Type", "application/json")

	if jsonBytes, err := json.Marshal(jsonObj); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"))
	} else {
		w.WriteHeader(statusCode)
		w.Write(jsonBytes)
	}
}

func (hc *Coordinator) writeErrorResponse(w http.ResponseWriter, r *http.Request, errValue int, message string) {
	hc.writeResponse(w, r, errValue, httpResponseError{
		Error:   true,
		Message: message,
		Request: makeRequestInfo(r),
	})
}

// This is a catch-all handler for unknown URLs. It should return a 404
type defaultHandler struct{}

func (handler *defaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "{\"error\":true,\"message\":\"invalid request type\",\"result\":{}}", http.StatusNotFound)
}

func (hc *Coordinator) handleAdmin(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Add CORS header, if configured
	corsHeader := viper.GetString("general.access-control-allow-origin")
	if corsHeader != "" {
		w.Header().Set("Access-Control-Allow-Origin", corsHeader)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("GOOD"))
}

func (hc *Coordinator) getLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseLogLevel{
		Error:   false,
		Message: "log level returned",
		Level:   hc.App.LogLevel.Level().String(),
		Request: requestInfo,
	})
}

func (hc *Coordinator) setLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Decode the JSON body
	decoder := json.NewDecoder(r.Body)
	var req logLevelRequest
	err := decoder.Decode(&req)
	if err != nil {
		hc.writeErrorResponse(w, r, http.StatusBadRequest, "could not decode message body")
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
		hc.writeErrorResponse(w, r, http.StatusNotFound, "unknown log level")
		return
	}

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseError{
		Error:   false,
		Message: "set log level",
		Request: requestInfo,
	})
}
