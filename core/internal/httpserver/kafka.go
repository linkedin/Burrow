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

	"github.com/linkedin/Burrow/core/protocol"
)

func (hc *Coordinator) handleClusterList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Fetch cluster list from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <-request.Reply

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseClusterList{
		Error:    false,
		Message:  "cluster list returned",
		Clusters: response.([]string),
		Request:  requestInfo,
	})
}

func getTLSProfile(name string) *httpResponseTLSProfile {
	configRoot := "tls." + name
	if !viper.IsSet(configRoot) {
		return nil
	}

	return &httpResponseTLSProfile{
		Name:     name,
		CertFile: viper.GetString(configRoot + ".certfile"),
		KeyFile:  viper.GetString(configRoot + ".keyfile"),
		CAFile:   viper.GetString(configRoot + ".cafile"),
		NoVerify: viper.GetBool(configRoot + ".noverify"),
	}
}

func getSASLProfile(name string) *httpResponseSASLProfile {
	configRoot := "sasl." + name
	if !viper.IsSet(configRoot) {
		return nil
	}

	return &httpResponseSASLProfile{
		Name:           name,
		HandshakeFirst: viper.GetBool(configRoot + ".handshake-first"),
		Username:       viper.GetString(configRoot + ".username"),
	}
}

func getClientProfile(name string) httpResponseClientProfile {
	configRoot := "client-profile." + name
	return httpResponseClientProfile{
		Name:         name,
		ClientID:     viper.GetString(configRoot + ".client-id"),
		KafkaVersion: viper.GetString(configRoot + ".kafka-version"),
		TLS:          getTLSProfile(viper.GetString(configRoot + ".tls")),
		SASL:         getSASLProfile(viper.GetString(configRoot + ".sasl")),
	}
}

func (hc *Coordinator) handleClusterDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Get cluster config
	configRoot := "cluster." + params.ByName("cluster")
	if !viper.IsSet(configRoot) {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "cluster module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseConfigModuleDetail{
			Error:   false,
			Message: "cluster module detail returned",
			Module: httpResponseConfigModuleCluster{
				ClassName:     viper.GetString(configRoot + ".class-name"),
				Servers:       viper.GetStringSlice(configRoot + ".servers"),
				TopicRefresh:  viper.GetInt64(configRoot + ".topic-refresh"),
				OffsetRefresh: viper.GetInt64(configRoot + ".offset-refresh"),
				ClientProfile: getClientProfile(viper.GetString(configRoot + ".client-profile")),
			},
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) handleTopicList(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch topic list from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopics,
		Cluster:     params.ByName("cluster"),
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <-request.Reply

	if response == nil {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "cluster not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseTopicList{
			Error:   false,
			Message: "topic list returned",
			Topics:  response.([]string),
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) handleTopicDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch topic offsets from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopic,
		Cluster:     params.ByName("cluster"),
		Topic:       params.ByName("topic"),
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <-request.Reply

	if response == nil {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "cluster or topic not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseTopicDetail{
			Error:   false,
			Message: "topic offsets returned",
			Offsets: response.([]int64),
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) handleTopicConsumerList(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch topic offsets from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumersForTopic,
		Cluster:     params.ByName("cluster"),
		Topic:       params.ByName("topic"),
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <-request.Reply

	if response == nil {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "cluster not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseTopicConsumerDetail{
			Error:     false,
			Message:   "consumers of topic returned",
			Consumers: response.([]string),
			Request:   requestInfo,
		})
	}
}

func (hc *Coordinator) handleConsumerList(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch consumer list from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumers,
		Cluster:     params.ByName("cluster"),
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <-request.Reply

	if response == nil {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "cluster not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseConsumerList{
			Error:     false,
			Message:   "consumer list returned",
			Consumers: response.([]string),
			Request:   requestInfo,
		})
	}
}

func (hc *Coordinator) handleConsumerDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch consumer data from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumer,
		Cluster:     params.ByName("cluster"),
		Group:       params.ByName("consumer"),
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <-request.Reply

	if response == nil {
		hc.writeErrorResponse(w, r, http.StatusNotFound, "cluster or consumer not found")
	} else {
		requestInfo := makeRequestInfo(r)
		hc.writeResponse(w, r, http.StatusOK, httpResponseConsumerDetail{
			Error:   false,
			Message: "consumer detail returned",
			Topics:  response.(protocol.ConsumerTopics),
			Request: requestInfo,
		})
	}
}

func (hc *Coordinator) handleConsumerStatus(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch consumer data from the storage module
	request := &protocol.EvaluatorRequest{
		Cluster: params.ByName("cluster"),
		Group:   params.ByName("consumer"),
		ShowAll: false,
		Reply:   make(chan *protocol.ConsumerGroupStatus),
	}
	hc.App.EvaluatorChannel <- request
	response := <-request.Reply

	responseCode := http.StatusOK
	if response.Status == protocol.StatusNotFound {
		responseCode = http.StatusNotFound
	}

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, responseCode, httpResponseConsumerStatus{
		Error:   false,
		Message: "consumer status returned",
		Status:  *response,
		Request: requestInfo,
	})
}

func (hc *Coordinator) handleConsumerStatusComplete(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Fetch consumer data from the storage module
	request := &protocol.EvaluatorRequest{
		Cluster: params.ByName("cluster"),
		Group:   params.ByName("consumer"),
		ShowAll: true,
		Reply:   make(chan *protocol.ConsumerGroupStatus),
	}
	hc.App.EvaluatorChannel <- request
	response := <-request.Reply

	responseCode := http.StatusOK
	if response.Status == protocol.StatusNotFound {
		responseCode = http.StatusNotFound
	}

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, responseCode, httpResponseConsumerStatus{
		Error:   false,
		Message: "consumer status returned",
		Status:  *response,
		Request: requestInfo,
	})
}

func (hc *Coordinator) handleConsumerDelete(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Delete consumer from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageSetDeleteGroup,
		Cluster:     params.ByName("cluster"),
		Group:       params.ByName("consumer"),
	}
	hc.App.StorageChannel <- request

	requestInfo := makeRequestInfo(r)
	hc.writeResponse(w, r, http.StatusOK, httpResponseError{
		Error:   false,
		Message: "consumer group removed",
		Request: requestInfo,
	})
}
