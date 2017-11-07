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

	"github.com/linkedin/Burrow/core/protocol"
	"github.com/linkedin/Burrow/core/configuration"
)

func (hc *Coordinator) handleClusterList(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Fetch cluster list from the storage module
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	hc.App.StorageChannel <- request
	response := <- request.Reply

	requestInfo := makeRequestInfo(r)
	writeResponse(w, r, http.StatusOK, HTTPResponseClusterList{
		Error:    false,
		Message:  "cluster list returned",
		Clusters: response.([]string),
		Request:  requestInfo,
	})
}

func getClientProfile(name string, profile *configuration.ClientProfile) HTTPResponseClientProfile {
	return HTTPResponseClientProfile{
		Name:            name,
		ClientID:        profile.ClientID,
		KafkaVersion:    profile.KafkaVersion,
		TLS:             profile.TLS,
		TLSNoVerify:     profile.TLSNoVerify,
		TLSCertFilePath: profile.TLSCertFilePath,
		TLSKeyFilePath:  profile.TLSKeyFilePath,
		TLSCAFilePath:   profile.TLSCAFilePath,
		SASL:            profile.SASL,
		HandshakeFirst:  profile.HandshakeFirst,
		Username:        profile.Username,
	}

}

func (hc *Coordinator) handleClusterDetail(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Get cluster config
	if cfg, ok := hc.App.Configuration.Cluster[params.ByName("cluster")]; !ok {
		writeErrorResponse(w, r, http.StatusNotFound, "cluster module not found")
	} else {
		requestInfo := makeRequestInfo(r)
		writeResponse(w, r, http.StatusOK, HTTPResponseConfigModuleDetail{
			Error:   false,
			Message: "cluster module detail returned",
			Module:  HTTPResponseConfigModuleCluster{
				ClassName:       cfg.ClassName,
				Servers:         cfg.Servers,
				ClientProfile:   getClientProfile(cfg.ClientProfile, hc.App.Configuration.ClientProfile[cfg.ClientProfile]),
				TopicRefresh:    cfg.TopicRefresh,
				OffsetRefresh:   cfg.OffsetRefresh,
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
	response := <- request.Reply

	if response == nil {
		writeErrorResponse(w, r, http.StatusNotFound, "cluster not found")
	} else {
		requestInfo := makeRequestInfo(r)
		writeResponse(w, r, http.StatusOK, HTTPResponseTopicList{
			Error:    false,
			Message:  "topic list returned",
			Topics:   response.([]string),
			Request:  requestInfo,
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
	response := <- request.Reply

	if response == nil {
		writeErrorResponse(w, r, http.StatusNotFound, "cluster or topic not found")
	} else {
		requestInfo := makeRequestInfo(r)
		writeResponse(w, r, http.StatusOK, HTTPResponseTopicDetail{
			Error:    false,
			Message:  "topic offsets returned",
			Offsets:  response.([]int64),
			Request:  requestInfo,
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
	response := <- request.Reply

	if response == nil {
		writeErrorResponse(w, r, http.StatusNotFound, "cluster not found")
	} else {
		requestInfo := makeRequestInfo(r)
		writeResponse(w, r, http.StatusOK, HTTPResponseConsumerList{
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
	response := <- request.Reply

	if response == nil {
		writeErrorResponse(w, r, http.StatusNotFound, "cluster or consumer not found")
	} else {
		requestInfo := makeRequestInfo(r)
		writeResponse(w, r, http.StatusOK, HTTPResponseConsumerDetail{
			Error:    false,
			Message:  "consumer list returned",
			Topics:   response.(protocol.ConsumerTopics),
			Request:  requestInfo,
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
	response := <- request.Reply

	responseCode := http.StatusOK
	if response.Status == protocol.StatusNotFound {
		responseCode = http.StatusNotFound
	}

	requestInfo := makeRequestInfo(r)
	writeResponse(w, r, responseCode, HTTPResponseConsumerStatus{
		Error:    false,
		Message:  "consumer status returned",
		Status:   *response,
		Request:  requestInfo,
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
	response := <- request.Reply

	responseCode := http.StatusOK
	if response.Status == protocol.StatusNotFound {
		responseCode = http.StatusNotFound
	}

	requestInfo := makeRequestInfo(r)
	writeResponse(w, r, responseCode, HTTPResponseConsumerStatus{
		Error:    false,
		Message:  "consumer status returned",
		Status:   *response,
		Request:  requestInfo,
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
	writeResponse(w, r, http.StatusOK, HTTPResponseError{
		Error:    false,
		Message:  "consumer group removed",
		Request:  requestInfo,
	})
}
