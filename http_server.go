/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type HttpServer struct {
	app *ApplicationContext
	mux *http.ServeMux
}

type appHandler struct {
	app     *ApplicationContext
	handler func(*ApplicationContext, http.ResponseWriter, *http.Request) (int, string)
}

func NewHttpServer(app *ApplicationContext) (*HttpServer, error) {
	server := &HttpServer{
		app: app,
		mux: http.NewServeMux(),
	}

	// This is a catchall for undefined URLs
	server.mux.HandleFunc("/", handleDefault)

	// This is a healthcheck URL. Please don't change it
	server.mux.HandleFunc("/burrow/admin", handleAdmin)

	// All valid paths go here. Make sure they use the right handler
	server.mux.Handle("/v2/kafka", appHandler{server.app, handleClusterList})
	server.mux.Handle("/v2/kafka/", appHandler{server.app, handleKafka})
	server.mux.Handle("/v2/zookeeper", appHandler{server.app, handleClusterList})
	// server.mux.Handle("/v2/zookeeper/", appHandler{server.app, handleZookeeper})

	go http.ListenAndServe(fmt.Sprintf(":%v", server.app.Config.Httpserver.Port), server.mux)
	return server, nil
}

func (ah appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == "GET":
		if status, err := ah.handler(ah.app, w, r); status != 200 {
			http.Error(w, err, status)
		} else {
			io.WriteString(w, err)
		}
	case r.Method == "DELETE":
		// Later we can add authentication here
		if status, err := ah.handler(ah.app, w, r); status != 200 {
			http.Error(w, err, status)
		} else {
			io.WriteString(w, err)
		}
	default:
		http.Error(w, "{\"error\":true,\"message\":\"request method not supported\",\"result\":{}}", http.StatusMethodNotAllowed)
	}
}

// This is a catch-all handler for unknown URLs. It should return a 404
func handleDefault(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "{\"error\":true,\"message\":\"invalid request type\",\"result\":{}}", http.StatusNotFound)
}

func handleAdmin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "{\"error\":true,\"message\":\"request method not supported\",\"result\":{}}", http.StatusMethodNotAllowed)
	}
	io.WriteString(w, "GOOD")
}

type HTTPResponseClusterList struct {
	Error    bool     `json:"error"`
	Message  string   `json:"message"`
	Clusters []string `json:"clusters"`
}

func handleClusterList(app *ApplicationContext, w http.ResponseWriter, r *http.Request) (int, string) {
	if r.Method != "GET" {
		return http.StatusMethodNotAllowed, "{\"error\":true,\"message\":\"request method not supported\",\"result\":{}}"
	}

	clusterList := make([]string, len(app.Config.Kafka))
	i := 0
	for cluster, _ := range app.Config.Kafka {
		clusterList[i] = cluster
		i++
	}
	jsonStr, err := json.Marshal(HTTPResponseClusterList{
		Error:    false,
		Message:  "cluster list returned",
		Clusters: clusterList,
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}

	w.Write(jsonStr)
	return 200, ""
}

// This is a router for all requests that operate against Kafka clusters (/v2/kafka/...)
func handleKafka(app *ApplicationContext, w http.ResponseWriter, r *http.Request) (int, string) {
	pathParts := strings.Split(r.URL.Path[1:], "/")
	if _, ok := app.Config.Kafka[pathParts[2]]; !ok {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"cluster not found\",\"result\":{}}"
	}
	if pathParts[2] == "" {
		// Allow a trailing / on requests
		return handleClusterList(app, w, r)
	}
	if (len(pathParts) == 3) || (pathParts[3] == "") {
		return handleClusterDetail(app, w, pathParts[2])
	}

	switch pathParts[3] {
	case "consumer":
		switch {
		case r.Method == "DELETE":
			switch {
			case (len(pathParts) == 5) || (pathParts[5] == ""):
				return handleConsumerDrop(app, w, pathParts[2], pathParts[4])
			default:
				return http.StatusMethodNotAllowed, "{\"error\":true,\"message\":\"request method not supported\",\"result\":{}}"
			}
		case r.Method == "GET":
			switch {
			case (len(pathParts) == 4) || (pathParts[4] == ""):
				return handleConsumerList(app, w, pathParts[2])
			case (len(pathParts) == 5) || (pathParts[5] == ""):
				// Consumer detail - list of consumer streams/hosts? Can be config info later
				return http.StatusNotFound, "{\"error\":true,\"message\":\"unknown API call\",\"result\":{}}"
			case pathParts[5] == "topic":
				switch {
				case (len(pathParts) == 6) || (pathParts[6] == ""):
					return handleConsumerTopicList(app, w, pathParts[2], pathParts[4])
				case (len(pathParts) == 7) || (pathParts[7] == ""):
					return handleConsumerTopicDetail(app, w, pathParts[2], pathParts[4], pathParts[6])
				}
			case pathParts[5] == "status":
				return handleConsumerStatus(app, w, pathParts[2], pathParts[4])
			}
		default:
			return http.StatusMethodNotAllowed, "{\"error\":true,\"message\":\"request method not supported\",\"result\":{}}"
		}
	case "topic":
		switch {
		case r.Method != "GET":
			return http.StatusMethodNotAllowed, "{\"error\":true,\"message\":\"request method not supported\",\"result\":{}}"
		case (len(pathParts) == 4) || (pathParts[4] == ""):
			return handleBrokerTopicList(app, w, pathParts[2])
		case (len(pathParts) == 5) || (pathParts[5] == ""):
			return handleBrokerTopicDetail(app, w, pathParts[2], pathParts[4])
		}
	case "offsets":
		// Reserving this endpoint to implement later
		return http.StatusNotFound, "{\"error\":true,\"message\":\"unknown API call\",\"result\":{}}"
	}

	// If we fell through, return a 404
	return http.StatusNotFound, "{\"error\":true,\"message\":\"unknown API call\",\"result\":{}}"
}

type HTTPResponseClusterDetailCluster struct {
	Zookeepers    []string `json:"zookeepers"`
	ZookeeperPort int      `json:"zookeeper_port"`
	ZookeeperPath string   `json:"zookeeper_path"`
	Brokers       []string `json:"brokers"`
	BrokerPort    int      `json:"broker_port"`
	OffsetsTopic  string   `json:"offsets_topic"`
}
type HTTPResponseClusterDetail struct {
	Error   bool                             `json:"error"`
	Message string                           `json:"message"`
	Cluster HTTPResponseClusterDetailCluster `json:"cluster"`
}

func handleClusterDetail(app *ApplicationContext, w http.ResponseWriter, cluster string) (int, string) {
	jsonStr, err := json.Marshal(HTTPResponseClusterDetail{
		Error:   false,
		Message: "cluster detail returned",
		Cluster: HTTPResponseClusterDetailCluster{
			Zookeepers:    app.Config.Kafka[cluster].Zookeepers,
			ZookeeperPort: app.Config.Kafka[cluster].ZookeeperPort,
			ZookeeperPath: app.Config.Kafka[cluster].ZookeeperPath,
			Brokers:       app.Config.Kafka[cluster].Brokers,
			BrokerPort:    app.Config.Kafka[cluster].BrokerPort,
			OffsetsTopic:  app.Config.Kafka[cluster].OffsetsTopic,
		},
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}

	w.Write(jsonStr)
	return 200, ""
}

func handleConsumerList(app *ApplicationContext, w http.ResponseWriter, cluster string) (int, string) {
	storageRequest := &RequestConsumerList{Result: make(chan []string), Cluster: cluster}
	app.Storage.requestChannel <- storageRequest
	jsonStr, err := json.Marshal(struct {
		Error     bool     `json:"error"`
		Message   string   `json:"message"`
		Consumers []string `json:"consumers"`
	}{
		Error:     false,
		Message:   "consumer list returned",
		Consumers: <-storageRequest.Result,
	})

	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}

	w.Write(jsonStr)
	return 200, ""
}

type HTTPResponseTopicList struct {
	Error   bool     `json:"error"`
	Message string   `json:"message"`
	Topics  []string `json:"topics"`
}

func handleConsumerTopicList(app *ApplicationContext, w http.ResponseWriter, cluster string, group string) (int, string) {
	storageRequest := &RequestTopicList{Result: make(chan *ResponseTopicList), Cluster: cluster, Group: group}
	app.Storage.requestChannel <- storageRequest
	result := <-storageRequest.Result
	if result.Error {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"consumer group not found\",\"result\":{}}"
	}
	jsonStr, err := json.Marshal(HTTPResponseTopicList{
		Error:   false,
		Message: "consumer topic list returned",
		Topics:  result.TopicList,
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}
	w.Write(jsonStr)
	return 200, ""
}

type HTTPResponseTopicDetail struct {
	Error   bool    `json:"error"`
	Message string  `json:"message"`
	Offsets []int64 `json:"offsets"`
}

func handleConsumerTopicDetail(app *ApplicationContext, w http.ResponseWriter, cluster string, group string, topic string) (int, string) {
	storageRequest := &RequestOffsets{Result: make(chan *ResponseOffsets), Cluster: cluster, Topic: topic, Group: group}
	app.Storage.requestChannel <- storageRequest
	result := <-storageRequest.Result
	if result.ErrorGroup {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"consumer group not found\",\"result\":{}}"
	}
	if result.ErrorTopic {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"topic not found for consumer group\",\"result\":{}}"
	}
	jsonStr, err := json.Marshal(HTTPResponseTopicDetail{
		Error:   false,
		Message: "consumer group topic offsets returned",
		Offsets: result.OffsetList,
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}

	w.Write(jsonStr)
	return 200, ""
}

type HTTPResponseConsumerStatus struct {
	Error   bool                `json:"error"`
	Message string              `json:"message"`
	Status  ConsumerGroupStatus `json:"status"`
}

func handleConsumerStatus(app *ApplicationContext, w http.ResponseWriter, cluster string, group string) (int, string) {
	storageRequest := &RequestConsumerStatus{Result: make(chan *ConsumerGroupStatus), Cluster: cluster, Group: group}
	app.Storage.requestChannel <- storageRequest
	result := <-storageRequest.Result
	if result.Status == StatusNotFound {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"consumer group not found\",\"result\":{}}"
	}
	jsonStr, err := json.Marshal(HTTPResponseConsumerStatus{
		Error:   false,
		Message: "consumer group status returned",
		Status:  *result,
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}
	w.Write(jsonStr)
	return 200, ""
}

func handleConsumerDrop(app *ApplicationContext, w http.ResponseWriter, cluster string, group string) (int, string) {
	storageRequest := &RequestConsumerDrop{Result: make(chan StatusConstant), Cluster: cluster, Group: group}
	app.Storage.requestChannel <- storageRequest
	result := <-storageRequest.Result
	if result == StatusNotFound {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"consumer group not found\",\"result\":{}}"
	}

	return 200, "{\"error\":false,\"message\":\"consumer group removed\",\"result\":{}}"
}

func handleBrokerTopicList(app *ApplicationContext, w http.ResponseWriter, cluster string) (int, string) {
	storageRequest := &RequestTopicList{Result: make(chan *ResponseTopicList), Cluster: cluster}
	app.Storage.requestChannel <- storageRequest
	result := <-storageRequest.Result
	jsonStr, err := json.Marshal(HTTPResponseTopicList{
		Error:   false,
		Message: "broker topic list returned",
		Topics:  result.TopicList,
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}

	w.Write(jsonStr)
	return 200, ""
}

func handleBrokerTopicDetail(app *ApplicationContext, w http.ResponseWriter, cluster string, topic string) (int, string) {
	storageRequest := &RequestOffsets{Result: make(chan *ResponseOffsets), Cluster: cluster, Topic: topic}
	app.Storage.requestChannel <- storageRequest
	result := <-storageRequest.Result
	if result.ErrorTopic {
		return http.StatusNotFound, "{\"error\":true,\"message\":\"topic not found\",\"result\":{}}"
	}
	jsonStr, err := json.Marshal(HTTPResponseTopicDetail{
		Error:   false,
		Message: "broker topic offsets returned",
		Offsets: result.OffsetList,
	})
	if err != nil {
		return http.StatusInternalServerError, "{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"
	}

	w.Write(jsonStr)
	return 200, ""
}

func (server *HttpServer) Stop() {
	// Nothing to do right now
}
