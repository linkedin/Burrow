package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	client "github.com/influxdata/influxdb/client/v2"
)

type Emiter struct {
	context        *ApplicationContext
	clusterMetrics *ClusterMetric
	influxdb       client.Client

	lock    sync.Mutex
	stop    chan struct{}
	stopped chan struct{}
}

type ClusterMetric struct {
	metrics  map[string]*BaseMetric
	clusters []string
}

type BaseMetric struct {
	// Consumers is the consumer group list of kafka
	Consumers []string
	Metrics   map[string]*ConsumerMetric
}

type ConsumerMetric struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Status  struct {
		Cluster    string `json:"cluster"`
		Group      string `json:"group"`
		Status     string `json:"status"`
		Complete   bool   `json:"complete"`
		Partitions []struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Status    string `json:"status"`
			Start     struct {
				Offset    int   `json:"offset"`
				Timestamp int64 `json:"timestamp"`
				Lag       int   `json:"lag"`
				MaxOffset int   `json:"max_offset"`
			} `json:"start"`
			End struct {
				Offset    int   `json:"offset"`
				Timestamp int64 `json:"timestamp"`
				Lag       int   `json:"lag"`
				MaxOffset int   `json:"max_offset"`
			} `json:"end"`
		} `json:"partitions"`
		PartitionCount int         `json:"partition_count"`
		Maxlag         interface{} `json:"maxlag"`
		Totallag       int         `json:"totallag"`
	} `json:"status"`
}

type ClusterResp struct {
	Clusters []string
}

type ConsumerResp struct {
	Consumers []string
}

// NewEmiter from ApplicationContext
func NewEmiter(context *ApplicationContext) *Emiter {
	emiter := &Emiter{
		context:        context,
		clusterMetrics: &ClusterMetric{},
		stop:           make(chan struct{}),
		stopped:        make(chan struct{}),
	}
	if !strings.Contains(context.Config.Influxdb.Addr, "http") {
		context.Config.Influxdb.Addr = "http://" + context.Config.Influxdb.Addr
	}
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     context.Config.Influxdb.Addr,
		Username: context.Config.Influxdb.Username,
		Password: context.Config.Influxdb.Pwd,
	})

	if err != nil {
		panic(err)
	}
	emiter.influxdb = c
	return emiter
}

// Run the emiter,it listens to the signal of stop
func (e *Emiter) Run() {
	var err error
	//check and create db
	_, err = e.runCmd("create database " + e.context.Config.Influxdb.Db)
	if err != nil {
		panic(err)
	}
	ticker := time.NewTicker(time.Second * time.Duration(e.context.Config.Lagcheck.Intervals))
FOR:
	for {
		select {
		case <-ticker.C:
			err = e.fetch()
			if err != nil {
				log.Debug("error in fetch metrics", err.Error())
			}
			e.emit()
		case <-e.stop:
			break FOR
		}
	}
	e.stopped <- struct{}{}
}

// Stop the emiter and wait for the exit signal
func (e *Emiter) Stop() {
	close(e.stop)
	<-e.stopped
}

// runCmd method is for influxb querys
func (e *Emiter) runCmd(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: e.context.Config.Influxdb.Db,
	}
	if response, err := e.influxdb.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil

}

// emit the metrics to influxdb
func (e *Emiter) emit() {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  e.context.Config.Influxdb.Db,
		Precision: "s",
	})

	if err != nil {
		log.Debug("error in new batch", err.Error())
		return
	}
	for c, cs := range e.clusterMetrics.metrics {
		for consumer, metric := range cs.Metrics {

			tagMap := map[string]map[string]string{}
			mp := map[string]map[string]interface{}{}
			for _, stats := range metric.Status.Partitions {
				if m, ok := mp[stats.Topic]; ok {
					m["logsize"] = m["logsize"].(int) + stats.End.Offset
					m["lag"] = m["lag"].(int) + stats.End.Offset
				} else {
					mp[stats.Topic] = map[string]interface{}{
						"logsize": stats.End.Offset,
						"lag":     stats.End.Lag,
					}
					tagMap[stats.Topic] = map[string]string{
						"consumer": consumer,
						"topic":    stats.Topic,
					}
				}
			}

			for topic, v := range tagMap {
				pt, err := client.NewPoint(c+"_"+consumer, v, mp[topic], time.Now())
				if err != nil {
					log.Debug("error in new point", err.Error())
				}
				bp.AddPoint(pt)
				log.Debugf("adding tags and fileds %v, %v\n", v, mp[topic])
			}
		}
	}
	// Write the batch
	if len(bp.Points()) == 0 {
		log.Info("empty metrics fetched,skip emit to db")
		return
	}

	if err := e.influxdb.Write(bp); err != nil {
		log.Debug("error in write to influxdb", err.Error())
	}
}

// fetch consumers from burrow api
func (e *Emiter) fetch() error {
	cs := &ClusterMetric{
		metrics: make(map[string]*BaseMetric),
	}
	var apiUrl = fmt.Sprintf("http://127.0.0.1:%d", e.context.Config.Httpserver.Port)

	var resp = &ClusterResp{}
	err := httpGetAndResp(apiUrl+"/v2/kafka", resp)
	if err != nil {
		return err
	}
	cs.clusters = resp.Clusters
	for _, cluster := range cs.clusters {
		//get all consumers
		log.Debug("fetching cluster =>", cluster)
		consumerResp := &ConsumerResp{}
		err = httpGetAndResp(apiUrl+"/v2/kafka/"+cluster+"/consumer", consumerResp)
		if err != nil {
			log.Debug("error in fetch consumers", err.Error())
			continue
		}

		ccs := &BaseMetric{
			Consumers: consumerResp.Consumers,
			Metrics:   make(map[string]*ConsumerMetric),
		}
		//fetch offset and lags for one consumer
		for _, consumer := range consumerResp.Consumers {
			metric := &ConsumerMetric{}
			err = httpGetAndResp(apiUrl+"/v2/kafka/"+cluster+"/consumer/"+consumer+"/lag", metric)
			if err != nil {
				log.Debug("error in fetch consumer lags", err.Error())
				continue
			}
			ccs.Metrics[consumer] = metric
		}
		cs.metrics[cluster] = ccs
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	e.clusterMetrics = cs
	return nil
}

func httpGetAndResp(api string, entity interface{}) error {
	resp, err := http.Get(api)
	if err != nil {
		return err
	}
	bs, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	return json.Unmarshal(bs, entity)
}
