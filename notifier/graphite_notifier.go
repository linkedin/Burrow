/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package notifier

import (
	"net"
	"time"

	log "github.com/cihub/seelog"

	"bufio"
	"fmt"
	"strings"
	"sync"
)

// GraphiteNotifier sends lags to graphite server
type GraphiteNotifier struct {
	threshold     int
	metrics       map[string]int64
	metricsLocker sync.Mutex
}

// NewGraphiteNotifier initializes sending lag metrics to graphite server
func NewGraphiteNotifier(addr string, prefix string, interval int64, threshold int) (*GraphiteNotifier, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Error("Error while resolving graphite address.")
		return nil, err
	}

	notifier := &GraphiteNotifier{threshold, make(map[string]int64), sync.Mutex{}}

	go func() {
		for range time.Tick(time.Duration(interval) * time.Second) {
			notifier.sendToGraphite(tcpAddr, prefix)
		}
	}()

	return notifier, nil
}

// NotifierName returns graphite name
func (g *GraphiteNotifier) NotifierName() string {
	return "graphite-notify"
}

// Ignore verifies if message should be ignored
func (g *GraphiteNotifier) Ignore(msg Message) bool {
	return int(msg.Status) < g.threshold
}

// Notify updates lag metrics
func (g *GraphiteNotifier) Notify(msg Message) error {
	for _, p := range msg.Partitions {
		metricName := partitionMetricName(msg.Cluster, p.Topic, msg.Group, p.Partition)

		g.metricsLocker.Lock()
		g.metrics[metricName+".lag"] = p.End.Lag
		g.metrics[metricName+".offset"] = p.End.Offset
		g.metricsLocker.Unlock()
	}
	return nil
}

func (g *GraphiteNotifier) sendToGraphite(tcpAddr *net.TCPAddr, prefix string) {
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Error("Error while creating connection to graphite.", err)
		return
	}
	defer conn.Close()
	writer := bufio.NewWriter(conn)

	now := time.Now().Unix()
	g.metricsLocker.Lock()
	for name, value := range g.metrics {
		fmt.Fprintf(writer, "%s.%s %d %d\n", prefix, name, value, now)
	}
	g.metricsLocker.Unlock()
	writer.Flush()
}

func partitionMetricName(cluster string, topic string, group string, partition int32) string {
	return fmt.Sprintf("%s.%s.%s.%d", escapeDots(cluster), escapeDots(topic), escapeDots(group), partition)
}

func escapeDots(toEscape string) string {
	return strings.Replace(toEscape, ".", "_", -1)
}
