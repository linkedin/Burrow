# Burrow - Kafka Consumer Lag Checking

Burrow is a monitoring companion for [Apache Kafka](http://kafka.apache.org) that provides consumer lag checking as a service without the need for specifying thresholds. It monitors committed offsets for all consumers and calculates the status of those consumers on demand. An HTTP endpoint is provided to request status on demand, as well as provide other Kafka cluster information. There are also configurable notifiers that can send status out via email or HTTP calls to another service.

## Features
* NO THRESHOLDS! Groups are evaluated over a sliding window.
* Multiple Kafka Cluster support
* Automatically monitors all consumers using Kafka-committed offsets stored in either internal offsets topic or external Zookeeper
* HTTP endpoint for consumer group status, as well as broker and consumer information
* Configurable emailer for sending alerts for specific groups
* Configurable HTTP client for sending alerts to another system for all groups
* Supports offsets managed by Storm Kafka Spout

## Getting Started
### Prerequisites
Burrow is written in Go, so before you get started, you should [install and set up Go](https://golang.org/doc/install).

If you have not yet installed the [Go Package Manager](https://github.com/pote/gpm), please go over there and follow their short installation instructions. GPM is used to automatically pull in the dependencies for Burrow so you don't have to chase them all down.

### Build and Install
```
$ go get github.com/linkedin/burrow
$ cd $GOPATH/src/github.com/linkedin/burrow
$ gpm install
$ go install
```

### Running Burrow
```
$ $GOPATH/bin/burrow --config path/to/burrow.cfg
```

### Configuration
For information on how to write your configuration file, check out the [detailed wiki](https://github.com/linkedin/Burrow/wiki)

## License
Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied.

