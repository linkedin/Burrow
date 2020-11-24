## 1.3.4

- #636 - @bai - Update sarama to the latest bugfix release, Go 1.14.3
- #632 - @bai - Update sarama with a fix for Shopify/sarama#1692
- #628 - @mwain - Add Prometheus Metrics Exporter
- #598 - @rjh-yext - Add SASL-SCRAM ability to Kafka connection
- #627 - @klDen - Fixes Travis CI build of PR 598
- #631 - @bai - Add support for Kafka 2.5.0 and Go 1.14

## 1.3.3

- #617 - @timbertson-zd - add and expose observedAt time for lag entries
- #616 - @timbertson - Fix self-reporting of burrow's own progress
- #615 - @bai - Run github actions when PR is created
- #545 - @Lukkie - Allow underscore in hostname

## 1.3.2

- #608 - @bai - Disable travis ci integration
- #607 - @bai - Build docker image with Go 1.13.7
- #605 - @alvarolmedo - Improve zk lock method to avoid trying lock errors
- #541 - @danudey - Use system SSL store for notifiers by default
- #606 - @bai - Update sarama with fixes for zstd and deadlock
- #548 - @hoesler - feature: configure sarama logger
- #601 - @bai - Add more linters and address select issues
- #597 - @bai - Use golangci-lint instead of assorted linters, build with github actions

## 1.3.1

- #596 - @bai - Fix deprecated goreleaser config opts

## 1.3.0

- #595 - @bai - Update sarama to add support for kafka 2.4.0
- #592 - @jantebeest - Feature/zookeeper tls auth
- #585 - @chadjefferies - Make DisableKeepAlives configurable
- #590 - @thenom - Added 2.2.1 supporting version
- #574 - @mweigel - Set Order field on StorageRequests produced by KafkaZkClient
- #571 - @bai - Add support for Go 1.13
- #567 - @dellalu - Replace '-' with '_' for Env
- #563 - @zerowidth - Update the sarama TLS config to allow anonymous SSL connections
- #562 - @chrnola - Add support for group member metadata v3
- #557 - @khorshuheng - Remove reference to dep in Readme
- #556 - @khorshuheng - Migrate from dep to go module
- #399 - @MadDataScience - allow space in consumer name
- #555 - @khorshuheng - Upgrade sarama to support Kafka 2.3.0
- #488 - @timbertson - start-latest + backfill-earliest mode
- #390 - @vixns - parse http notifier open-url and close-url as templates.
- #528 - @timbertson - Report burrow's own progress as a fake consumer
- #524 - @bai - Run CI and build on Golang 1.12
- #523 - @bai - Update sarama to 1.22.1
- #521 - @asaf400 - Upgrade sarama to support Kafka 2.2.0
- #512 - @vvuibert - sarama 1.21.0
- #506 - @jbvmio - Groupmeta v2
- #477 - @mlongob - Only process metadata for groups with ProtocolType = consumer
- #415 - @lins05 - Fixed pid check in docker containers
- #443 - @Lavaerius - Enabling HTTPS endpoints by using ServeTLS
- #413 - @jorgelbg - Retrieve partition owners on the kafka zookeeper client
- #420 - @maxtuzz - Notifier TLS options
- #439 - @BewareMyPower - Replace topicMap with topicPartitions in cluster module
- #493 - @timbertson - travis: test output of gofmt as a single argument
- #502 - @him2994 - Fixed: *storage.brokerOffset has value nil when no leader election for partition

## 1.2.2 (2019-02-28)

**Release Highlights**

* More fixes to binary release process.

## 1.2.1 (2019-02-21)

**Release Highlights**

* Fix binary release process.
* Report `ClientID` for consumers.
* Fix division by zero error in evaluator.

## 1.2.0 (2019-01-18)

**Release Highlights**

* Add support for Kafka up to version 2.1.0.
* Update sarama to version 1.20.1 with support for zstd compression.
* Support linux/arm64.
* Add blacklist for memory store.

**Changes**

* [[`d244fce922`](https://github.com/nodejs/node/commit/d244fce922)] - Bump sarama to 1.20.1 (Vlad Gorodetsky)
* [[`793430d249`](https://github.com/nodejs/node/commit/793430d249)] - Golang 1.9.x is no longer supported (Vlad Gorodetsky)
* [[`735fcb7c82`](https://github.com/nodejs/node/commit/735fcb7c82)] - Replace deprecated megacheck with staticcheck (Vlad Gorodetsky)
* [[`3d49b2588b`](https://github.com/nodejs/node/commit/3d49b2588b)] - Link the README to the Compose file in the project (Jordan Moore)
* [[`3a59b36d94`](https://github.com/nodejs/node/commit/3a59b36d94)] - Tests fixed (Mikhail Chugunkov)
* [[`6684c5e4db`](https://github.com/nodejs/node/commit/6684c5e4db)] - Added unit test for v3 value decoding (Mikhail Chugunkov)
* [[`10d4dc39eb`](https://github.com/nodejs/node/commit/10d4dc39eb)] - Added v3 messages protocol support (Mikhail Chugunkov)
* [[`d6b075b781`](https://github.com/nodejs/node/commit/d6b075b781)] - Replace deprecated MAINTAINER directive with a label (Vlad Gorodetsky)
* [[`52606499a6`](https://github.com/nodejs/node/commit/52606499a6)] - Refactor parseKafkaVersion to reduce method complexity (gocyclo) (Vlad Gorodetsky)
* [[`b0440f9dea`](https://github.com/nodejs/node/commit/b0440f9dea)] - Add gcc to build zstd (Vlad Gorodetsky)
* [[`6898a8de26`](https://github.com/nodejs/node/commit/6898a8de26)] - Add libc-dev to build zstd (Vlad Gorodetsky)
* [[`b81089aada`](https://github.com/nodejs/node/commit/b81089aada)] - Add support for Kafka 2.1.0 (Vlad Gorodetsky)
* [[`cb004f9405`](https://github.com/nodejs/node/commit/cb004f9405)] - Build with Go 1.11 (Vlad Gorodetsky)
* [[`679a95fb38`](https://github.com/nodejs/node/commit/679a95fb38)] - Fix golint import path (golint fixer)
* [[`f88bb7d3a8`](https://github.com/nodejs/node/commit/f88bb7d3a8)] - Update docker-compose Readme section with working url. (Daniel Wojda)
* [[`3f888cdb2d`](https://github.com/nodejs/node/commit/3f888cdb2d)] - Upgrade sarama to support Kafka 2.0.0 (#440) (daniel)
* [[`1150f6fef9`](https://github.com/nodejs/node/commit/1150f6fef9)] - Support linux/arm64 using Dup3() instead of Dup2() (Mpampis Kostas)
* [[`1b65b4b2f2`](https://github.com/nodejs/node/commit/1b65b4b2f2)] - Add support for Kafka 1.1.0 (#403) (Vlad Gorodetsky)
* [[`74b309fc8d`](https://github.com/nodejs/node/commit/74b309fc8d)] - code coverage for newly added lines (Clemens Valiente)
* [[`279c75375c`](https://github.com/nodejs/node/commit/279c75375c)] - accidentally reverted this (Clemens Valiente)
* [[`192878c69c`](https://github.com/nodejs/node/commit/192878c69c)] - gofmt (Clemens Valiente)
* [[`33bc8defcd`](https://github.com/nodejs/node/commit/33bc8defcd)] - make first regex test case a proper match everything (Clemens Valiente)
* [[`279b256b27`](https://github.com/nodejs/node/commit/279b256b27)] - only set whitelist / blacklist if it's not empty string (Clemens Valiente)
* [[`b48d30d18c`](https://github.com/nodejs/node/commit/b48d30d18c)] - naming (Clemens Valiente)
* [[`7d6c6ccb03`](https://github.com/nodejs/node/commit/7d6c6ccb03)] - variable naming (Clemens Valiente)
* [[`4e051e973f`](https://github.com/nodejs/node/commit/4e051e973f)] - add tests (Clemens Valiente)
* [[`545bec66d0`](https://github.com/nodejs/node/commit/545bec66d0)] - add blacklist for memory store (Clemens Valiente)
* [[`07af26d2f1`](https://github.com/nodejs/node/commit/07af26d2f1)] - Updated burrow endpoint in README : #401 (Ratish Ravindran)
* [[`fecab1ea88`](https://github.com/nodejs/node/commit/fecab1ea88)] - pass custom headers to http notifications. (#357) (vixns)

## 1.0.0 (TBD)

Features:
  - Code overhaul - more modular and now with tests
  - Actual documentation (godoc)
  - Support for topic deletion in Kafka clusters
  - Removed Slack notifier in favor of just using the HTTP notifier

Bugfixes:
  - Too many to count

## 0.1.1 (2016-05-01)

Features:
  - ZK Offset checking
  - Storm offset checking

Bugfixes:
  - Fixed an issue with not closing HTTP requests and responses properly
  - Change internal hostname structures for properly support IPv6

## 0.1.0 (2015-10-07)

Initial version release
