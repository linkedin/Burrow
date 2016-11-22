/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package burrow

import (
	"os"

	log "github.com/cihub/seelog"
)

type BurrowLogger struct {
	logger log.LoggerInterface
}

func NewLogger(cfgfile string) *BurrowLogger {
	logger, err := log.LoggerFromConfigAsFile(cfgfile)
	if err != nil {
		log.Criticalf("Cannot start logger: %v", err)
		os.Exit(1)
	}
	log.ReplaceLogger(logger)
	return &BurrowLogger{logger}
}

// These are needed to complete the KafkaLogger interface
func (l *BurrowLogger) Trace(message string, params ...interface{}) {
	l.logger.Tracef(message, params...)
}
func (l *BurrowLogger) Debug(message string, params ...interface{}) {
	l.logger.Debugf(message, params...)
}
func (l *BurrowLogger) Info(message string, params ...interface{}) {
	l.logger.Infof(message, params...)
}
func (l *BurrowLogger) Warn(message string, params ...interface{}) {
	l.logger.Warnf(message, params...)
}
func (l *BurrowLogger) Error(message string, params ...interface{}) {
	l.logger.Errorf(message, params...)
}
func (l *BurrowLogger) Critical(message string, params ...interface{}) {
	l.logger.Criticalf(message, params...)
}
