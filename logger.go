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
	"fmt"
	log "github.com/cihub/seelog"
	"os"
	//"syscall"
	"time"
)

type BurrowLogger struct {
	logger log.LoggerInterface
}

func createPidFile(filename string) {
	// Create a PID file, making sure it doesn't already exist
	pidfile, err := os.OpenFile(filename, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Criticalf("Cannot write PID file: %v", err)
		os.Exit(1)
	}
	fmt.Fprintf(pidfile, "%v", os.Getpid())
	pidfile.Close()
}

func removePidFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		fmt.Printf("Failed to remove PID file: %v\n", err)
	}
}

func openOutLog(filename string) *os.File {
	// Move existing out file to a dated file if it exists
	if _, err := os.Stat(filename); err == nil {
		if err = os.Rename(filename, filename+"."+time.Now().Format("2006-01-02_15:04:05")); err != nil {
			log.Criticalf("Cannot move old out file: %v", err)
			os.Exit(1)
		}
	}

	// Redirect stdout and stderr to out file
	logFile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	//syscall.Dup2(int(logFile.Fd()), 1)
	//syscall.Dup2(int(logFile.Fd()), 2)
	return logFile
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
