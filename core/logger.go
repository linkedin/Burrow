/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package core

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/toddpalino/Burrow/core/configuration"
)

func CreatePidFile(filename string) {
	// Create a PID file, making sure it doesn't already exist
	pidfile, err := os.OpenFile(filename, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Cannot write PID file: %v", err)
		os.Exit(1)
	}
	fmt.Fprintf(pidfile, "%v", os.Getpid())
	pidfile.Close()
}

func RemovePidFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		fmt.Printf("Failed to remove PID file: %v\n", err)
	}
}

func OpenOutLog(filename string) *os.File {
	// Move existing out file to a dated file if it exists
	if _, err := os.Stat(filename); err == nil {
		if err = os.Rename(filename, filename+"."+time.Now().Format("2006-01-02_15:04:05")); err != nil {
			fmt.Printf("Cannot move old out file: %v", err)
			os.Exit(1)
		}
	}

	// Redirect stdout and stderr to out file
	logFile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	syscall.Dup2(int(logFile.Fd()), 1)
	syscall.Dup2(int(logFile.Fd()), 2)
	return logFile
}

func ConfigureLogger(configuration *configuration.Configuration) (*zap.Logger, *zap.AtomicLevel) {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer

	// Create an AtomicLevel that we can use elsewhere to dynamically change the logging level
	switch configuration.Logging.Level {
	case "", "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "panic":
		level = zap.NewAtomicLevelAt(zap.PanicLevel)
	case "fatal":
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		fmt.Printf("Invalid log level supplied: %s", configuration.Logging.Level)
	}

	// If a filename has been set, set up a rotating logger. Otherwise, use Stdout
	if configuration.Logging.Filename != "" {
		syncOutput = zapcore.AddSync(&lumberjack.Logger{
			Filename:   configuration.Logging.Filename,
			MaxSize:    configuration.Logging.MaxSize,
			MaxBackups: configuration.Logging.MaxBackups,
			MaxAge:     configuration.Logging.MaxAge,
			LocalTime:  configuration.Logging.UseLocalTime,
			Compress:   configuration.Logging.UseCompression,
		})
	} else {
		syncOutput = zapcore.Lock(os.Stdout)
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		syncOutput,
		level,
	)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)
	return logger, &level
}
