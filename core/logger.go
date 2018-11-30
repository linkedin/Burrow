/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
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
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// CheckAndCreatePidFile takes a single argument, which is the path to a PID file (a file that contains a single
// integer, which is the process ID of a running process). If this file exists, and if the PID is that of a running
// process, return false as that indicates another copy of this process is already running. Otherwise, create the
// file and write this process's PID to the file and return true. Any error doing this (such as not having permissions
// to write the file) will return false.
//
// This func should be called when Burrow starts to prevent multiple copies from running.
func CheckAndCreatePidFile(filename string) bool {
	// Check if the PID file exists
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		// The file exists, so read it and check if the PID specified is running
		pidString, err := ioutil.ReadFile(filename)
		if err != nil {
			fmt.Printf("Cannot read PID file: %v", err)
			return false
		}
		pid, err := strconv.Atoi(string(pidString))
		if err != nil {
			fmt.Printf("Cannot interpret contents of PID file: %v", err)
			return false
		}

		if pid == os.Getpid() {
			// This could happen inside a docker
			// container, e.g. the pid of Burrow could be
			// equal to 1 each time the container is
			// restarted.
			fmt.Println("Found existing pidfile matching current pid")
			return true
		}

		// Try sending a signal to the process to see if it is still running
		process, err := os.FindProcess(int(pid))
		if err == nil {
			err = process.Signal(syscall.Signal(0))
			if (err == nil) || (err == syscall.EPERM) {
				// The process exists, so we're going to assume it's an old Burrow and we shouldn't start
				fmt.Printf("Existing process running on PID %d. Exiting (my pid = %d)", pid, os.Getpid())
				return false
			}
		}

	}

	// Create a PID file, replacing any existing one (as we already checked it)
	pidfile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Cannot write PID file: %v", err)
		return false
	}
	fmt.Fprintf(pidfile, "%v", os.Getpid())
	pidfile.Close()
	return true
}

// RemovePidFile takes a single argument, which is the path to a PID file. That file is deleted. This func should be
// called when Burrow exits.
func RemovePidFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		fmt.Printf("Failed to remove PID file: %v\n", err)
	}
}

// ConfigureLogger returns a configured zap.Logger which can be used by Burrow for all logging. It also returns a
// zap.AtomicLevel, which can be used to dynamically adjust the level of the logger. The configuration for the logger
// is read from viper, with the following defaults:
//
// logging.level = info
//
// If logging.filename (path to the log file) is provided, a rolling log file is set up using lumberjack. The
// configuration for that log file is read from viper, with the following defaults:
//
// logging.maxsize = 100
// logging.maxbackups = 10
// logging.maxage = 30
// logging.use-localtime = false
// logging.use-compression = false
func ConfigureLogger() (*zap.Logger, *zap.AtomicLevel) {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer

	// Set config defaults for logging
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.maxsize", 100)
	viper.SetDefault("logging.maxbackups", 10)
	viper.SetDefault("logging.maxage", 30)

	// Create an AtomicLevel that we can use elsewhere to dynamically change the logging level
	logLevel := viper.GetString("logging.level")
	switch strings.ToLower(logLevel) {
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
		fmt.Printf("Invalid log level supplied. Defaulting to info: %s", logLevel)
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	// If a filename has been set, set up a rotating logger. Otherwise, use Stdout
	logFilename := viper.GetString("logging.filename")
	if logFilename != "" {
		syncOutput = zapcore.AddSync(&lumberjack.Logger{
			Filename:   logFilename,
			MaxSize:    viper.GetInt("logging.maxsize"),
			MaxBackups: viper.GetInt("logging.maxbackups"),
			MaxAge:     viper.GetInt("logging.maxage"),
			LocalTime:  viper.GetBool("logging.use-localtime"),
			Compress:   viper.GetBool("logging.use-compression"),
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

// OpenOutLog takes a single argument, which is the path to a log file. This process's stdout and stderr are redirected
// to this log file. The os.File object is returned so that it can be managed.
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
	internalDup2(logFile.Fd(), 1)
	internalDup2(logFile.Fd(), 2)
	return logFile
}
