/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package helpers

import (
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

func ValidateIP(ipaddr string) bool {
	addr := net.ParseIP(ipaddr)
	return addr != nil
}

func ValidateHostname(hostname string) bool {
	matches, _ := regexp.MatchString(`^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*$`, hostname)
	if !matches {
		// Try as an IP address
		return ValidateIP(hostname)
	}
	return matches
}

func ValidateZookeeperPath(path string) bool {
	parts := strings.Split(path, "/")
	if (len(parts) < 2) || (parts[0] != "") {
		return false
	}
	if (len(parts) == 2) && (parts[1] == "") {
		// Root node is OK
		return true
	}
	for i, node := range parts {
		if i == 0 {
			continue
		}
		matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\-][a-zA-Z0-9_\-.]*$`, node)
		if !matches {
			return false
		}
	}
	return true
}

func ValidateTopic(topic string) bool {
	matches, _ := regexp.MatchString(`^[a-zA-Z0-9_.-]+$`, topic)
	return matches
}

// We don't match valid filenames, we match decent filenames (same as topics)
func ValidateFilename(filename string) bool {
	return ValidateTopic(filename)
}

// Very simplistic email address validator - just looks for an @ and a .
func ValidateEmail(email string) bool {
	matches, _ := regexp.MatchString(`^.+@.+\..+$`, email)
	return matches
}

// Just use the golang Url library for this
func ValidateUrl(rawUrl string) bool {
	_, err := url.Parse(rawUrl)
	return err == nil
}

// Validate a list of ZK or Kafka hosts of the form hostname:port
func ValidateHostList(hosts []string) bool {
	for _, host := range hosts {
		if !ValidateHostPort(host, false) {
			return false
		}
	}

	return true
}
func ValidateHostPort(host string, allowBlankHost bool) bool {
	// Must be hostname:port, ipv4:port, or [ipv6]:port. Optionally allow blank hostname
	hostname, portString, err := net.SplitHostPort(host)
	if err != nil {
		return false
	}

	// Validate the port is a numeric (yeah, strings are valid in some places, but we don't support it)
	_, err = strconv.Atoi(portString)
	if err != nil {
		return false
	}

	// Listeners can have blank hostnames, so we'll skip validation if that's what we're looking for
	if allowBlankHost && hostname == "" {
		return true
	}

	// Only IPv6 can contain :
	if strings.Contains(hostname, ":") && (!ValidateIP(hostname)) {
		return false
	}

	// If all the parts of the hostname are numbers, validate as IP. Otherwise, it's a hostname
	hostnameParts := strings.Split(hostname, ".")
	isIP4 := true
	for _, section := range hostnameParts {
		_, err := strconv.Atoi(section)
		if err != nil {
			isIP4 = false
			break
		}
	}
	if isIP4 {
		return ValidateIP(hostname)
	} else {
		return ValidateHostname(hostname)
	}
}
