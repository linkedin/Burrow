/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package configuration

import (
	"fmt"
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
		matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\-][a-zA-Z0-9_\-\.]*$`, node)
		if !matches {
			return false
		}
	}
	return true
}

func ValidateTopic(topic string) bool {
	matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\.-]+$`, topic)
	return matches
}

// We don't match valid filenames, we match decent filenames
func ValidateFilename(filename string) bool {
	matches, _ := regexp.MatchString(`^[a-zA-Z0-9_\.-]+$`, filename)
	return matches
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

// Validate a list of ZK or Kafka hosts with optional ports
func CheckHostList(hosts []string, defaultPort int, appName string) string {
	for i, host := range hosts {
		hostparts := strings.Split(host, ":")
		hostport := defaultPort
		hostname := hostparts[0]

		if len(hostparts) == 2 {
			// Must be a hostname or IPv4 address with a port
			var err error
			hostport, err = strconv.Atoi(hostparts[1])
			if (err != nil) || (hostport == 0) {
				return fmt.Sprintf("One or more %s hostnames have invalid port components", appName)
			}
		}

		if len(hostparts) > 2 {
			// Must be an IPv6 address
			// Try without popping off the last segment as a port number first
			if ValidateIP(host) {
				hostname = host
			} else {
				// The full host didn't validate as an IP, so let's pull off the last piece as a port number and try again
				hostname = strings.Join(hostparts[:len(hostparts)-1], ":")

				hostport, err := strconv.Atoi(hostparts[len(hostparts)-1])
				if (err != nil) || (hostport == 0) {
					return fmt.Sprintf("One or more %s hostnames have invalid port components", appName)
				}
			}
		}

		if !ValidateHostname(hostname) {
			return fmt.Sprintf("One or more %s hostnames are invalid", appName)
		}

		hosts[i] = fmt.Sprintf("[%s]:%v", hostname, hostport)
	}

	return ""
}
