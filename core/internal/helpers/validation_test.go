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
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestSet struct {
	TestValue string
	Result    bool
}

var testIP = []TestSet{
	{"1.2.3.4", true},
	{"127.0.0.1", true},
	{"204.27.175.1", true},
	{"255.255.255.255", true},
	{"256.1.2.3", false},
	{"1.2.3.4.5", false},
	{"notanip", false},
	{"2001:0db8:0a0b:12f0:0000:0000:0000:0001", true},
	{"2001:db8:a0b:12f0::1", true},
	{"2001:db8::1", true},
	{"2001:db8::2:1", true},
	{"2001:db8:0:1:1:1:1:1", true},
	{"2001:0db8:0a0b:12f0:0000:0000:0000:0001:0004", false},
}

func TestValidateIP(t *testing.T) {
	for i, testSet := range testIP {
		result := ValidateIP(testSet.TestValue)
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}

var testHostnames = []TestSet{
	{"hostname", true},
	{"host0", true},
	{"host.example.com", true},
	{"example.com", true},
	{"thissegmentiswaytoolongbecauseitshouldnotbemorethansixtythreecharacters.foo.com", false},
	{"underscores_are.not.valid.com", false},
	{"800.hostnames.starting.with.numbers.are.valid.because.people.suck.org", true},
	{"hostnames-.may.not.end.with.a.dash.com", false},
	{"no spaces.com", false},
}

func TestValidateHostname(t *testing.T) {
	for i, testSet := range testHostnames {
		result := ValidateHostname(testSet.TestValue)
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}

var testZkPaths = []TestSet{
	{"/", true},
	{"", false},
	{"/no/trailing/slash/", false},
	{"/this/is/fine", true},
	{"/underscores_are/ok", true},
	{"/dashes-are/fine/too", true},
	{"/no spaces/in/paths", false},
}

func TestValidateZookeeperPath(t *testing.T) {
	for i, testSet := range testZkPaths {
		result := ValidateZookeeperPath(testSet.TestValue)
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}

var testTopics = []TestSet{
	{"metrics", true},
	{"__consumer_offsets", true},
	{"stars*arent_valid_you_monster", false},
	{"dashes-are-ok", true},
	{"numbers0-are_fine", true},
	{"no spaces", false},
	{"dots.are_ok", true},
}

func TestValidateTopic(t *testing.T) {
	for i, testSet := range testTopics {
		result := ValidateTopic(testSet.TestValue)
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}

var testEmails = []TestSet{
	{"ok@example.com", true},
	{"need@domain", false},
	{"gotta.have.an.at", false},
	{"nogood@", false},
	{"this.is@ok.com", true},
}

func TestValidateEmail(t *testing.T) {
	for i, testSet := range testEmails {
		result := ValidateEmail(testSet.TestValue)
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}

var testUrls = []TestSet{
	{"http://foo.com/blah_blah", true},
	{"http://foo.com/blah_blah/", true},
	{"http://www.example.com/wpstyle/?p=364", true},
	{"https://www.example.com/foo/?bar=baz&inga=42&quux", true},
	{"http://✪df.ws/123", true},
	{"http://userid:password@example.com:8080", true},
	{"http://userid:password@example.com:8080/", true},
	{"http://userid@example.com", true},
	{"http://userid@example.com/", true},
	{"http://userid@example.com:8080", true},
	{"http://userid@example.com:8080/", true},
	{"http://userid:password@example.com", true},
	{"http://userid:password@example.com/", true},
	{"http://142.42.1.1/", true},
	{"http://142.42.1.1:8080/", true},
	{"http://➡.ws/䨹", true},
	{"http://⌘.ws", true},
	{"http://⌘.ws/", true},
	{"http://foo.com/blah_(wikipedia)#cite-1", true},
	{"http://foo.com/blah_(wikipedia)_blah#cite-1", true},
	{"http://foo.com/unicode_(✪)_in_parens", true},
	{"http://foo.com/(something)?after=parens", true},
	{"http://☺.damowmow.com/", true},
	{"http://code.google.com/events/#&product=browser", true},
	{"http://j.mp", true},
	{"ftp://foo.bar/baz", true},
	{"http://foo.bar/?q=Test%20URL-encoded%20stuff", true},
	{"http://مثال.إختبار", true},
	{"http://例子.测试", true},
	{"http://उदाहरण.परीक्षा", true},
	{"http://-.~_!$&'()*+,;=:%40:80%2f::::::@example.com", true},
	{"http://1337.net", true},
	{"http://a.b-c.de", true},
	{"http://223.255.255.254", true},
	{"http:// shouldfail.com", false},
	{":// should fail", false},
}

func TestValidateUrl(t *testing.T) {
	for i, testSet := range testUrls {
		result := ValidateURL(testSet.TestValue)
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}

var testHostPorts = []TestSet{
	{"1.2.3.4:3453", true},
	{"127.0.0.1:2342", true},
	{"204.27.175.1:4", true},
	{"256.1.2.3:3743", false},
	{"1.2.3.4.5:2452", false},
	{"[2001:0db8:0a0b:12f0:0000:0000:0000:0001]:4356", true},
	{"[2001:db8:a0b:12f0::1]:234", true},
	{"[2001:db8::1]:3453", true},
	{"2001:db8:0:1:1:1:1:1:3453", false},
	{"[2001:0db8:0a0b:12f0:0000:0000:0000:0001:0004]:4533", false},
	{"hostname:3432", true},
	{"host0:4234", true},
	{"host.example.com:23", true},
	{"thissegmentiswaytoolongbecauseitshouldnotbemorethansixtythreecharacters.foo.com:36334", false},
	{"underscores_are.not.valid.com:3453", false},
}

func TestValidateHostList(t *testing.T) {
	for i, testSet := range testHostPorts {
		result := ValidateHostList([]string{testSet.TestValue})
		assert.Equalf(t, testSet.Result, result, "Test %v - Expected '%v' to return %v, not %v", i, testSet.TestValue, testSet.Result, result)
	}
}
