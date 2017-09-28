package main

import (
	"testing"
)

func TestSplitHttpListen(t *testing.T) {
	t.Log("Basic URL")
	scheme, host, port := SplitHttpListen("http://myhost:1234")
	assertStringEqual(t, "http", scheme)
	assertStringEqual(t, "myhost", host)
	assertUInt16Equal(t, 1234, port)

	t.Log("HTTPS URL")
	scheme, host, port = SplitHttpListen("https://myhost:1234")
	assertStringEqual(t, "https", scheme)
	assertStringEqual(t, "myhost", host)
	assertUInt16Equal(t, 1234, port)

	t.Log("Invalid FTPS URL")
	scheme, host, port = SplitHttpListen("ftp://myhost:1234")
	assertStringEqual(t, "", scheme)
	assertUInt16Equal(t, 0, port)

	t.Log("Scheme-less URL")
	scheme, host, port = SplitHttpListen("myhost:1234")
	assertStringEqual(t, "http", scheme)
	assertStringEqual(t, "myhost", host)
	assertUInt16Equal(t, 1234, port)

	t.Log("Invalid Port-less URL")
	scheme, host, port = SplitHttpListen("myhost:abcd")
	assertStringEqual(t, "", scheme)
	assertUInt16Equal(t, 0, port)
}
