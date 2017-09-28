package main

import (
	"crypto/tls"
	"testing"
)

func TestNewTLSConfig_Ciphers(t *testing.T) {
	config := &BurrowConfig{}
	config.Tls = map[string]*TLSConfig{
		"default": &TLSConfig{Ciphers: "RSA_WITH_AES_256_GCM_SHA384,ECDHE_ECDSA_WITH_AES_256_CBC_SHA"},
	}
	tlsConfig, err := NewTLSConfig(config, "default")
	assertNoError(t, err)
	assertIntEqual(t, 2, len(tlsConfig.CipherSuites))
	assertUInt16Equal(t, tls.TLS_RSA_WITH_AES_256_GCM_SHA384, tlsConfig.CipherSuites[0])
	assertUInt16Equal(t, tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, tlsConfig.CipherSuites[1])
}

func TestNewTLSConfig_Versions(t *testing.T) {
	config := &BurrowConfig{}
	config.Tls = map[string]*TLSConfig{
		"default": &TLSConfig{Versions: "TLSv1.0,TLSv1.1"},
	}
	tlsConfig, err := NewTLSConfig(config, "default")
	assertNoError(t, err)
	assertUInt16Equal(t, tls.VersionTLS10, tlsConfig.MinVersion)
	assertUInt16Equal(t, tls.VersionTLS11, tlsConfig.MaxVersion)
}
