package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"strings"
	"errors"
	"fmt"
)

// Create TLS Config from application config file
func NewTLSConfig(appConfig *BurrowConfig, name string) (*tls.Config, error) {
	config, _ := appConfig.Tls[name]
	tlsConfig := &tls.Config{InsecureSkipVerify: config.NoVerify}
	if config.CertFile != "" && config.KeyFile != "" {
		keyPair, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{keyPair}
		tlsConfig.BuildNameToCertificate()
	}
	if config.CAFile != "" {
		cas := x509.NewCertPool()
		caCerts, err := ioutil.ReadFile(config.CAFile)
		if err != nil {
			return nil, err
		}
		cas.AppendCertsFromPEM(caCerts)
		tlsConfig.RootCAs = cas
	}
	// Configure Version
	if config.Versions != "" {
		versionMin, versionMax, err := convertTLSVersions(config.Versions)
		if err != nil {
			return nil, err
		}
		tlsConfig.MinVersion = versionMin
		tlsConfig.MaxVersion = versionMax
	}
	// Configure Cipher suites
	if config.Ciphers != "" {
		ciphers, err := convertTLSCiphers(config.Ciphers)
		if err != nil {
			return nil, err
		}
		tlsConfig.CipherSuites = ciphers
	}
	return tlsConfig, nil
}

func convertTLSVersions(versions string) (versionMin uint16, versionMax uint16, err error) {
	versionMap := map[string]uint16{
		"SSLv3": tls.VersionSSL30,
		"SSLv3.0": tls.VersionSSL30,
		"TLSv1.0": tls.VersionTLS10,
		"TLSv1.1": tls.VersionTLS11,
		"TLSv1.2": tls.VersionTLS12,
	}
	versionNames := strings.Split(versions,",")
	versionMin, versionMax = tls.VersionTLS12, tls.VersionSSL30
	for _,versionName := range versionNames {
		versionName = strings.TrimSpace(versionName)
		version, ok := versionMap[versionName]
		if !ok {
			return 0, 0, errors.New(fmt.Sprintf("TLS Version %s unsupported", versionName))
		}
		if version < versionMin {
			versionMin = version
		}
		if version > versionMax {
			versionMax = version
		}
	}
	return
}

func convertTLSCiphers(ciphers string) ( cipherList[]uint16, err error) {
	// Some ciphers are not supported in Golang 1.6
	cipherMap := map[string]uint16{
		"RSA_WITH_RC4_128_SHA": tls.TLS_RSA_WITH_RC4_128_SHA,
		"RSA_WITH_3DES_EDE_CBC_SHA": tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
		"RSA_WITH_AES_128_CBC_SHA": tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		"RSA_WITH_AES_256_CBC_SHA": tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		// "RSA_WITH_AES_128_CBC_SHA256": tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
		"RSA_WITH_AES_128_GCM_SHA256": tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		"RSA_WITH_AES_256_GCM_SHA384": tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		"ECDHE_ECDSA_WITH_RC4_128_SHA": tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
		"ECDHE_ECDSA_WITH_AES_128_CBC_SHA": tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		"ECDHE_ECDSA_WITH_AES_256_CBC_SHA": tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		"ECDHE_RSA_WITH_RC4_128_SHA": tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
		"ECDHE_RSA_WITH_3DES_EDE_CBC_SHA": tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
		"ECDHE_RSA_WITH_AES_128_CBC_SHA": tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		"ECDHE_RSA_WITH_AES_256_CBC_SHA": tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		// "ECDHE_ECDSA_WITH_AES_128_CBC_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
		// "ECDHE_RSA_WITH_AES_128_CBC_SHA256": tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
		"ECDHE_RSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		"ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"ECDHE_RSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		"ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		// "ECDHE_RSA_WITH_CHACHA20_POLY1305": tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		// "ECDHE_ECDSA_WITH_CHACHA20_POLY1305": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	}
	cipherNames := strings.Split(ciphers,",")
	cipherList = make([]uint16, len(cipherNames))
	for i,cipherName := range cipherNames {
		cipherName = strings.TrimSpace(cipherName)
		cipher, ok := cipherMap[cipherName]
		if !ok {
			return nil, errors.New(fmt.Sprintf("TLS Cipher %s unsupported", cipherName))
		}
		cipherList[i] = cipher
	}
	return
}
