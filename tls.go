package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

// Create TLS Config from application config file
func NewTLSConfig(appConfig *BurrowConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	if appConfig.Tls.CertFile != "" && appConfig.Tls.KeyFile != "" {
		keyPair, err := tls.LoadX509KeyPair(appConfig.Tls.CertFile, appConfig.Tls.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{keyPair}
	}
	if appConfig.Tls.CAFile != "" {
		cas := x509.NewCertPool()
		caCerts, err := ioutil.ReadFile(appConfig.Tls.CAFile)
		if err != nil {
			return nil, err
		}
		cas.AppendCertsFromPEM(caCerts)
		tlsConfig.RootCAs = cas
	}
	return tlsConfig, nil
}
