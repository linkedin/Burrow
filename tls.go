package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
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
	return tlsConfig, nil
}
