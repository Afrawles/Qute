package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	// Load server/client certificate
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	var caPool *x509.CertPool

	// Load CA certificate
	if cfg.CAFile != "" {
		caBytes, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		caPool = x509.NewCertPool()

		if ok := caPool.AppendCertsFromPEM(caBytes); !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %s", cfg.CAFile)
		}

		tlsConfig.RootCAs = caPool
		tlsConfig.ServerName = cfg.ServerAddress
	}

	// Server-side TLS settings
	if cfg.Server && caPool != nil {
		tlsConfig.ClientCAs = caPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}
