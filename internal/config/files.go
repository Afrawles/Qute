package config

import (
	"fmt"
	"os"
	"path/filepath"
)

const defaultConfigDir = ".qute"

type Config struct {
	Dir            string
	CAFile         string
	ServerCertFile string
	ServerKeyFile  string
	ClientCertFile string
	ClientKeyFile  string
	RootClientCertFile string
	RootClientKeyFile string
	NobodyClientCertFile string
	NobodyClientKeyFile string
	ACLModelFile string
	ACLPolicyFile string
}

// Load resolves configuration paths.
func Load() (*Config, error) {
	dir := os.Getenv("CONFIG_DIR")

	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("could not determine home directory: %w", err)
		}

		dir = filepath.Join(home, defaultConfigDir)
	}

	cfg := &Config{
		Dir:            dir,
		CAFile:         filepath.Join(dir, "ca.pem"),
		ServerCertFile: filepath.Join(dir, "server.pem"),
		ServerKeyFile:  filepath.Join(dir, "server-key.pem"),
		ClientCertFile: filepath.Join(dir, "client.pem"),
		ClientKeyFile:  filepath.Join(dir, "client-key.pem"),
		RootClientKeyFile: filepath.Join(dir, "root-client-key.pem"),
		RootClientCertFile: filepath.Join(dir, "root-client.pem"),
		NobodyClientCertFile: filepath.Join(dir, "nobody-client.pem"),
		NobodyClientKeyFile: filepath.Join(dir, "nobody-client-key.pem"),
		ACLModelFile: filepath.Join(dir, "model.conf"),
		ACLPolicyFile: filepath.Join(dir, "policy.csv"),
	}

	return cfg, nil
}
