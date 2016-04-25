package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/config"
	"github.com/kopia/kopia/session"
)

var (
	configFile = app.Flag("config_file", "Specify config filename.").Default(getDefaultConfigFileName()).String()
)

func failOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func mustOpenSession() session.Session {
	s, err := openSession()
	failOnError(err)
	return s
}

func configFileName() string {
	if *configFile != "" {
		return *configFile
	}

	return getDefaultConfigFileName()
}

func getDefaultConfigFileName() string {
	return filepath.Join(getHomeDir(), ".kopia/config.json")
}

func getHomeDir() string {
	return os.Getenv("HOME")
}

func loadConfig() (*config.Config, error) {
	path := configFileName()
	if path == "" {
		return nil, fmt.Errorf("Cannot find config file. You may pass --config_file to specify config file location.")
	}

	var cfg config.Config

	//log.Printf("Loading config file from %v", path)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Error opening configuration file: %v", err)
	}
	defer f.Close()

	err = cfg.Load(f)
	if err == nil {
		return &cfg, nil
	}

	return nil, fmt.Errorf("Error loading configuration file: %v", err)
}

func openSession() (session.Session, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	storage, err := blob.NewStorage(cfg.Storage)
	if err != nil {
		return nil, err
	}

	storage = blob.NewLoggingWrapper(storage)

	var creds auth.Credentials

	return session.New(storage, creds)
}
