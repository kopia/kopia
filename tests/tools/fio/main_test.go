package fio

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	fioExe := os.Getenv(FioExeEnvKey)
	fioImg := os.Getenv(FioDockerImageEnvKey)

	if fioExe == "" && fioImg == "" {
		fmt.Printf("Skipping fio tests if neither %s no %s is set\n", FioExeEnvKey, FioDockerImageEnvKey)
		os.Exit(0)
	}

	result := m.Run()

	os.Exit(result)
}
