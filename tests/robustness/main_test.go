// +build darwin linux

package robustness

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine

const (
	fsDataPath     = "/tmp/robustness-data"
	fsMetadataPath = "/tmp/robustness-metadata"
	s3DataPath     = "robustness-data"
	s3MetadataPath = "robustness-metadata"
)

func TestMain(m *testing.M) {
	var err error

	eng, err = engine.NewEngine()
	if err != nil {
		log.Println("skipping robustness tests:", err)

		if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
			os.Exit(0)
		}

		os.Exit(1)
	}

	switch {
	case os.Getenv(engine.S3BucketNameEnvKey) != "":
		eng.InitS3(context.Background(), s3DataPath, s3MetadataPath)
	default:
		eng.InitFilesystem(context.Background(), fsDataPath, fsMetadataPath)
	}

	result := m.Run()

	err = eng.Cleanup()
	if err != nil {
		panic(err)
	}

	os.Exit(result)
}
