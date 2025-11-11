package testutil

import (
	"bytes"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// RunDockerAndGetOutputOrSkip runs Docker and returns the output as a string.
func RunDockerAndGetOutputOrSkip(tb testing.TB, args ...string) string {
	tb.Helper()
	tb.Logf("running docker %v", args)

	c := exec.Command("docker", args...)

	var stderr bytes.Buffer

	c.Stderr = &stderr

	out, err := c.Output()
	if err != nil {
		// skip or fail hard when running in CI environment.
		if os.Getenv("CI") == "" {
			tb.Skipf("unable to run docker: %v %s (stderr %s)", err, out, stderr.String())
		} else {
			tb.Fatalf("unable to run docker: %v %s (stderr %s)", err, out, stderr.String())
		}
	}

	return strings.TrimSpace(string(out))
}

// RunContainerAndKillOnCloseOrSkip runs "docker run" and ensures that resulting container is killed
// on exit. Returns containerID.
func RunContainerAndKillOnCloseOrSkip(t *testing.T, args ...string) string {
	t.Helper()

	containerID := RunDockerAndGetOutputOrSkip(t, args...)

	t.Cleanup(func() {
		RunDockerAndGetOutputOrSkip(t, "kill", containerID)
	})

	return containerID
}

// GetContainerMappedPortAddress returns <host>:<port> that can be used to connect to
// a given container and private port.
func GetContainerMappedPortAddress(t *testing.T, containerID, privatePort string) string {
	t.Helper()

	portMapping := RunDockerAndGetOutputOrSkip(t, "port", containerID, privatePort)

	p := strings.LastIndex(portMapping, ":")
	if p < 0 {
		t.Fatalf("invalid port mapping: %v", portMapping)
	}

	colonPort := portMapping[p:]

	dockerhost := os.Getenv("DOCKER_HOST")
	if dockerhost == "" {
		return "localhost" + colonPort
	}

	u, err := url.Parse(dockerhost)
	if err != nil {
		t.Fatalf("unable to parse DOCKER_HOST: %v", err)
	}

	return u.Hostname() + colonPort
}
