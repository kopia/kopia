//go:build !no_extra_providers

package sftp

import (
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/timetrack"
)

// silenceableServer is an in-process SSH server with the SFTP subsystem that
// can be silenced: it then keeps connections open but neither reads nor writes,
// like a server or network path that hangs without closing the connection.
type silenceableServer struct {
	host           string
	port           int
	knownHostsFile string

	silenced   atomic.Bool
	shutdown   chan struct{}
	connsMutex sync.Mutex
	conns      []net.Conn
}

func (s *silenceableServer) silence() {
	s.silenced.Store(true)
}

func (s *silenceableServer) options(t *testing.T) *Options {
	t.Helper()

	return &Options{
		Path:           t.TempDir(),
		Host:           s.host,
		Port:           s.port,
		Username:       "user",
		Password:       "password",
		KnownHostsFile: s.knownHostsFile,
	}
}

// gatedConn stops passing data in either direction once the server is silenced.
type gatedConn struct {
	net.Conn

	s *silenceableServer
}

func (c gatedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if c.s.silenced.Load() {
		<-c.s.shutdown
		return 0, net.ErrClosed
	}

	return n, err
}

func (c gatedConn) Write(b []byte) (int, error) {
	if c.s.silenced.Load() {
		<-c.s.shutdown
		return 0, net.ErrClosed
	}

	return c.Conn.Write(b)
}

func startSilenceableServer(t *testing.T) *silenceableServer {
	t.Helper()

	_, hostPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	hostKey, err := ssh.NewSignerFromKey(hostPrivateKey)
	require.NoError(t, err)

	config := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			return &ssh.Permissions{}, nil
		},
	}
	config.AddHostKey(hostKey)

	l, err := (&net.ListenConfig{}).Listen(testlogging.Context(t), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().(*net.TCPAddr) //nolint:forcetypeassert

	s := &silenceableServer{
		host:           addr.IP.String(),
		port:           addr.Port,
		knownHostsFile: filepath.Join(t.TempDir(), "known_hosts"),
		shutdown:       make(chan struct{}),
	}

	knownHostsLine := knownhosts.Line([]string{knownhosts.Normalize(l.Addr().String())}, hostKey.PublicKey())
	require.NoError(t, os.WriteFile(s.knownHostsFile, []byte(knownHostsLine+"\n"), 0o600))

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			s.connsMutex.Lock()
			s.conns = append(s.conns, conn)
			s.connsMutex.Unlock()

			go serveSSH(gatedConn{conn, s}, config)
		}
	}()

	t.Cleanup(func() {
		close(s.shutdown)
		l.Close()

		s.connsMutex.Lock()
		defer s.connsMutex.Unlock()

		for _, c := range s.conns {
			c.Close()
		}
	})

	return s
}

func serveSSH(conn net.Conn, config *ssh.ServerConfig) {
	sconn, chans, reqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return
	}

	defer sconn.Close()

	// replies to keepalive requests
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			return
		}

		go func() {
			for req := range requests {
				isSFTP := req.Type == "subsystem" && len(req.Payload) > 4 && string(req.Payload[4:]) == "sftp"
				req.Reply(isSFTP, nil)

				if isSFTP {
					go func() {
						srv, err := sftp.NewServer(channel)
						if err != nil {
							return
						}

						srv.Serve()
						srv.Close()
					}()
				}
			}
		}()
	}
}

func useShortTimeouts(t *testing.T) {
	t.Helper()

	oldConnect, oldKeepalive, oldIdle := connectTimeout, keepaliveInterval, readIdleTimeout

	connectTimeout = 5 * time.Second
	keepaliveInterval = 200 * time.Millisecond
	readIdleTimeout = 2 * time.Second

	t.Cleanup(func() {
		connectTimeout, keepaliveInterval, readIdleTimeout = oldConnect, oldKeepalive, oldIdle
	})
}

// Not parallel: tests in this file change the package-level timeouts.

func Test_getSFTPClient_silentServerFailsConnect(t *testing.T) {
	useShortTimeouts(t)

	s := startSilenceableServer(t)
	s.silence()

	timer := timetrack.StartTimer()

	_, err := getSFTPClient(testlogging.Context(t), s.options(t))
	require.Error(t, err)
	require.Less(t, timer.Elapsed(), 10*time.Second)
}

func Test_getSFTPClient_silentConnectionFailsRequest(t *testing.T) {
	useShortTimeouts(t)

	s := startSilenceableServer(t)
	opt := s.options(t)

	conn, err := getSFTPClient(testlogging.Context(t), opt)
	require.NoError(t, err)

	t.Cleanup(func() { conn.Close() })

	_, err = conn.currentClient.Stat(opt.Path)
	require.NoError(t, err)

	s.silence()

	timer := timetrack.StartTimer()

	_, err = conn.currentClient.Stat(opt.Path)
	require.Error(t, err)
	require.Less(t, timer.Elapsed(), 10*time.Second)

	// the error must trigger reconnecting and retrying
	require.True(t, (&sftpImpl{}).IsConnectionClosedError(err), "unexpected error: %v", err)
}

func Test_getSFTPClient_idleConnectionIsKeptAlive(t *testing.T) {
	useShortTimeouts(t)

	s := startSilenceableServer(t)
	opt := s.options(t)

	conn, err := getSFTPClient(testlogging.Context(t), opt)
	require.NoError(t, err)

	t.Cleanup(func() { conn.Close() })

	time.Sleep(2 * readIdleTimeout)

	_, err = conn.currentClient.Stat(opt.Path)
	require.NoError(t, err)
}
