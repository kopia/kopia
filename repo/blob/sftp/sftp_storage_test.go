package sftp_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	psftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

const (
	t1 = "392ee1bc299db9f235e046a62625afb84902"
	t2 = "2a7ff4f29eddbcd4c18fa9e73fec20bbb71f"
	t3 = "0dae5918f83e6a24c8b3e274ca1026e43f24"
)

func TestSSHStorage(t *testing.T) {
	go server(t)

	cwd, err := os.Getwd()
	if err != nil {
		t.Errorf("unable to getwd: %s", err)
	}

	host := "localhost"
	port := 2222
	keyfile := filepath.Join(cwd, "id_rsa")
	usr, err := user.Current()
	if err != nil {
		t.Errorf("unable to get current user: %s", err)
	}

	ctx := context.Background()
	st, err := sftp.New(ctx, &sftp.Options{
		Path:       ".",
		Host:       host,
		Username:   usr.Username,
		Port:       port,
		Keyfile:    keyfile,
		KnownHosts: filepath.Join(cwd, "known_hosts"),
	})

	if err != nil {
		t.Fatalf("unable to connect to SSH: %v", err)
	}

	assertNoError(t, st.PutBlob(ctx, t1, []byte{1}))
	time.Sleep(1 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution
	assertNoError(t, st.PutBlob(ctx, t2, []byte{1}))
	time.Sleep(1 * time.Second)
	assertNoError(t, st.PutBlob(ctx, t3, []byte{1}))

	deleteBlobs(ctx, t, st)

	blobtesting.VerifyStorage(ctx, t, st)

	// delete everything again
	deleteBlobs(ctx, t, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func server(t *testing.T) {
	debugStream := os.Stderr

	c := createConfig(t)

	listener, _ := net.Listen("tcp", "127.0.0.1:2222")
	for {
		conn, _ := listener.Accept()
		_, chans, reqs, _ := ssh.NewServerConn(conn, c)
		go ssh.DiscardRequests(reqs)

		for newChannel := range chans {
			// Channels have a type, depending on the application level
			// protocol intended. In the case of an SFTP session, this is "subsystem"
			// with a payload string of "<length=4>sftp"
			fmt.Fprintf(debugStream, "Incoming channel: %s\n", newChannel.ChannelType())
			if newChannel.ChannelType() != "session" {
				_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
				fmt.Fprintf(debugStream, "Unknown channel type: %s\n", newChannel.ChannelType())
				continue
			}

			channel, requests, err := newChannel.Accept()
			if err != nil {
				log.Fatalf("Could not accept channel: %v", err)
			}
			fmt.Fprintf(debugStream, "Channel accepted\n")

			// Sessions have out-of-band requests such as "shell",
			// "pty-req" and "env".  Here we handle only the
			// "subsystem" request.
			go func(in <-chan *ssh.Request) {
				for req := range in {
					fmt.Fprintf(debugStream, "Request: %v\n", req.Type)
					ok := false
					switch req.Type {
					case "subsystem":
						fmt.Fprintf(debugStream, "Subsystem: %s\n", req.Payload[4:])
						if string(req.Payload[4:]) == "sftp" {
							ok = true
						}
					default:
						ok = false
					}
					fmt.Fprintf(debugStream, " - accepted: %v\n", ok)
					_ = req.Reply(ok, nil)
				}
			}(requests)

			serverOptions := []psftp.ServerOption{
				psftp.WithDebug(debugStream),
			}

			fmt.Fprintf(debugStream, "Read write server\n")

			server, err := psftp.NewServer(
				channel,
				serverOptions...,
			)
			if err != nil {
				log.Fatal(err)
			}
			if err := server.Serve(); err == io.EOF {
				channel.Close()
				server.Close()
				fmt.Fprintln(debugStream, "sftp client exited session.")
			} else if err != nil {
				fmt.Fprintf(debugStream, "sftp server completed with error: %s", err)
			}
		}
	}

}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func createConfig(t *testing.T) *ssh.ServerConfig {
	cwd, err := os.Getwd()
	if err != nil {
		t.Errorf("unable to getwd: %s", err)
	}

	// Public key authentication is done by comparing
	// the public key of a received connection
	// with the entries in the authorized_keys file.
	authorizedKeysBytes, err := ioutil.ReadFile(filepath.Join(cwd, "known_hosts"))
	if err != nil {
		t.Errorf("failed to load authorized_keys, err: %v", err)
	}

	authorizedKeysMap := map[string]bool{}
	for len(authorizedKeysBytes) > 0 {
		pubKey, _, _, rest, e := ssh.ParseAuthorizedKey(authorizedKeysBytes)
		if e != nil {
			log.Fatal(e)
		}

		authorizedKeysMap[string(pubKey.Marshal())] = true
		authorizedKeysBytes = rest
	}

	c := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			if authorizedKeysMap[string(pubKey.Marshal())] {
				return &ssh.Permissions{
					// Record the public key used for authentication.
					Extensions: map[string]string{
						"pubkey-fp": ssh.FingerprintSHA256(pubKey),
					},
				}, nil
			}
			return nil, fmt.Errorf("unknown public key for %q", c.User())
		},
	}

	privateBytes, err := ioutil.ReadFile(filepath.Join(cwd, "id_rsa"))
	if err != nil {
		log.Fatal("Failed to load private key: ", err)
	}

	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		log.Fatal("Failed to parse private key: ", err)
	}

	c.AddHostKey(private)

	return c
}

func deleteBlobs(ctx context.Context, t *testing.T, st blob.Storage) {
	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear sftp storage: %v", err)
	}
}
