// Package connection manages (abstract) connections with retrying and reconnection.
package connection

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("connection")

// Connection encapsulates a single connection.
type Connection interface {
	fmt.Stringer
	io.Closer
}

// ConnectorImpl provides a set of methods to manage connections.
type ConnectorImpl interface {
	NewConnection(ctx context.Context) (Connection, error)
	IsConnectionClosedError(err error) bool
}

// Reconnector manages active Connection with automatic retrying and reconnection.
type Reconnector struct {
	connector ConnectorImpl

	mu sync.Mutex
	// +checklocks:mu
	activeConnection Connection
}

// GetOrOpenConnection gets or establishes new connection and returns it.
func (r *Reconnector) GetOrOpenConnection(ctx context.Context) (Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.activeConnection == nil {
		log(ctx).Debugf("establishing new connection...")

		conn, err := r.connector.NewConnection(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "error establishing connecting")
		}

		r.activeConnection = conn
	}

	return r.activeConnection, nil
}

// UsingConnection invokes the provided callback for a Connection.
func (r *Reconnector) UsingConnection(ctx context.Context, desc string, cb func(cli Connection) (interface{}, error)) (interface{}, error) {
	//nolint:wrapcheck
	return retry.WithExponentialBackoff(ctx, desc, func() (interface{}, error) {
		conn, err := r.GetOrOpenConnection(ctx)
		if err != nil {
			if r.connector.IsConnectionClosedError(err) {
				log(ctx).Errorf("connection failed: %v, will retry", err)
			}

			r.CloseActiveConnection(ctx)

			return nil, errors.Wrap(err, "error opening connection")
		}

		v, err := cb(conn)
		if err != nil {
			if r.connector.IsConnectionClosedError(err) {
				log(ctx).Errorf("connection closed: %v, will retry", err)

				r.CloseActiveConnection(ctx)
			}
		}

		return v, err
	}, r.connector.IsConnectionClosedError)
}

// UsingConnectionNoResult invokes the provided callback for a Connection.
func (r *Reconnector) UsingConnectionNoResult(ctx context.Context, desc string, cb func(cli Connection) error) error {
	_, err := r.UsingConnection(ctx, desc, func(cli Connection) (interface{}, error) {
		return nil, cb(cli)
	})

	return err
}

// CloseActiveConnection closes the active connection if any.
func (r *Reconnector) CloseActiveConnection(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c := r.activeConnection
	r.activeConnection = nil

	if c != nil {
		log(ctx).Debugf("closing active connection.")

		if err := c.Close(); err != nil {
			log(ctx).Errorf("error closing active connection: %v", err)
		}
	}
}

// NewReconnector creates a new Pool for a given connector.
func NewReconnector(conn ConnectorImpl) *Reconnector {
	return &Reconnector{
		connector: conn,
	}
}
