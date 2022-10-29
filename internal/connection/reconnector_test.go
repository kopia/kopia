package connection_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/connection"
	"github.com/kopia/kopia/internal/testlogging"
)

var (
	errFakeConnectionFailed = errors.New("fake connection failed")
	errSomeFatalError       = errors.New("some fatal error")
)

type fakeConnector struct {
	// +checkatomic
	nextConnectionID int32

	maxConnections        int
	connectionConcurrency int
	nextError             error
}

type fakeConnection struct {
	id       int32
	isClosed bool
}

func (c *fakeConnection) String() string {
	return fmt.Sprintf("fake-connection-%v", c.id)
}

func (c *fakeConnection) Close() error {
	return nil
}

func (c *fakeConnector) MaxConnections() int {
	return c.maxConnections
}

func (c *fakeConnector) ConnectionConcurrency() int {
	return c.connectionConcurrency
}

func (c *fakeConnector) NewConnection(ctx context.Context) (connection.Connection, error) {
	if err := c.nextError; err != nil {
		c.nextError = nil

		return nil, err
	}

	return &fakeConnection{
		atomic.AddInt32(&c.nextConnectionID, 1),
		false,
	}, nil
}

func (c *fakeConnector) IsConnectionClosedError(err error) bool {
	return errors.Is(err, errFakeConnectionFailed)
}

func TestConnection(t *testing.T) {
	fc := &fakeConnector{maxConnections: 2, connectionConcurrency: 1}

	ctx := testlogging.Context(t)

	r := connection.NewReconnector(fc)

	v, err := connection.UsingConnection(ctx, r, "first", func(cli connection.Connection) (interface{}, error) {
		require.EqualValues(t, 1, cli.(*fakeConnection).id)
		return "foo", nil
	})

	require.NoError(t, err)
	require.Equal(t, "foo", v)

	cnt := 0

	r.UsingConnectionNoResult(ctx, "second", func(cli connection.Connection) error {
		t.Logf("second called with %v", cli.(*fakeConnection).id)

		// still using connection # 1
		if cnt == 0 {
			cnt++
			require.EqualValues(t, 1, cli.(*fakeConnection).id)

			cli.(*fakeConnection).isClosed = true

			return errFakeConnectionFailed
		}

		require.EqualValues(t, 2, cli.(*fakeConnection).id)

		return nil
	})

	require.EqualValues(t, 2, fc.nextConnectionID)

	r.UsingConnectionNoResult(ctx, "third", func(cli connection.Connection) error {
		t.Logf("third called with %v", cli.(*fakeConnection).id)
		require.EqualValues(t, 2, cli.(*fakeConnection).id)
		return nil
	})

	require.EqualValues(t, 2, fc.nextConnectionID)

	r.UsingConnectionNoResult(ctx, "parallel-1", func(cli connection.Connection) error {
		t.Logf("parallel-1 called with %v", cli.(*fakeConnection).id)
		require.EqualValues(t, 2, cli.(*fakeConnection).id)

		r.UsingConnectionNoResult(ctx, "parallel-2", func(cli connection.Connection) error {
			t.Logf("parallel-2 called with %v", cli.(*fakeConnection).id)
			require.EqualValues(t, 2, cli.(*fakeConnection).id)
			return nil
		})

		return nil
	})

	r.CloseActiveConnection(ctx)

	require.NoError(t, r.UsingConnectionNoResult(ctx, "fourth", func(cli connection.Connection) error {
		t.Logf("fourth called with %v", cli.(*fakeConnection).id)
		require.EqualValues(t, 3, cli.(*fakeConnection).id)
		return nil
	}))

	r.CloseActiveConnection(ctx)

	fc.nextError = errSomeFatalError

	require.ErrorIs(t, r.UsingConnectionNoResult(ctx, "fifth", func(cli connection.Connection) error {
		t.Fatal("this won't be called")
		return nil
	}), errSomeFatalError)

	fc.nextError = errFakeConnectionFailed

	require.NoError(t, r.UsingConnectionNoResult(ctx, "sixth", func(cli connection.Connection) error {
		t.Logf("sixth called with %v", cli.(*fakeConnection).id)
		require.EqualValues(t, 4, cli.(*fakeConnection).id)
		return nil
	}))

	var eg errgroup.Group

	eg.Go(func() error {
		return r.UsingConnectionNoResult(ctx, "parallel-a", func(cli connection.Connection) error {
			time.Sleep(500 * time.Millisecond)
			t.Logf("parallel-a called with %v", cli.(*fakeConnection).id)
			return nil
		})
	})
	eg.Go(func() error {
		return r.UsingConnectionNoResult(ctx, "parallel-b", func(cli connection.Connection) error {
			time.Sleep(300 * time.Millisecond)
			t.Logf("parallel-b called with %v", cli.(*fakeConnection).id)
			return nil
		})
	})
	eg.Go(func() error {
		return r.UsingConnectionNoResult(ctx, "parallel-c", func(cli connection.Connection) error {
			time.Sleep(100 * time.Millisecond)
			t.Logf("parallel-c called with %v", cli.(*fakeConnection).id)
			return nil
		})
	})

	require.NoError(t, eg.Wait())
}
