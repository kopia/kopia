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
	"github.com/kopia/kopia/internal/testutil"
)

var (
	errFakeConnectionFailed = errors.New("fake connection failed")
	errSomeFatalError       = errors.New("some fatal error")
)

type fakeConnector struct {
	nextConnectionID atomic.Int32

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
		c.nextConnectionID.Add(1),
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

	v, err := connection.UsingConnection(ctx, r, "first", func(cli connection.Connection) (any, error) {
		require.EqualValues(t, 1, testutil.EnsureType[*fakeConnection](t, cli).id)

		return "foo", nil
	})

	require.NoError(t, err)
	require.Equal(t, "foo", v)

	cnt := 0

	r.UsingConnectionNoResult(ctx, "second", func(cli connection.Connection) error {
		fcon := testutil.EnsureType[*fakeConnection](t, cli)

		t.Log("second called with", fcon.id)

		// still using connection # 1
		if cnt == 0 {
			cnt++

			require.EqualValues(t, 1, fcon.id)

			fcon.isClosed = true

			return errFakeConnectionFailed
		}

		require.EqualValues(t, 2, fcon.id)

		return nil
	})

	require.EqualValues(t, 2, fc.nextConnectionID.Load())

	r.UsingConnectionNoResult(ctx, "third", func(cli connection.Connection) error {
		id0 := testutil.EnsureType[*fakeConnection](t, cli).id

		t.Log("third called with", id0)
		require.EqualValues(t, 2, id0)

		return nil
	})

	require.EqualValues(t, 2, fc.nextConnectionID.Load())

	r.UsingConnectionNoResult(ctx, "parallel-1", func(cli connection.Connection) error {
		id1 := testutil.EnsureType[*fakeConnection](t, cli).id

		t.Log("parallel-1 called with", id1)
		require.EqualValues(t, 2, id1)

		r.UsingConnectionNoResult(ctx, "parallel-2", func(cli connection.Connection) error {
			id2 := testutil.EnsureType[*fakeConnection](t, cli).id

			t.Log("parallel-2 called with", id2)
			require.EqualValues(t, 2, id2)

			return nil
		})

		return nil
	})

	r.CloseActiveConnection(ctx)

	require.NoError(t, r.UsingConnectionNoResult(ctx, "fourth", func(cli connection.Connection) error {
		id3 := testutil.EnsureType[*fakeConnection](t, cli).id

		t.Log("fourth called with", id3)
		require.EqualValues(t, 3, id3)

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
		id4 := testutil.EnsureType[*fakeConnection](t, cli).id

		t.Log("sixth called with", id4)
		require.EqualValues(t, 4, id4)

		return nil
	}))

	var eg errgroup.Group

	eg.Go(func() error {
		return r.UsingConnectionNoResult(ctx, "parallel-a", func(cli connection.Connection) error {
			time.Sleep(500 * time.Millisecond)
			t.Log("parallel-a called with", testutil.EnsureType[*fakeConnection](t, cli).id)

			return nil
		})
	})
	eg.Go(func() error {
		return r.UsingConnectionNoResult(ctx, "parallel-b", func(cli connection.Connection) error {
			time.Sleep(300 * time.Millisecond)
			t.Log("parallel-b called with", testutil.EnsureType[*fakeConnection](t, cli).id)

			return nil
		})
	})
	eg.Go(func() error {
		return r.UsingConnectionNoResult(ctx, "parallel-c", func(cli connection.Connection) error {
			time.Sleep(100 * time.Millisecond)
			t.Log("parallel-c called with", testutil.EnsureType[*fakeConnection](t, cli).id)

			return nil
		})
	})

	require.NoError(t, eg.Wait())
}
