// +build darwin,amd64 linux,amd64

// Package framework contains tools to enable multiple clients to connect to a
// central repository server and run robustness tests concurrently.
package framework

import (
	"context"
	"sync"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"
)

const nameLen int = 2

var clientKey = struct{}{}

// Client is a unique client for use in multiclient robustness tests.
type Client struct {
	ID string
}

func init() {
	petname.NonDeterministicMode()
}

func newClient() *Client {
	return &Client{
		ID: petname.Generate(nameLen, "-") + "-" + uuid.NewString(),
	}
}

// NewClientContext returns a copy of ctx with a new client.
func NewClientContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, clientKey, newClient())
}

// NewClientContexts returns copies of ctx with n new clients.
func NewClientContexts(ctx context.Context, n int) []context.Context {
	ctxs := make([]context.Context, n)
	for i := range ctxs {
		ctxs[i] = NewClientContext(ctx)
	}

	return ctxs
}

// UnwrapContext returns a client from the given context.
func UnwrapContext(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey).(*Client)
	return c
}

// RunAllAndWait runs the provided function asynchronously for each of the
// given client contexts and waits for all of them to finish.
func RunAllAndWait(ctxs []context.Context, f func(context.Context)) {
	var wg sync.WaitGroup

	for _, ctx := range ctxs {
		wg.Add(1)

		go func(ctx context.Context) {
			f(ctx)
			wg.Done()
		}(ctx)
	}

	wg.Wait()
}
