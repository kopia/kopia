//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package framework

import (
	"context"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"
)

const nameLen int = 2

type clientKeyT struct{}

var clientKey clientKeyT

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
		ctxs[i] = NewClientContext(ctx) //nolint:fatcontext
	}

	return ctxs
}

// UnwrapContext returns a client from the given context.
func UnwrapContext(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey).(*Client)
	return c
}
