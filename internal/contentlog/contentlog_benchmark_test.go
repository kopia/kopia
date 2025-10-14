package contentlog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/contentparam"
	"github.com/kopia/kopia/repo/content/index"
)

func BenchmarkLogger(b *testing.B) {
	ctx := context.Background()

	cid, err := index.ParseID("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	require.NoError(b, err)

	// context params
	ctx = contentlog.WithParams(ctx,
		logparam.String("service", "test-service"),
		logparam.Int("version", 2),
		contentparam.ContentID("cid", cid),
	)

	// logger params
	l := contentlog.NewLogger(func(data []byte) {},
		logparam.String("lservice", "test-service"),
	)

	for b.Loop() {
		contentlog.Log(ctx, l, "baz")
		contentlog.Log1(ctx, l, "baz", logparam.String("arg1", "123\x01foobar"))
		contentlog.Log2(ctx, l, "baz", logparam.Int("arg1", 123), logparam.Int("arg2", 456))
		contentlog.Log3(ctx, l, "baz", logparam.Int("arg1", 123), logparam.Int("arg2", 456), logparam.Int("arg3", 789))
		contentlog.Log4(ctx, l, "baz", logparam.Int("arg1", 123), logparam.Int("arg2", 456), logparam.Int("arg3", 789), logparam.Int("arg4", 101112))
		contentlog.Log5(ctx, l, "baz", logparam.Int("arg1", 123), logparam.Int("arg2", 456), logparam.Int("arg3", 789), logparam.Int("arg4", 101112), logparam.Int("arg5", 123456))
		contentlog.Log6(ctx, l, "baz", logparam.Int("arg1", 123), logparam.Int("arg2", 456), logparam.Int("arg3", 789), logparam.Int("arg4", 101112), logparam.Int("arg5", 123456), logparam.Int("arg6", 123456))
	}
}
