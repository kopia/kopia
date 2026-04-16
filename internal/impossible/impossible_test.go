package impossible_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/impossible"
)

func TestImpossible(t *testing.T) {
	impossible.PanicOnError(nil)

	someErr := errors.New("some error")
	require.PanicsWithError(t, someErr.Error(), func() {
		impossible.PanicOnError(someErr)
	})
}
