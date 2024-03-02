package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMutexMap_ExclusiveLock(t *testing.T) {
	var m mutexMap

	require.Empty(t, m.entries)
	m.exclusiveLock("foo")
	require.Len(t, m.entries, 1)
	require.False(t, m.tryExclusiveLock("foo"))
	require.True(t, m.tryExclusiveLock("bar"))
	require.False(t, m.trySharedLock("bar"))
	require.Len(t, m.entries, 2)
	m.exclusiveUnlock("foo")
	require.Len(t, m.entries, 1)
	require.True(t, m.tryExclusiveLock("foo"))
	require.Len(t, m.entries, 2)
	m.exclusiveUnlock("foo")
	require.Len(t, m.entries, 1)
	m.exclusiveUnlock("bar")
	require.Empty(t, m.entries)
}

func TestMutexMap_SharedLock(t *testing.T) {
	var m mutexMap

	require.Empty(t, m.entries)
	m.sharedLock("foo")
	require.Len(t, m.entries, 1)
	m.sharedLock("foo")
	require.Len(t, m.entries, 1)
	require.True(t, m.trySharedLock("foo"))
	require.Len(t, m.entries, 1)

	// exclusive lock can't be acquired while shared lock is held
	require.False(t, m.tryExclusiveLock("foo"))
	m.sharedUnlock("foo")
	require.False(t, m.tryExclusiveLock("foo"))
	m.sharedUnlock("foo")
	require.False(t, m.tryExclusiveLock("foo"))
	m.sharedUnlock("foo")

	// now exclusive lock can be acquired
	require.True(t, m.tryExclusiveLock("foo"))
}
