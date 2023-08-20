package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMutexMap_ExclusiveLock(t *testing.T) {
	m := newMutexMap()

	require.Len(t, m.entries, 0)
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
	require.Len(t, m.entries, 0)
}

func TestMutexMap_SharedLock(t *testing.T) {
	m := newMutexMap()

	require.Len(t, m.entries, 0)
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

func TestMutexMap_Nil(t *testing.T) {
	var m *mutexMap

	// make sure all operations are no-ops on nil map
	m.exclusiveUnlock("foo")
	m.sharedUnlock("bar")
	m.exclusiveLock("foo")
	m.sharedLock("bar")
	require.True(t, m.tryExclusiveLock("foo"))
	require.True(t, m.trySharedLock("bar"))
}
