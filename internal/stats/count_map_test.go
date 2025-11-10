package stats

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentCountMap_Get_MissingKey(t *testing.T) {
	var m CountersMap[string]

	v, found := m.Get("missing")

	require.False(t, found)
	require.EqualValues(t, 0, v, "expected 0 for missing key")
}

func TestConcurrentCountMap_IncrementAndGet_NewAndExistingKey(t *testing.T) {
	var m CountersMap[string]

	found := m.Increment("foo")
	require.False(t, found, "Increment on new key should return false")

	got, found := m.Get("foo")
	require.True(t, found)
	require.EqualValues(t, 1, got, "expected 1 after first increment")

	found = m.Increment("foo")
	require.True(t, found, "Increment on existing key should return true")

	got, found = m.Get("foo")
	require.True(t, found)
	require.EqualValues(t, 2, got, "expected 2 after second increment")
}

func TestConcurrentCountMap_Add(t *testing.T) {
	var m CountersMap[int]

	found := m.add(42, 5)
	require.False(t, found, "Add on new key should return false to indicate 'key not previously found'")

	v, found := m.Get(42)

	require.EqualValues(t, 5, v, "expected 5 after add")
	require.True(t, found)

	found = m.add(42, 3)
	require.True(t, found, "Add on existing key should return true")

	v, found = m.Get(42)

	require.EqualValues(t, 8, v, "expected 8 after second add")
	require.True(t, found)
}

func TestConcurrentCountMap_Range(t *testing.T) {
	var m CountersMap[string]

	expectedMap := map[string]uint32{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	for k, v := range expectedMap {
		require.False(t, m.add(k, v))
	}

	m.Range(func(key string, gotCount uint32) bool {
		expectedCount, found := expectedMap[key]

		require.True(t, found)
		require.Equal(t, expectedCount, gotCount)

		return true
	})
}

func TestConcurrentCountMap_Length(t *testing.T) {
	var m CountersMap[string]

	require.False(t, m.add("a", 1))
	require.False(t, m.add("b", 2))

	require.EqualValues(t, 2, m.Length())
	require.True(t, m.add("b", 2))
	require.EqualValues(t, 2, m.Length())

	require.False(t, m.add("c", 3))
	require.EqualValues(t, 3, m.Length())
}

func TestConcurrentCountMap_Range_StopEarly(t *testing.T) {
	var m CountersMap[string]

	require.False(t, m.add("a", 1))
	require.False(t, m.add("b", 2))
	require.False(t, m.add("c", 3))

	count := 0

	m.Range(func(key string, v uint32) bool {
		count++

		return false // stop after first
	})

	require.Equal(t, 1, count, "Range should stop after one iteration")
}

func TestConcurrentCountMap_CountMap_Snapshot(t *testing.T) {
	var m CountersMap[string]

	require.False(t, m.add("x", 10))
	require.False(t, m.add("y", 20))

	expected := map[string]uint32{
		"x": 10,
		"y": 20,
	}

	require.Equal(t, expected, m.CountMap())
}

func TestConcurrentCountMap_ConcurrentIncrement(t *testing.T) {
	const (
		key         = 7
		concurrency = 8
		inc         = 1000
	)

	var m CountersMap[int]

	var wg sync.WaitGroup

	wg.Add(concurrency)

	for range concurrency {
		go func() {
			for range inc {
				m.Increment(key)
			}

			wg.Done()
		}()
	}

	wg.Wait()

	got, found := m.Get(key)

	require.True(t, found)
	require.EqualValues(t, concurrency*inc, got)
}
