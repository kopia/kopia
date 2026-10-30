package freepool_test

import (
	"testing"

	"github.com/kopia/kopia/internal/freepool"
)

func TestNewStruct(t *testing.T) {
	// Create a new pool with a clean struct
	cleanItem := struct{ value int }{value: 0}
	pool := freepool.NewStruct(cleanItem)

	// Take an item from the pool and verify that it matches the clean struct
	item := pool.Take()
	if item.value != cleanItem.value {
		t.Errorf("Expected item to be %#v, but got %#v", cleanItem, *item)
	}

	// Modify the item and return it to the pool
	item.value = 1
	pool.Return(item)

	// Take another item from the pool and verify that it matches the clean struct again
	item = pool.Take()
	if item.value != cleanItem.value {
		t.Errorf("Expected item to be %#v, but got %#v", cleanItem, *item)
	}
}

func TestNew(t *testing.T) {
	// Create a new pool with a makeNew function and a clean function
	makeNew := func() *int { v := 0; return &v }
	clean := func(v *int) { *v = 0 }
	pool := freepool.New(makeNew, clean)

	// Take an item from the pool and verify that it's a pointer to an int with value 0
	item := pool.Take()
	if *item != 0 {
		t.Errorf("Expected item to be %d, but got %d", 0, *item)
	}

	// Modify the item and return it to the pool
	*item = 1
	pool.Return(item)

	// Take another item from the pool and verify that it's a pointer to an int with value 0 again
	item = pool.Take()
	if *item != 0 {
		t.Errorf("Expected item to be %d, but got %d", 0, *item)
	}
}

func TestPool_MultipleItems(t *testing.T) {
	// Create a new pool with a makeNew function and a clean function
	makeNew := func() *int { v := 0; return &v }
	clean := func(v *int) { *v = 0 }
	pool := freepool.New(makeNew, clean)

	// Take multiple items from the pool and verify that they're all unique
	item1 := pool.Take()
	item2 := pool.Take()

	if item1 == item2 {
		t.Errorf("Expected items to be unique, but they're the same")
	}

	// Return the items to the pool
	pool.Return(item1)
	pool.Return(item2)

	// Take multiple items from the pool again and verify that they're all unique again
	item1 = pool.Take()
	item2 = pool.Take()

	if item1 == item2 {
		t.Errorf("Expected items to be unique, but they're the same")
	}
}
