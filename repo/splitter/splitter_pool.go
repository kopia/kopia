package splitter

import (
	"sync"
)

type recyclableSplitter struct {
	Splitter
	pool *sync.Pool
}

func (s recyclableSplitter) Close() {
	s.Splitter.Reset()
	s.Splitter.Close()
	s.pool.Put(s.Splitter)
}

// pooled returns a factory that recycles the splitters on Close().
func pooled(f Factory) Factory {
	pool := &sync.Pool{}

	return func() Splitter {
		s := pool.Get()
		if s == nil {
			return recyclableSplitter{f(), pool}
		}

		return recyclableSplitter{s.(Splitter), pool} //nolint:forcetypeassert
	}
}
