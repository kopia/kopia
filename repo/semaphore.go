package repo

type empty struct{}
type semaphore chan empty

func (s semaphore) Lock() {
	s <- empty{}
}

func (s semaphore) Unlock() {
	<-s
}
