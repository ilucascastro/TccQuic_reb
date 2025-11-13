package session

type Semaphore struct {
	channel chan struct{}
}

func NewSemaphore(n int) Semaphore {
	return Semaphore{
		channel: make(chan struct{}, n),
	}
}

func (s Semaphore) Acquire() {
	s.channel <- struct{}{}
}

func (s Semaphore) Release() {
	<-s.channel
}
