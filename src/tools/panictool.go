package tools

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
)

type Panic struct {
	R     interface{} // recover() return value
	Stack []byte      // The call stack at the time
}

func (p Panic) String() string {
	return fmt.Sprintf("%v\n%s", p.R, p.Stack)
}

type PanicGroup struct {
	panics chan Panic // Goroutine panic notification channel
	dones  chan int   // Goroutine completion notification channel
	jobN   int32      // Number of concurrent goroutines
}

func NewPanicGroup() *PanicGroup {
	return &PanicGroup{
		panics: make(chan Panic, 8),
		dones:  make(chan int, 8),
	}
}
func (g *PanicGroup) Go(f func()) *PanicGroup {
	atomic.AddInt32(&g.jobN, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				g.panics <- Panic{R: r, Stack: debug.Stack()}
				return
			}
			g.dones <- 1
		}()
		f()
	}()

	return g // Allows method chaining
}
func (g *PanicGroup) Wait(ctx context.Context) error {
	for {
		select {
		case <-g.dones:
			if atomic.AddInt32(&g.jobN, -1) == 0 {
				return nil
			}
		case p := <-g.panics:
			panic(p)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
