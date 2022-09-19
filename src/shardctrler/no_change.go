package shardctrler

import "time"

type void struct{}

func ensureClosed(i interface{}) {
	switch ch := i.(type) {
	case chan void:
		if ch != nil {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	}
}

func timeoutCh(t time.Duration) (done chan void) {
	done = make(chan void)
	go func() {
		time.Sleep(t)
		select {
		case done <- void{}:
		// if no one wait for it, then just abort
		default:
		}
	}()
	return
}
