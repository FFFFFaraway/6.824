package raft

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

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
	case chan int:
		if ch != nil {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	case chan chan void:
		if ch != nil {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	}
}

func (rf *Raft) cleaner() {
	select {
	case <-rf.dead:
		Debug(dTerm, rf.me, "killed")
		closedCh := make(chan void)
		ensureClosed(closedCh)
		for {
			select {
			case <-rf.electionTimer:
			case <-rf.term:
			case <-rf.voteFor:
			case <-rf.logCh:
			case <-rf.commitIndex:
			case <-rf.lastApplied:
			case <-rf.matchIndexCh:
			case <-rf.nextIndexCh:
			case c := <-rf.leaderCtx:
				ensureClosed(c)
			case c := <-rf.candidateCtx:
				ensureClosed(c)
			case c := <-rf.followerCtx:
				ensureClosed(c)
			case c := <-rf.tickerCtx:
				ensureClosed(c)
			// can't put anything like below! Otherwise, there will be many race problems
			// case rf.logCh <- void{}:
			case <-timeoutCh(WaitAllDie):
				return
			}
		}
	}
}
