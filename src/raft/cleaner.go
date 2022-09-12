package raft

func min(a, b int) int {
	if a < b {
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
			// ##########################################################
			case rf.electionTimer <- void{}:
			case rf.term <- -1:
			case rf.voteFor <- -1:
			case rf.logCh <- void{}:
			case rf.commitIndex <- -1:
			case rf.lastApplied <- -1:
			case rf.matchIndexCh <- void{}:
			case rf.nextIndexCh <- void{}:
			case rf.leaderCtx <- closedCh:
			case rf.candidateCtx <- closedCh:
			case rf.followerCtx <- closedCh:
			case rf.tickerCtx <- closedCh:
			case <-timeoutCh(WaitAllDie):
				return
			}
		}
	}
}
