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

type ChanT interface {
	void | chan void | int
}

func ensureClosed[T ChanT](ch chan T) {
	if ch != nil {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func (rf *Raft) cleaner() {
	select {
	case <-rf.dead:
		Debug(dTerm, rf.me, "killed")
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
