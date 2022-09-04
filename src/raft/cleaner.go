package raft

func ensureClosed(ch chan void) {
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
		for {
			select {
			case <-rf.heartbeatTimer:
				Debug(dClean, rf.me, "rf.heartbeatTimer")
			case <-rf.electionTimer:
				Debug(dClean, rf.me, "rf.electionTimer")
			case <-rf.phase.Leader:
				Debug(dClean, rf.me, "rf.phase.Leader")
			case <-rf.phase.Candidate:
				Debug(dClean, rf.me, "rf.phase.Candidate")
			case <-rf.phase.Follower:
				Debug(dClean, rf.me, "rf.phase.Follower")
			case t := <-rf.term:
				Debug(dClean, rf.me, "rf.term %v", t)
			case vf := <-rf.voteFor:
				Debug(dClean, rf.me, "rf.voteFor %v", vf)
			case <-rf.logCh:
				Debug(dClean, rf.me, "rf.logCh")
			case c := <-rf.commitIndex:
				Debug(dClean, rf.me, "rf.commitIndex %v", c)
			case l := <-rf.lastApplied:
				Debug(dClean, rf.me, "rf.lastApplied %v", l)
			case c := <-rf.leaderCtx:
				Debug(dClean, rf.me, "rf.lastApplied %v", c)
				ensureClosed(c)
			default:
				return
			}
		}
	}
}
