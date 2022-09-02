package raft

import (
	"time"
)

func (rf *Raft) cleaner() {
	for {
		if rf.killed() {
			time.Sleep(KilledCheckTimeout)
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
				case <-rf.phase.Exit:
					Debug(dClean, rf.me, "rf.phase.Exit")
				case t := <-rf.term:
					Debug(dClean, rf.me, "rf.term %v", t)
				case vf := <-rf.voteFor:
					Debug(dClean, rf.me, "rf.voteFor %v", vf)
				case <-rf.logCh:
					Debug(dClean, rf.me, "rf.logCh")
				case <-rf.leaderCtx:
					Debug(dClean, rf.me, "rf.leaderCtx")
				case c := <-rf.commitIndex:
					Debug(dClean, rf.me, "rf.commitIndex %v", c)
				case l := <-rf.lastApplied:
					Debug(dClean, rf.me, "rf.lastApplied %v", l)
				default:
					return
				}
			}
		}
		time.Sleep(KilledCheckTimeout)
	}
}
