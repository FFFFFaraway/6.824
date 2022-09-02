package raft

import (
	"time"
)

func (rf *Raft) cleaner() {
	for {
		if rf.killed() {
			time.Sleep(FollowerSleepTimeout)
			for {
				select {
				case <-rf.reset:
					Debug(dClean, rf.me, "rf.reset")
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
				case <-rf.leaderCtxCh:
					Debug(dClean, rf.me, "rf.leaderCtxCh")
				default:
					return
				}
			}
		}
		time.Sleep(FollowerSleepTimeout)
	}
}
