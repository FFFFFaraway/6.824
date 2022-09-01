package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) becomeFollower() {
clean:
	for {
		select {
		case <-rf.voteFor:
		case <-rf.phase.Candidate:
		case <-rf.phase.Leader:
		default:
			break clean
		}
	}
	go func() { rf.voteFor <- -1 }()
	go rf.ticker()
	go func() { rf.phase.Follower <- void{} }()

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Follower %v", term)
}

func (rf *Raft) Follower() {
	for {
		<-rf.phase.Follower
		if rf.killed() {
			return
		}
		go func() { rf.phase.Follower <- void{} }()

		select {
		case <-rf.phase.Exit:
			go func() { rf.reset <- void{} }()
		default:
		}
		time.Sleep(FollowerSleepTimeout)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		span := rand.Intn(ElectionTimeoutRandomRange)
		timeout := timeoutCh(ElectionTimeoutStart + time.Duration(span)*time.Millisecond)
		select {
		case <-timeout:
			rf.becomeCandidate()
			return
		// suppress the reset button by AE or RV or heartbeat
		case <-rf.reset:
		}
	}
}

func timeoutCh(t time.Duration) (done chan void) {
	done = make(chan void)
	go func() {
		time.Sleep(t)
		done <- void{}
	}()
	return
}
