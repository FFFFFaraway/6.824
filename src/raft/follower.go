package raft

import (
	"math/rand"
	"time"
)

// make ensure multiple call will create only one follower
func (rf *Raft) becomeFollower(exitLeader bool) {
	if exitLeader {
		ensureClosed(rf.leaderCtx)
	}

	// ensure Leader and Candidate are stopped
	timeout := timeoutCh(SelectTimeout)
phase:
	for {
		select {
		case <-rf.phase.Leader:
			Debug(dDrop, rf.me, "Leader -> Follower")
			break phase
		case <-rf.phase.Candidate:
			Debug(dDrop, rf.me, "Candidate -> Follower")
			break phase
		case <-rf.phase.Follower:
			go func() { rf.electionTimer <- void{} }()
			go func() { rf.phase.Follower <- void{} }()
			Debug(dDrop, rf.me, "Already Follower")
			return
		case <-timeout:
			Debug(dDrop, rf.me, "No Phase!!!")
			break phase
		}
	}

	go rf.ticker()
	go func() { rf.phase.Follower <- void{} }()

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Follower %v", term)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.dead:
			return
		default:
		}
		span := rand.Intn(ElectionTimeoutRandomRange)
		timeout := timeoutCh(ElectionTimeoutStart + time.Duration(span)*time.Millisecond)
		select {
		case <-timeout:
			rf.becomeCandidate()
			return
		// suppress the electionTimer button by AE or RV or heartbeat
		case <-rf.electionTimer:
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
