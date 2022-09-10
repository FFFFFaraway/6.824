package raft

import (
	"math/rand"
	"time"
)

// make ensure multiple call will create only one follower
// it should be called with term locked
func (rf *Raft) becomeFollower(term int, newTerm *int, needPersist bool) {
	vf := <-rf.voteFor
	go func() { rf.voteFor <- vf }()

	if newTerm != nil && *newTerm > term {
		term = *newTerm
	}

	if needPersist {
		rf.persist(term, vf)
	}

	leaderCtx := <-rf.leaderCtx
	ensureClosed(leaderCtx)
	go func() { rf.leaderCtx <- leaderCtx }()
	candidateCtx := <-rf.candidateCtx
	ensureClosed(candidateCtx)
	go func() { rf.candidateCtx <- candidateCtx }()

	// must change the phase before release the term
	go func() { rf.term <- term }()

	// ensure the followerCtx is alive
	oldDone := <-rf.followerCtx
	select {
	case <-oldDone:
		Debug(dPhase, rf.me, "become Follower %v", term)
	default:
		go func() { rf.followerCtx <- oldDone }()
		Debug(dDrop, rf.me, "Already Follower")
		return
	}
	done := make(chan void)
	go func() { rf.followerCtx <- done }()

	// ensure the tickerCtx is alive
	oldTickerDone := <-rf.tickerCtx
	select {
	case <-oldTickerDone:
		tickerDone := make(chan void)
		go func() { rf.tickerCtx <- tickerDone }()
		go rf.ticker(tickerDone)
	default:
		go func() { rf.tickerCtx <- oldTickerDone }()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(done chan void) {
	for {
		select {
		case <-rf.dead:
			return
		case <-done:
			return
		case <-timeoutCh(ElectionTimeoutStart + time.Duration(rand.Intn(ElectionTimeoutRandomRange))*time.Millisecond):
			go rf.becomeCandidate()
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
