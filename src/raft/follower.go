package raft

import (
	"math/rand"
	"time"
)

// make ensure multiple call will create only one follower
func (rf *Raft) becomeFollower(newTerm *int, needPersist bool) {
	term := <-rf.term
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
	done := <-rf.followerCtx
	select {
	case <-done:
		go func() { rf.followerCtx <- make(chan void) }()
		Debug(dPhase, rf.me, "become Follower %v", term)
	default:
		go func() { rf.followerCtx <- done }()
		Debug(dDrop, rf.me, "Already Follower")
		return
	}

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
