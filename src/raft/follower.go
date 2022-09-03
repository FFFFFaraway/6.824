package raft

import (
	"math/rand"
	"time"
)

// make ensure multiple call will create only one follower
func (rf *Raft) becomeFollower(cc *CtxCancel) {
	if cc != nil {
		select {
		case <-cc.ctx.Done():
		// if not done, then call cancel
		default:
			cc.cancel()
		}
	}

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
			Debug(dError, rf.me, "No Phase!!!")
			break phase
		}
	}

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

func (rf *Raft) applier() {
	for {
		if rf.killed() {
			return
		}

		commitIndex := <-rf.commitIndex
		go func() { rf.commitIndex <- commitIndex }()

		lastApplied := <-rf.lastApplied
		go func() { rf.lastApplied <- lastApplied }()

		<-rf.logCh
		log := rf.log
		go func() { rf.logCh <- void{} }()

		cnt := 0
		for i := lastApplied + 1; i <= commitIndex; i++ {
			Debug(dApply, rf.me, "apply %v", i)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log[i-1].Command,
				CommandIndex: i,
			}
			cnt += 1
		}
		go func() { rf.lastApplied <- <-rf.lastApplied + cnt }()

		time.Sleep(ApplierSleepTimeout)
	}
}
