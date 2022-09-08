package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) becomeCandidate() {
	term := <-rf.term
	vf := <-rf.voteFor
	go func() { rf.voteFor <- vf }()
	term += 1
	rf.persist(term, vf)

	leaderCtx := <-rf.leaderCtx
	go func() {
		ensureClosed(leaderCtx)
		rf.leaderCtx <- leaderCtx
	}()
	followerCtx := <-rf.followerCtx
	go func() {
		ensureClosed(followerCtx)
		rf.followerCtx <- followerCtx
	}()

	// must change the phase before release the term
	go func() { rf.term <- term }()

	// ensure the candidateCtx is alive
	done := <-rf.candidateCtx
	select {
	case <-done:
		// leader vote for self
		<-rf.voteFor
		go func() { rf.voteFor <- rf.me }()
		Debug(dPhase, rf.me, "become Candidate %v", term)
	default:
		Debug(dPhase, rf.me, "Already Candidate, start newer term election")
		ensureClosed(done)
	}
	go func() { rf.candidateCtx <- make(chan void) }()
	rf.sendAllRV()
}

func (rf *Raft) sendAllRV() {
	// counter for received vote
	cnt := make(chan int)
	// add self
	need := len(rf.peers) / 2
	// whether the received vote satisfy need
	suc := make(chan void)

	span := rand.Intn(ElectionTimeoutRandomRange)
	timeout := timeoutCh(ElectionTimeoutStart + time.Duration(span)*time.Millisecond)

	// when sending RV, the term of candidate
	oldTerm := <-rf.term
	go func() { rf.term <- oldTerm }()

	// calculate the lastLogIndex and lastLogTerm for every sending goroutine
	<-rf.logCh
	lastLogIndex := len(rf.log) + rf.snapshotLastIndex
	lastLogTerm := rf.snapshotLastTerm
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	go func() { rf.logCh <- void{} }()

	// can multiple goroutine use global one candidateCtx?
	// close won't change the channel itself, but make(chan) will.
	// so before make a new one, ensure last one is closed.
	done := <-rf.candidateCtx
	go func() { rf.candidateCtx <- done }()

	sendOneRV := func(i int) {
		// before sending, check whether candidateCtx is done
		select {
		case <-done:
			return
		default:
		}

		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(i, &RequestVoteArgs{
			Term:         oldTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}, reply)

		// term when receiving reply
		term := <-rf.term
		go func() { rf.term <- term }()
		if reply.Term > term {
			Debug(dVote, rf.me, "RV reply, newer term:%v", reply.Term)
			rf.becomeFollower(&reply.Term, true)
			return
		}

		// after received reply, check whether candidateCtx is done
		// although the variable is the same done, but its content may differ
		// The only operation to channel `done` is close(done),
		// and it won't change the channel itself, so there also isn't a race problem
		select {
		case <-done:
			return
		default:
		}

		if ok && reply.Vote {
			Debug(dElection, rf.me, "RV reply Vote from <- %v", i)
			cnt <- <-cnt + 1
		}
	}

	go func() { cnt <- 0 }()

	for i := range rf.peers {
		if i != rf.me {
			go sendOneRV(i)
		}
	}

	// wait reply and check whether vote count satisfy the need
	go func() {
		for {
			select {
			case c := <-cnt:
				if c >= need {
					suc <- void{}
					return
				}
				// block here wait for RV ack
				cnt <- c
			case <-done:
				return
			}
		}
	}()

	select {
	case <-suc:
		Debug(dElection, rf.me, "election success")
		rf.becomeLeader()
	case <-done:
		Debug(dElection, rf.me, "election fail, canceled")
		rf.becomeFollower(nil, false)
	case <-timeout:
		Debug(dElection, rf.me, "election fail, timeout")
		rf.becomeCandidate()
	}
}
