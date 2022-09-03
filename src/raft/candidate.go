package raft

import "context"

func (rf *Raft) becomeCandidate() {
	<-rf.phase.Follower
	go func() { rf.phase.Candidate <- void{} }()
	// leader vote for self
	<-rf.voteFor
	go func() { rf.voteFor <- rf.me }()

	term := <-rf.term
	go func() { rf.term <- term + 1 }()
	Debug(dPhase, rf.me, "become Candidate %v", term+1)

	rf.sendAllRV()
}

func (rf *Raft) sendAllRV() {
	// counter for received vote
	cnt := make(chan int)
	// add self
	need := len(rf.peers) / 2
	// whether the received vote satisfy need
	suc := make(chan void)

	var cancel context.CancelFunc
	ctx, cancel := context.WithTimeout(context.Background(), RequestVoteTotalTimeout)
	defer cancel()

	term := <-rf.term
	go func() { rf.term <- term }()

	<-rf.logCh
	log := rf.log
	lastLogIndex := len(log)
	lastLogTerm := 0
	if lastLogIndex != 0 {
		lastLogTerm = log[lastLogIndex-1].Term
	}
	go func() { rf.logCh <- void{} }()

	go func() { cnt <- 0 }()
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}, reply)

				term := <-rf.term
				if reply.Term > term {
					Debug(dElection, rf.me, "election fail, canceled")
					go func() { rf.term <- reply.Term }()
					cancel()
					go rf.becomeFollower(nil)
					return
				}
				go func() { rf.term <- term }()

				if ok && reply.Vote {
					select {
					case <-ctx.Done():
						return
					default:
						cnt <- <-cnt + 1
					}
				}
			}(i)
		}
	}
	// periodically check if cnt satisfy the need
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
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-suc:
		select {
		// it's possible that the suc channel and the cancel are both ready
		case <-ctx.Done():
			Debug(dElection, rf.me, "election fail, canceled or timeout")
			// election fail, continue being follower
			go rf.becomeFollower(nil)
		default:
			Debug(dElection, rf.me, "election success")
			// must use go to call the cancel func quickly
			go rf.becomeLeader()
		}
	case <-ctx.Done():
		Debug(dElection, rf.me, "election fail, canceled or timeout")
		// election fail, continue being follower
		go rf.becomeFollower(nil)
	}
}
