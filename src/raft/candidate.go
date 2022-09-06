package raft

func (rf *Raft) becomeCandidate() {
	term := <-rf.term
	term += 1

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
		go func() { rf.candidateCtx <- make(chan void) }()
		// leader vote for self
		<-rf.voteFor
		go func() { rf.voteFor <- rf.me }()
		Debug(dPhase, rf.me, "become Candidate %v", term)
		rf.sendAllRV()
	default:
		go func() { rf.candidateCtx <- done }()
		Debug(dError, rf.me, "Already Candidate!!!")
	}
}

func (rf *Raft) sendAllRV() {
	// counter for received vote
	cnt := make(chan int)
	// add self
	need := len(rf.peers) / 2
	// whether the received vote satisfy need
	suc := make(chan void)

	timeout := timeoutCh(RequestVoteTotalTimeout)

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
				before := <-rf.candidateCtx
				go func() { rf.candidateCtx <- before }()
				select {
				case <-before:
					return
				default:
				}

				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}, reply)

				term := <-rf.term
				go func() { rf.term <- term }()
				if reply.Term > term {
					Debug(dVote, rf.me, "RV reply, newer term:%v", reply.Term)
					rf.becomeFollower(&reply.Term)
					return
				}

				done := <-rf.candidateCtx
				go func() { rf.candidateCtx <- done }()
				select {
				case <-done:
					return
				default:
				}

				if ok && reply.Vote {
					cnt <- <-cnt + 1
				}
			}(i)
		}
	}
	// periodically check if cnt satisfy the need
	go func() {
		for {
			done := <-rf.candidateCtx
			go func() { rf.candidateCtx <- done }()
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

	done := <-rf.candidateCtx
	go func() { rf.candidateCtx <- done }()
	select {
	case <-suc:
		Debug(dElection, rf.me, "election success")
		// must use add go to call the cancel func quickly
		rf.becomeLeader()
	case <-done:
		Debug(dElection, rf.me, "election fail, canceled")
		rf.becomeFollower(nil)
	case <-timeout:
		Debug(dElection, rf.me, "election fail, timeout")
		rf.becomeFollower(nil)
	}
}
