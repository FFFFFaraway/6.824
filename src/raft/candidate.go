package raft

func (rf *Raft) becomeCandidate() {
	term := <-rf.term
	vf := <-rf.voteFor
	go func() { rf.voteFor <- vf }()
	term += 1
	rf.persist(term, vf)

	leaderCtx := <-rf.leaderCtx
	ensureClosed(leaderCtx)
	go func() { rf.leaderCtx <- leaderCtx }()
	followerCtx := <-rf.followerCtx
	ensureClosed(followerCtx)
	go func() { rf.followerCtx <- followerCtx }()

	// must change the phase before release the term
	go func() { rf.term <- term }()

	// ensure the candidateCtx is alive
	oldCandidateCtx := <-rf.candidateCtx
	select {
	case <-oldCandidateCtx:
		// leader vote for self
		<-rf.voteFor
		go func() { rf.voteFor <- rf.me }()
		Debug(dPhase, rf.me, "become Candidate %v", term)
	default:
		Debug(dPhase, rf.me, "Already Candidate, start newer term election")
		// exit all goroutine in last term
		ensureClosed(oldCandidateCtx)
	}

	// can multiple goroutine use global one candidateCtx?
	// close won't change the channel itself, but make(chan) will.
	// so before make a new one, ensure last one is closed.
	candidateCtx := make(chan void)
	go func() { rf.candidateCtx <- candidateCtx }()
	rf.sendAllRV(candidateCtx)
}

func (rf *Raft) sendAllRV(done chan void) {
	// counter for received vote
	cnt := make(chan int)
	// add self
	need := len(rf.peers) / 2
	// whether the received vote satisfy need
	suc := make(chan void)

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

	sendOneRV := func(i int) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(done, i, &RequestVoteArgs{
			Term:         oldTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}, reply)

		if ok {
			// term when receiving reply
			term := <-rf.term
			if reply.Term > term {
				Debug(dVote, rf.me, "RV reply, newer term:%v", reply.Term)
				rf.becomeFollower(term, &reply.Term, true)
				return
			}
			go func() { rf.term <- term }()
			if reply.Vote {
				Debug(dElection, rf.me, "RV reply Vote from <- %v", i)
				cnt <- <-cnt + 1
			}
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
		term := <-rf.term
		Debug(dElection, rf.me, "election fail, canceled")
		rf.becomeFollower(term, nil, false)
	}
}
