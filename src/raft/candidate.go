package raft

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

	timeout := timeoutCh(RequestVoteTotalTimeout)
	done := make(chan void)
	defer ensureClosed(done)

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
					go func() { rf.term <- reply.Term }()
					ensureClosed(done)
					go rf.becomeFollower(false)
					return
				}
				go func() { rf.term <- term }()

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
		select {
		case <-done:
			Debug(dElection, rf.me, "election fail, canceled")
			go rf.becomeFollower(false)
		case <-timeout:
			Debug(dElection, rf.me, "election fail, timeout")
			go rf.becomeFollower(false)
		default:
			Debug(dElection, rf.me, "election success")
			// must use add go to call the cancel func quickly
			go rf.becomeLeader()
		}
	case <-done:
		Debug(dElection, rf.me, "election fail, canceled")
		go rf.becomeFollower(false)
	case <-timeout:
		Debug(dElection, rf.me, "election fail, timeout")
		go rf.becomeFollower(false)
	}
}
