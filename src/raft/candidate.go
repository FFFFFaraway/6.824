package raft

import "context"

func (rf *Raft) becomeCandidate() {
clean:
	for {
		select {
		// stop being follower phase
		case <-rf.phase.Follower:
		case <-rf.voteFor:
		default:
			break clean
		}
	}
	// leader vote for self
	go func() { rf.voteFor <- rf.me }()
	go func() { rf.phase.Candidate <- void{} }()

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

	ctx, cancel := context.WithTimeout(context.Background(), RequestVoteTotalTimeout)
	defer cancel()

	term := <-rf.term
	go func() { rf.term <- term }()

	go func() { cnt <- 0 }()
	for i := range rf.peers {
		if i != rf.me {
			go func(ctx context.Context, i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, &RequestVoteArgs{Term: term, CandidateId: rf.me}, reply)

				term := <-rf.term
				if reply.Term > term {
					go func() { rf.term <- reply.Term }()
					go func() { rf.phase.Exit <- void{} }()
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
			}(ctx, i)
		}
	}
	// periodically check if cnt satisfy the need
	go func(ctx context.Context) {
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
	}(ctx)

	select {
	case <-suc:
		Debug(dElection, rf.me, "election success")
		rf.becomeLeader()
	case <-ctx.Done():
		Debug(dElection, rf.me, "election fail, timeout")
		// election fail, continue being follower
		rf.becomeFollower()
	case <-rf.phase.Exit:
		Debug(dElection, rf.me, "election fail, exit")
		rf.becomeFollower()
	}
}
