package raft

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	Vote bool
}

type AEArgs struct {
	Term        int
	Logs        []*Entry
	CommitIndex int
	LeaderID    int
}

type AEReply struct {
	Term int
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := <-rf.term
	reply.Term = term
	if args.Term > term {
		Debug(dTerm, rf.me, "<- RV from S%v, newer term:%v", args.CandidateId, args.Term)
		go func() { rf.term <- args.Term }()

		rf.becomeFollower()

		<-rf.logCh
		log := rf.log
		lastLogIndex := len(log)
		go func() { rf.logCh <- void{} }()

		vote := lastLogIndex == 0
		vote = vote || args.LastLogTerm > log[lastLogIndex-1].Term
		vote = vote || (args.LastLogTerm == log[lastLogIndex-1].Term && args.LastLogIndex >= lastLogIndex)

		if lastLogIndex > 0 {
			Debug(dDrop, rf.me, "args LastTerm: %v, me LastTerm: %v, vote: %v", args.LastLogTerm, log[lastLogIndex-1].Term, vote)
		}

		if !vote {
			Debug(dVote, rf.me, "Refuse Vote -> S%v", args.CandidateId)
			return
		}
		reply.Vote = true
		vf := <-rf.voteFor
		Debug(dVote, rf.me, "Grant Vote %v -> S%v", vf, args.CandidateId)
		go func() { rf.voteFor <- args.CandidateId }()
		return
	}

	go func() { rf.term <- term }()

	if args.Term < term {
		return
	}

	go func() { rf.electionTimer <- void{} }()

	vf := <-rf.voteFor
	// voted for someone else
	if vf != -1 && vf != args.CandidateId {
		go func() { rf.voteFor <- vf }()
		return
	}

	<-rf.logCh
	log := rf.log
	lastLogIndex := len(log)
	go func() { rf.logCh <- void{} }()

	vote := lastLogIndex == 0
	vote = vote || args.LastLogTerm > rf.log[lastLogIndex-1].Term
	vote = vote || (args.LastLogTerm == rf.log[lastLogIndex-1].Term && args.LastLogIndex >= len(log))

	if !vote {
		go func() { rf.voteFor <- vf }()
		Debug(dVote, rf.me, "Refuse Vote -> S%v", args.CandidateId)
		return
	}

	Debug(dVote, rf.me, "Grant Vote %v -> S%v, Same Term", vf, args.CandidateId)
	go func() { rf.voteFor <- args.CandidateId }()
	reply.Vote = true
	return
}

func (rf *Raft) AE(args *AEArgs, reply *AEReply) {
	term := <-rf.term
	reply.Term = term
	if args.Term > term {
		Debug(dTerm, rf.me, "<- AE, newer term:%v", args.Term)
		go func() { rf.term <- args.Term }()

		rf.becomeFollower()

		<-rf.voteFor
		go func() { rf.voteFor <- args.LeaderID }()

		// Logs
		<-rf.logCh
		rf.log = args.Logs
		go func() { rf.logCh <- void{} }()

		commitIndex := <-rf.commitIndex
		if args.CommitIndex > commitIndex {
			Debug(dApply, rf.me, "<- AE, update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
			go func() { rf.commitIndex <- args.CommitIndex }()
		} else {
			Debug(dApply, rf.me, "<- AE, refuse update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
			go func() { rf.commitIndex <- commitIndex }()
		}
		return
	}

	go func() { rf.term <- term }()

	if args.Term < term {
		return
	}

	Debug(dTerm, rf.me, "<- AE, same term: %v", args.Term)

	rf.becomeFollower()

	<-rf.voteFor
	go func() { rf.voteFor <- args.LeaderID }()

	// Logs
	<-rf.logCh
	rf.log = args.Logs
	go func() { rf.logCh <- void{} }()

	commitIndex := <-rf.commitIndex
	if args.CommitIndex > commitIndex {
		Debug(dApply, rf.me, "<- AE, update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- args.CommitIndex }()
	} else {
		Debug(dApply, rf.me, "<- AE, refuse update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- commitIndex }()
	}
}
