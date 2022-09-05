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
	// copy log
	PrevLogIndex int
	PrevLogTerm  int
}

type AEReply struct {
	Term    int
	Success bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := <-rf.term
	reply.Term = term
	if args.Term < term {
		go func() { rf.term <- term }()
		return
	}
	go func() { rf.term <- args.Term }()

	// grant vote restriction
	<-rf.logCh
	log := rf.log
	lastLogIndex := len(log)
	go func() { rf.logCh <- void{} }()

	vote := lastLogIndex == 0
	vote = vote || args.LastLogTerm > log[lastLogIndex-1].Term
	vote = vote || (args.LastLogTerm == log[lastLogIndex-1].Term && args.LastLogIndex >= lastLogIndex)

	vf := <-rf.voteFor
	if args.Term > term {
		Debug(dTerm, rf.me, "<- RV from S%v, newer term:%v", args.CandidateId, args.Term)
		rf.becomeFollower()
	} else {
		Debug(dTerm, rf.me, "<- RV from S%v, same term:%v", args.CandidateId, args.Term)
		go func() { rf.electionTimer <- void{} }()
		// voted for someone else
		if vf != -1 && vf != args.CandidateId {
			vote = false
		}
	}

	if !vote {
		go func() { rf.voteFor <- vf }()
		Debug(dVote, rf.me, "Refuse Vote -> S%v", args.CandidateId)
		return
	}
	Debug(dVote, rf.me, "Grant Vote %v -> S%v", vf, args.CandidateId)
	go func() { rf.voteFor <- args.CandidateId }()
	reply.Vote = true
}

func (rf *Raft) AE(args *AEArgs, reply *AEReply) {
	term := <-rf.term
	reply.Term = term
	if args.Term < term {
		go func() { rf.term <- term }()
		return
	}
	go func() { rf.term <- args.Term }()

	if args.Term > term {
		Debug(dTerm, rf.me, "<- AE, newer term:%v", args.Term)
	} else {
		Debug(dTerm, rf.me, "<- AE, same term: %v", args.Term)
	}

	rf.becomeFollower()

	<-rf.voteFor
	go func() { rf.voteFor <- args.LeaderID }()

	// Logs
	<-rf.logCh
	if len(rf.log) >= args.PrevLogIndex {
		if args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
			rf.log = append(rf.log[:args.PrevLogIndex], args.Logs...)
			reply.Success = true
		}
	}
	go func() { rf.logCh <- void{} }()

	commitIndex := <-rf.commitIndex
	if reply.Success && args.CommitIndex > commitIndex {
		Debug(dApply, rf.me, "<- AE, update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- args.CommitIndex }()
	} else {
		Debug(dDrop, rf.me, "<- AE, refuse update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- commitIndex }()
	}
}
