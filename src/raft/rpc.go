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
	Term int
	// copy log
	Success bool
	// conflict position, follower log term
	XTerm int
	// pre XTerm last log entry index
	XIndex int
	// follower log len or last index
	XLen int
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := <-rf.term
	go func() { rf.term <- term }()

	reply.Term = term
	if args.Term < term {
		return
	}

	// grant vote restriction
	<-rf.logCh
	log := rf.log
	lastLogIndex := len(log)
	go func() { rf.logCh <- void{} }()

	vote := lastLogIndex == 0
	vote = vote || args.LastLogTerm > log[lastLogIndex-1].Term
	vote = vote || (args.LastLogTerm == log[lastLogIndex-1].Term && args.LastLogIndex >= lastLogIndex)

	vf := <-rf.voteFor
	// if same term, check not voted for someone else before
	if args.Term == term && vf != -1 && vf != args.CandidateId {
		vote = false
	}

	if !vote {
		Debug(dVote, rf.me, "Refuse Vote -> S%v", args.CandidateId)
		go func() { rf.voteFor <- vf }()
	} else {
		Debug(dVote, rf.me, "Grant Vote %v -> S%v", vf, args.CandidateId)
		go func() { rf.voteFor <- args.CandidateId }()
		reply.Vote = true
	}

	if args.Term > term {
		Debug(dTerm, rf.me, "<- RV from S%v, newer term:%v", args.CandidateId, args.Term)
		rf.becomeFollower(&args.Term, true)
	} else {
		Debug(dTerm, rf.me, "<- RV from S%v, same term:%v", args.CandidateId, args.Term)
		go func() { rf.electionTimer <- void{} }()
		// if voted, then need to persist
		if vote {
			rf.persist(term, args.CandidateId)
		}
	}
}

func (rf *Raft) AE(args *AEArgs, reply *AEReply) {
	term := <-rf.term
	go func() { rf.term <- term }()
	reply.Term = term
	if args.Term < term {
		return
	}

	if args.Term > term {
		Debug(dTerm, rf.me, "<- AE, newer term:%v", args.Term)
	} else {
		Debug(dTerm, rf.me, "<- AE, same term: %v", args.Term)
	}

	<-rf.voteFor
	go func() { rf.voteFor <- args.LeaderID }()

	// Logs
	<-rf.logCh
	if len(rf.log) >= args.PrevLogIndex {
		if args.PrevLogIndex == 0 {
			rf.log = args.Logs
			reply.Success = true
		} else if rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
			rf.log = append(rf.log[:args.PrevLogIndex], args.Logs...)
			reply.Success = true
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex-1].Term
			reply.XIndex = 0
			for i := args.PrevLogIndex; i >= 1; i-- {
				if rf.log[i-1].Term != reply.XTerm {
					reply.XIndex = i
					break
				}
			}
		}
	} else {
		reply.XTerm = -1
		reply.XLen = len(rf.log)
	}
	go func() { rf.logCh <- void{} }()

	// persist after log have been copied
	rf.becomeFollower(&args.Term, true)

	commitIndex := <-rf.commitIndex
	if reply.Success && args.CommitIndex > commitIndex {
		Debug(dApply, rf.me, "<- AE, update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- args.CommitIndex }()
	} else {
		Debug(dDrop, rf.me, "<- AE, refuse update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- commitIndex }()
	}
}
