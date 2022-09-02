package raft

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
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
		go func() { rf.phase.Exit <- void{} }()
		reply.Vote = true
		Debug(dVote, rf.me, "Grant Vote -> S%v", args.CandidateId)
		<-rf.voteFor
		go func() { rf.voteFor <- args.CandidateId }()
		return
	}

	go func() { rf.term <- term }()

	if args.Term < term {
		return
	}

	select {
	case <-rf.phase.Follower:
		go func() { rf.phase.Follower <- void{} }()
		// use electionTimer instead of Exit, because it won't wait follower cycle
		go func() { rf.electionTimer <- void{} }()
	default:
	}

	vf := <-rf.voteFor
	// voted for someone else
	if vf != -1 {
		go func() { rf.voteFor <- vf }()
		return
	}

	Debug(dVote, rf.me, "Grant Vote -> S%v, Same Term", args.CandidateId)
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
		go func() { rf.phase.Exit <- void{} }()

		// Logs
		if len(args.Logs) > 0 {
			<-rf.logCh
			rf.log = args.Logs
			go func() { rf.logCh <- void{} }()
		}

		commitIndex := <-rf.commitIndex
		if args.CommitIndex > commitIndex {
			go func() { rf.commitIndex <- args.CommitIndex }()
		} else {
			go func() { rf.commitIndex <- commitIndex }()
		}
		return
	}

	go func() { rf.term <- term }()

	if args.Term < term {
		return
	}

	select {
	case <-rf.phase.Candidate:
		Debug(dTerm, rf.me, "<- AE, same term: %v", args.Term)
		go func() { rf.phase.Candidate <- void{} }()
		go func() { rf.phase.Exit <- void{} }()
	// leader with same term is not possible to receive an AE
	case <-rf.phase.Follower:
		Debug(dTerm, rf.me, "<- AE, same term: %v", args.Term)
		go func() { rf.phase.Follower <- void{} }()
		go func() { rf.electionTimer <- void{} }()
	}

	// Logs
	if len(args.Logs) > 0 {
		<-rf.logCh
		rf.log = args.Logs
		go func() { rf.logCh <- void{} }()
	}

	commitIndex := <-rf.commitIndex
	if args.CommitIndex > commitIndex {
		go func() { rf.commitIndex <- args.CommitIndex }()
	} else {
		go func() { rf.commitIndex <- commitIndex }()
	}
}
