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
		cc := <-rf.leaderCtx
		go func() { rf.leaderCtx <- cc }()
		go rf.becomeFollower(cc)

		Debug(dTerm, rf.me, "<- RV from S%v, newer term:%v", args.CandidateId, args.Term)
		go func() { rf.term <- args.Term }()

		<-rf.logCh
		log := rf.log
		lastLogIndex := len(log)
		go func() { rf.logCh <- void{} }()

		vote := lastLogIndex == 0
		vote = vote || args.LastLogTerm > rf.log[lastLogIndex-1].Term
		vote = vote || (args.LastLogTerm == rf.log[lastLogIndex-1].Term && args.LastLogIndex >= len(log))

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

	select {
	case <-rf.phase.Follower:
		go func() { rf.phase.Follower <- void{} }()
		// use electionTimer instead of Exit, because it won't wait follower cycle
		go func() { rf.electionTimer <- void{} }()
	default:
	}

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
		cc := <-rf.leaderCtx
		go func() { rf.leaderCtx <- cc }()
		go rf.becomeFollower(cc)

		Debug(dTerm, rf.me, "<- AE, newer term:%v", args.Term)
		go func() { rf.term <- args.Term }()

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
	// this will not exit the leader phase
	go rf.becomeFollower(nil)

	vf := <-rf.voteFor
	go func() { rf.voteFor <- args.LeaderID }()
	Debug(dTerm, rf.me, "<- AE, votefor %v -> %v, same term", vf, args.LeaderID)

	// Logs
	<-rf.logCh
	rf.log = args.Logs
	go func() { rf.logCh <- void{} }()
	Debug(dTerm, rf.me, "<- AE, copy logs, same term")

	commitIndex := <-rf.commitIndex
	if args.CommitIndex > commitIndex {
		Debug(dApply, rf.me, "<- AE, update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- args.CommitIndex }()
	} else {
		Debug(dApply, rf.me, "<- AE, refuse update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- commitIndex }()
	}
}
