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

type SnapshotArgs struct {
	Term          int
	LeaderID      int
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type SnapshotReply struct {
	Term int
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

	// grant vote restriction
	<-rf.logCh
	var vote bool
	if len(rf.log) == 0 {
		vote = rf.snapshotLastIndex == 0
		vote = vote || args.LastLogTerm > rf.snapshotLastTerm
		vote = vote || (args.LastLogTerm == rf.snapshotLastTerm && args.LastLogIndex >= rf.snapshotLastIndex)
	} else {
		vote = args.LastLogTerm > rf.log[len(rf.log)-1].Term
		vote = vote || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)+rf.snapshotLastIndex)
	}
	go func() { rf.logCh <- void{} }()

	vf := <-rf.voteFor
	// if same term, check not voted for someone else before
	if args.Term == term && vf != -1 && vf != args.CandidateId {
		Debug(dVote, rf.me, "Refuse Vote -> S%v, votedFor: %v", args.CandidateId, vf)
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

	// need to be close to the rf.becomeFollower
	// If a server receive 2 RV request at the same time, and both have newer term.
	// We expect that one RV call rf.becomeFollower, and the other one use the changed term.
	// still can't fix the problem! If there is a RV already waiting the term.
	// go func() { rf.term <- term }()

	if args.Term > term {
		Debug(dTerm, rf.me, "<- RV from S%v, newer term:%v", args.CandidateId, args.Term)
		rf.becomeFollower(term, &args.Term, true)
	} else {
		go func() { rf.term <- term }()
		Debug(dTerm, rf.me, "<- RV from S%v, same term:%v", args.CandidateId, args.Term)
		// if voted, then need to persist
		if vote {
			// must reset electionTimer under vote condition
			go func() { rf.electionTimer <- void{} }()
			rf.persist(term, args.CandidateId)
		}
	}
}

func (rf *Raft) AE(args *AEArgs, reply *AEReply) {
	term := <-rf.term
	reply.Term = term
	if args.Term < term {
		go func() { rf.term <- term }()
		return
	}

	go func() { rf.electionTimer <- void{} }()

	if args.Term > term {
		Debug(dTerm, rf.me, "<- AE from %v, newer term:%v", args.LeaderID, args.Term)
	} else {
		Debug(dTerm, rf.me, "<- AE from %v, same term: %v", args.LeaderID, args.Term)
	}

	<-rf.voteFor
	go func() { rf.voteFor <- args.LeaderID }()

	// Logs
	<-rf.logCh
	reply.XLen = len(rf.log) + rf.snapshotLastIndex

	if reply.XLen >= args.PrevLogIndex {
		if args.PrevLogIndex <= rf.snapshotLastIndex {
			// the prevLog is the snapshot, then copy the logs
			if args.PrevLogTerm == rf.snapshotLastTerm {
				rf.log = args.Logs
				reply.Success = true
			} else {
				Debug(dError, rf.me, "Snapshot Error, mismatch!")
				reply.XTerm = rf.snapshotLastTerm
				reply.XIndex = 0
			}
		} else if rf.log[args.PrevLogIndex-1-rf.snapshotLastIndex].Term == args.PrevLogTerm {
			rf.log = append(rf.log[:args.PrevLogIndex-rf.snapshotLastIndex], args.Logs...)
			reply.Success = true
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex-1-rf.snapshotLastIndex].Term
			reply.XIndex = rf.snapshotLastIndex
			for i := args.PrevLogIndex; i-rf.snapshotLastIndex >= 1; i-- {
				if rf.log[i-1-rf.snapshotLastIndex].Term != reply.XTerm {
					reply.XIndex = i
					break
				}
			}
		}
	} else {
		// the PrevLogIndex position has no log yet
		reply.XTerm = -1
	}
	go func() { rf.logCh <- void{} }()

	if !reply.Success {
		Debug(dApply, rf.me, "AE fail, XTerm: %v, XIndex: %v, XLen: %v", reply.XTerm, reply.XIndex, reply.XLen)
	}

	// persist after log have been copied
	rf.becomeFollower(term, &args.Term, len(args.Logs) > 0)

	commitIndex := <-rf.commitIndex
	if reply.Success && args.CommitIndex > commitIndex {
		Debug(dApply, rf.me, "<- AE, update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		// must use the minimum
		if reply.XLen < args.CommitIndex {
			go func() { rf.commitIndex <- reply.XLen }()
		} else {
			go func() { rf.commitIndex <- args.CommitIndex }()
		}
	} else {
		Debug(dDrop, rf.me, "<- AE, refuse update commitIndex: %v -> %v", commitIndex, args.CommitIndex)
		go func() { rf.commitIndex <- commitIndex }()
	}
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	term := <-rf.term
	go func() { rf.term <- term }()
	reply.Term = term
	if args.Term < term {
		return
	}

	go func() { rf.electionTimer <- void{} }()

	if args.Term > term {
		Debug(dTerm, rf.me, "<- Snapshot, newer term:%v", args.Term)
	} else {
		Debug(dTerm, rf.me, "<- Snapshot, same term: %v", args.Term)
	}

	<-rf.voteFor
	go func() { rf.voteFor <- args.LeaderID }()

	<-rf.logCh
	snapshotLastIndex := rf.snapshotLastIndex
	go func() { rf.logCh <- void{} }()
	// although leader sends snapshot, but it's possible that follower's snapshot is newer
	if args.SnapshotIndex > snapshotLastIndex {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.SnapshotTerm,
			SnapshotIndex: args.SnapshotIndex,
		}
	}
}

func isAlive(done chan void) bool {
	select {
	case <-done:
		return false
	default:
	}
	return true
}

// if before sending or after sending, the context is done, then return false
// although the variable is the same done, but its content may differ
// The only operation to channel `done` is close(done),
// and it won't change the channel itself, so there also isn't a race problem
func (rf *Raft) sendRequestVote(done chan void, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return isAlive(done) && rf.peers[server].Call("Raft.RequestVote", args, reply) && isAlive(done)
}

func (rf *Raft) sendAE(done chan void, server int, args *AEArgs, reply *AEReply) bool {
	return isAlive(done) && rf.peers[server].Call("Raft.AE", args, reply) && isAlive(done)
}

func (rf *Raft) sendSnapshot(done chan void, server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	return isAlive(done) && rf.peers[server].Call("Raft.InstallSnapshot", args, reply) && isAlive(done)
}
