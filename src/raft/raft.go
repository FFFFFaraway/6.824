package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"math/rand"

	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type void struct{}

type Phase struct {
	Leader    chan void
	Candidate chan void
	Follower  chan void
	// exit leader or candidate phase, become follower
	Exit chan void
}

const (
	RequestVoteTotalTimeout    = 100 * time.Millisecond
	HeartBeatTimeout           = 100 * time.Millisecond
	ElectionTimeoutStart       = 1000 * time.Millisecond
	ElectionTimeoutRandomRange = 1000 // time.Millisecond
	FollowerSleepTimeout       = 100 * time.Millisecond
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state

	// peers read only no mutex need
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	// me read only no mutex need
	me int // this peer's index into peers[]
	// atomic
	dead int32 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	reset   chan void
	phase   Phase
	term    chan int
	voteFor chan int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := <-rf.term
	go func() { rf.term <- term }()
	isLeader := false

	select {
	case <-rf.phase.Leader:
		go func() { rf.phase.Leader <- void{} }()
		isLeader = true
	default:
	}

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
	Term int
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
		go func() { rf.reset <- void{} }()
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

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AE(args *AEArgs, reply *AEReply) {
	term := <-rf.term
	reply.Term = term
	if args.Term > term {
		Debug(dTerm, rf.me, "<- AE, newer term:%v", args.Term)
		go func() { rf.term <- args.Term }()
		go func() { rf.phase.Exit <- void{} }()
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
		go func() { rf.reset <- void{} }()
	}
}

func (rf *Raft) sendAE(server int, args *AEArgs, reply *AEReply) bool {
	ok := rf.peers[server].Call("Raft.AE", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := <-rf.term
	go func() { rf.term <- term }()
	isLeader := false

	select {
	case <-rf.phase.Leader:
		go func() { rf.phase.Leader <- void{} }()
		isLeader = true
	default:
	}

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func timeoutCh(t time.Duration) (done chan void) {
	done = make(chan void)
	go func() {
		time.Sleep(t)
		done <- void{}
	}()
	return
}

func (rf *Raft) sendHB(term int) {
	Debug(dLeader, rf.me, "send out HB, term %v", term)
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term}, reply)
				term := <-rf.term
				if ok && reply.Term > term {
					go func() { rf.term <- reply.Term }()
					go func() { rf.phase.Exit <- void{} }()
					return
				}
				go func() { rf.term <- term }()
			}(i)
		}
	}

}

func (rf *Raft) becomeLeader() {
	// stop candidate
	<-rf.phase.Candidate

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)

	// voteFor unchanged

	rf.phase.Leader <- void{}
}

func (rf *Raft) Leader() {
main:
	for {
		<-rf.phase.Leader
		if rf.killed() {
			return
		}

		select {
		case <-rf.phase.Exit:
			rf.becomeFollower()
			continue main
		default:
			// if someone take the rf.leader, then it won't execute the leader code
			go func() { rf.phase.Leader <- void{} }()
		}

		term := <-rf.term
		go func() { rf.term <- term }()
		go rf.sendHB(term)

		time.Sleep(HeartBeatTimeout)
	}
}

func (rf *Raft) sendAllRV() {
	cnt := make(chan int)
	ctx, cancel := context.WithTimeout(context.Background(), RequestVoteTotalTimeout)
	defer cancel()
	suc := make(chan void)
	term := <-rf.term
	go func() { rf.term <- term }()
	// add self
	need := len(rf.peers) / 2
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

func (rf *Raft) becomeCandidate() {
	// stop follower phase, handle rpc as phase of candidate
	<-rf.phase.Follower
	go func() { rf.phase.Candidate <- void{} }()

	term := <-rf.term
	go func(term int) {
		Debug(dTerm, rf.me, "inc %v -> %v", term, term+1)
		rf.term <- term + 1
	}(term)
	Debug(dPhase, rf.me, "become Candidate %v", term+1)
clean:
	for {
		select {
		case <-rf.voteFor:
		default:
			break clean
		}
	}
	// leader vote for self
	go func() { rf.voteFor <- rf.me }()

	rf.sendAllRV()
}

func (rf *Raft) becomeFollower() {
	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Follower %v", term)
clean:
	for {
		select {
		case <-rf.voteFor:
		case <-rf.phase.Candidate:
		case <-rf.phase.Leader:
		default:
			break clean
		}
	}
	go func() { rf.voteFor <- -1 }()
	rf.phase.Follower <- void{}
	go rf.ticker()
}

func (rf *Raft) Follower() {
	for {
		<-rf.phase.Follower
		if rf.killed() {
			return
		}

		go func() { rf.phase.Follower <- void{} }()
		select {
		case <-rf.phase.Exit:
			go func() { rf.reset <- void{} }()
		default:
		}
		time.Sleep(FollowerSleepTimeout)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		span := rand.Intn(ElectionTimeoutRandomRange)
		timeout := timeoutCh(ElectionTimeoutStart + time.Duration(span)*time.Millisecond)
		select {
		case <-timeout:
			rf.becomeCandidate()
			return
		// suppress the reset button by AE or RV or heartbeat
		case <-rf.reset:
		}
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		reset:     make(chan void),
		phase: Phase{
			Leader:    make(chan void),
			Candidate: make(chan void),
			Follower:  make(chan void),
			Exit:      make(chan void),
		},
		term:    make(chan int),
		voteFor: make(chan int),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Follower()
	go rf.Leader()
	go func() { rf.term <- 0 }()
	rf.becomeFollower()

	return rf
}
