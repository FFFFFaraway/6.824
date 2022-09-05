package raft

import (
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

type Entry struct {
	Term    int
	Command interface{}
}

const (
	RequestVoteTotalTimeout    = 100 * time.Millisecond
	HeartBeatTimeout           = 100 * time.Millisecond
	ElectionTimeoutStart       = 300 * time.Millisecond
	ElectionTimeoutRandomRange = 200 // time.Millisecond
	ApplierSleepTimeout        = 100 * time.Millisecond
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state

	// peers read only, no mutex need
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	// me read only, no mutex need
	me   int       // this peer's index into peers[]
	dead chan void // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimer  chan void
	heartbeatTimer chan void
	term           chan int
	voteFor        chan int

	// don't know whether there is a better way to operate logs
	logCh        chan void
	log          []*Entry
	applyCh      chan ApplyMsg
	leaderCtx    chan chan void
	candidateCtx chan chan void
	followerCtx  chan chan void
	commitIndex  chan int
	lastApplied  chan int
	matchIndex   []int
	matchIndexCh chan void
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
	select {
	case <-rf.dead:
	default:
		close(rf.dead)
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := <-rf.term
	go func() { rf.term <- term }()
	isLeader := false

	done := <-rf.leaderCtx
	select {
	case <-done:
	default:
		isLeader = true
	}
	go func() { rf.leaderCtx <- done }()

	return term, isLeader
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
	Debug(dDrop, rf.me, "Start a command")
	index := -1
	isLeader := false

	done := <-rf.leaderCtx
	select {
	case <-done:
	default:
		isLeader = true
	}
	go func() { rf.leaderCtx <- done }()

	term := <-rf.term
	go func() { rf.term <- term }()

	if !isLeader {
		return index, term, isLeader
	}

	Debug(dDrop, rf.me, "Append Log with Term")
	// append entry to log
	<-rf.logCh
	rf.log = append(rf.log, &Entry{
		Term:    term,
		Command: command,
	})
	index = len(rf.log)
	go func() { rf.logCh <- void{} }()
	// send to followers by AE
	go rf.startAgreement()

	return index, term, isLeader
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
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           make(chan void),
		electionTimer:  make(chan void),
		heartbeatTimer: make(chan void),
		term:           make(chan int),
		voteFor:        make(chan int),
		log:            make([]*Entry, 0),
		logCh:          make(chan void),
		applyCh:        applyCh,
		leaderCtx:      make(chan chan void),
		candidateCtx:   make(chan chan void),
		followerCtx:    make(chan chan void),
		commitIndex:    make(chan int),
		lastApplied:    make(chan int),
		matchIndex:     make([]int, len(peers)),
		matchIndexCh:   make(chan void),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go func() { rf.term <- 0 }()
	go func() { rf.logCh <- void{} }()
	go func() { rf.commitIndex <- 0 }()
	go func() { rf.lastApplied <- 0 }()
	go func() { rf.voteFor <- -1 }()
	go func() { rf.matchIndexCh <- void{} }()
	go func() {
		c := make(chan void)
		ensureClosed(c)
		rf.leaderCtx <- c
	}()
	go func() {
		c := make(chan void)
		ensureClosed(c)
		rf.candidateCtx <- c
	}()
	go func() {
		c := make(chan void)
		ensureClosed(c)
		rf.followerCtx <- c
	}()
	go rf.applier()
	rf.becomeFollower()
	go rf.cleaner()

	return rf
}
