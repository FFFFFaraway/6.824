package raft

import (
	"6.824/labgob"
	"bytes"
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
	ElectionTimeoutRandomRange = 300 // time.Millisecond
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
	electionTimer chan void
	term          chan int
	voteFor       chan int

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
	nextIndex    []int
	nextIndexCh  chan void
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
	isLeader := false

	done := <-rf.leaderCtx
	select {
	case <-done:
	default:
		isLeader = true
	}

	go func() { rf.term <- term }()
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
	term, isLeader := rf.GetState()
	index := -1

	if !isLeader {
		return index, term, isLeader
	}

	Debug(dLog, rf.me, "Append Log with Term %v", term)
	// append entry to log
	<-rf.logCh
	rf.log = append(rf.log, &Entry{
		Term:    term,
		Command: command,
	})
	index = len(rf.log)
	go func() { rf.logCh <- void{} }()

	vf := <-rf.voteFor
	go func() { rf.voteFor <- vf }()
	rf.persist(term, vf)

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
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          make(chan void),
		electionTimer: make(chan void),
		term:          make(chan int),
		voteFor:       make(chan int),
		log:           make([]*Entry, 0),
		logCh:         make(chan void),
		applyCh:       applyCh,
		leaderCtx:     make(chan chan void),
		candidateCtx:  make(chan chan void),
		followerCtx:   make(chan chan void),
		commitIndex:   make(chan int),
		lastApplied:   make(chan int),
		matchIndex:    make([]int, len(peers)),
		matchIndexCh:  make(chan void),
		nextIndex:     make([]int, len(peers)),
		nextIndexCh:   make(chan void),
	}

	go func() { rf.term <- 0 }()
	go func() { rf.logCh <- void{} }()
	go func() { rf.commitIndex <- 0 }()
	go func() { rf.lastApplied <- 0 }()
	go func() { rf.voteFor <- -1 }()
	go func() { rf.matchIndexCh <- void{} }()
	go func() { rf.nextIndexCh <- void{} }()
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()
	rf.becomeFollower(nil, false)
	go rf.cleaner()

	return rf
}

type TermCnt struct {
	Term int
	Cnt  int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(term, voteFor int) {
	<-rf.logCh

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.log)
	compactLogTerm := make([]TermCnt, 1)
	if len(rf.log) > 0 {
		compactLogTerm[0] = TermCnt{Term: rf.log[0].Term, Cnt: 1}
	}
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == rf.log[i-1].Term {
			compactLogTerm[len(compactLogTerm)-1].Cnt += 1
		} else {
			compactLogTerm = append(compactLogTerm, TermCnt{Term: rf.log[i].Term, Cnt: 1})
		}
	}
	go func() { rf.logCh <- void{} }()
	e.Encode(term)
	e.Encode(voteFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, rf.me, "logTerm: %v, term: %v", compactLogTerm, term)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	<-rf.term
	<-rf.voteFor
	<-rf.logCh

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	if d.Decode(&rf.log) != nil || d.Decode(&term) != nil || d.Decode(&voteFor) != nil {
		panic("readPersist error")
	}
	Debug(dPersist, rf.me, "len(log): %v, term: %v, voteFor: %v", len(rf.log), term, voteFor)

	go func() { rf.term <- term }()
	go func() { rf.voteFor <- voteFor }()
	go func() { rf.logCh <- void{} }()
}
