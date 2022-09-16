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
	HeartBeatTimeout           = 100 * time.Millisecond
	ElectionTimeoutStart       = 300 * time.Millisecond
	ElectionTimeoutRandomRange = 300 // time.Millisecond
	ApplierSleepTimeout        = 10 * time.Millisecond
	ApplierSelectWait          = time.Millisecond
	CommitIndexUpdateTimout    = 10 * time.Millisecond
	WaitAllDie                 = 100 * time.Second
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      chan void           // set by Kill()

	// the order of channel definition is the order of acquirement, to avoid deadlock
	electionTimer chan void
	term          chan int
	voteFor       chan int
	logCh         chan void
	log           []*Entry
	applyCh       chan ApplyMsg
	leaderCtx     chan chan void
	candidateCtx  chan chan void
	followerCtx   chan chan void
	tickerCtx     chan chan void
	commitIndex   chan int
	lastApplied   chan int
	matchIndex    []int
	matchIndexCh  chan void
	nextIndex     []int
	nextIndexCh   chan void
	// 2D, protected by logCh
	snapshotLastIndex int
	snapshotLastTerm  int
	snapshot          []byte
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
	select {
	case <-rf.dead:
		return -1, false
	default:
	}

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
	select {
	case <-rf.dead:
		return -1, -1, false
	default:
	}

	term, isLeader := rf.GetState()
	index := -1

	if !isLeader {
		return index, term, isLeader
	}

	Debug(dLog, rf.me, "Append Log with Term %v", term)
	// append entry to log
	<-rf.logCh
	start := rf.snapshotLastIndex
	rf.log = append(rf.log, &Entry{
		Term:    term,
		Command: command,
	})
	index = len(rf.log) + start
	go func() { rf.logCh <- void{} }()

	vf := <-rf.voteFor
	go func() { rf.voteFor <- vf }()
	rf.persist(term, vf)

	leaderCtx := <-rf.leaderCtx
	go func() { rf.leaderCtx <- leaderCtx }()
	go rf.sendAllHB(leaderCtx)

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
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              make(chan void),
		electionTimer:     make(chan void),
		term:              make(chan int),
		voteFor:           make(chan int),
		log:               make([]*Entry, 0),
		logCh:             make(chan void),
		applyCh:           applyCh,
		leaderCtx:         make(chan chan void),
		candidateCtx:      make(chan chan void),
		followerCtx:       make(chan chan void),
		tickerCtx:         make(chan chan void),
		commitIndex:       make(chan int),
		lastApplied:       make(chan int),
		matchIndex:        make([]int, len(peers)),
		matchIndexCh:      make(chan void),
		nextIndex:         make([]int, len(peers)),
		nextIndexCh:       make(chan void),
		snapshotLastIndex: 0,
		snapshotLastTerm:  0,
		snapshot:          nil,
	}

	go func() { rf.term <- 0 }()
	go func() { rf.logCh <- void{} }()
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
	go func() {
		c := make(chan void)
		ensureClosed(c)
		rf.tickerCtx <- c
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	<-rf.logCh
	go func() { rf.commitIndex <- rf.snapshotLastIndex }()
	go func() { rf.lastApplied <- rf.snapshotLastIndex }()
	go func() { rf.logCh <- void{} }()

	go rf.applier()
	term := <-rf.term
	rf.becomeFollower(term, nil, false)
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
	compactLogTerm[0] = TermCnt{Term: rf.snapshotLastTerm, Cnt: rf.snapshotLastIndex}
	if len(rf.log) > 0 {
		compactLogTerm = append(compactLogTerm, TermCnt{Term: rf.log[0].Term, Cnt: 1})
	}
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == rf.log[i-1].Term {
			compactLogTerm[len(compactLogTerm)-1].Cnt += 1
		} else {
			compactLogTerm = append(compactLogTerm, TermCnt{Term: rf.log[i].Term, Cnt: 1})
		}
	}
	e.Encode(term)
	e.Encode(voteFor)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	go func() { rf.logCh <- void{} }()
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
	d.Decode(&rf.log)
	d.Decode(&term)
	d.Decode(&voteFor)
	d.Decode(&rf.snapshotLastIndex)
	d.Decode(&rf.snapshotLastTerm)
	go func() { rf.term <- term }()
	go func() { rf.voteFor <- voteFor }()
	go func() { rf.logCh <- void{} }()
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	Debug(dSnap, rf.me, "Snapshot before index: %v", index)
	<-rf.logCh
	oldStart := rf.snapshotLastIndex
	lastEntry := rf.log[index-1-oldStart]
	lastTerm := lastEntry.Term

	// start at index + 1 position
	rf.log = rf.log[index+1-1-oldStart:]
	rf.snapshotLastIndex = index
	rf.snapshotLastTerm = lastTerm
	rf.snapshot = snapshot
	go func() { rf.logCh <- void{} }()
	rf.persistSnapshot(snapshot)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// more recent info since it communicate the snapshot on applyCh.
//
// faraway: since this snapshot is not generate by this server.
// So lastIncludedTerm and lastIncludedIndex is needed.
// It is called when service reply the snapshot applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	Debug(dServer, rf.me, "CondInstallSnapshot before index: %v", lastIncludedIndex)
	<-rf.logCh
	oldStart := rf.snapshotLastIndex
	if lastIncludedIndex <= rf.snapshotLastIndex {
		Debug(dSnap, rf.me, "CondInstallSnapshot discard %v, still use %v", lastIncludedIndex, rf.snapshotLastIndex)
		go func() { rf.logCh <- void{} }()
		return false
	}
	rf.snapshotLastTerm = lastIncludedTerm
	rf.snapshotLastIndex = lastIncludedIndex
	rf.snapshot = snapshot
	// reset log
	if lastIncludedIndex+1-1-oldStart < len(rf.log) {
		rf.log = rf.log[lastIncludedIndex+1-1-oldStart:]
	} else {
		rf.log = make([]*Entry, 0)
	}

	lastApplied := <-rf.lastApplied
	Debug(dSnap, rf.me, "CondInstallSnapshot lastApplied %v -> %v", lastApplied, lastIncludedIndex)

	go func() { rf.logCh <- void{} }()
	go func() { rf.lastApplied <- lastIncludedIndex }()
	rf.persistSnapshot(snapshot)
	return true
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without snapshot
		return
	}
	<-rf.logCh
	rf.snapshot = data
	go func() { rf.logCh <- void{} }()
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	term := <-rf.term
	voteFor := <-rf.voteFor
	<-rf.logCh

	e.Encode(rf.log)
	e.Encode(term)
	e.Encode(voteFor)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)

	go func() { rf.term <- term }()
	go func() { rf.voteFor <- voteFor }()
	go func() { rf.logCh <- void{} }()

	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}
