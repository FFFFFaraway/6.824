package raft

import (
	"time"
)

func (rf *Raft) sendOneHB(oldLog []*Entry, i, oldTerm, commitIndex int) {

	<-rf.nextIndexCh
	startIndex := rf.nextIndex[i]
	go func() { rf.nextIndexCh <- void{} }()

	prevLogTerm := 0
	if startIndex != 1 {
		// prev: -1, index to log index: -1 again
		prevLogTerm = oldLog[startIndex-2].Term
	}
	reply := &AEReply{}
	ok := rf.sendAE(i, &AEArgs{
		Term:         oldTerm,
		Logs:         oldLog[startIndex-1:],
		CommitIndex:  commitIndex,
		LeaderID:     rf.me,
		PrevLogIndex: startIndex - 1,
		PrevLogTerm:  prevLogTerm,
	}, reply)

	// must use the latest term
	term := <-rf.term
	if ok && reply.Term > term {
		Debug(dTerm, rf.me, "command reply, newer term:%v", reply.Term)
		go func() { rf.term <- reply.Term }()
		rf.becomeFollower()
		return
	}
	go func() { rf.term <- term }()

	done := <-rf.leaderCtx
	go func() { rf.leaderCtx <- done }()

	select {
	case <-done:
		return
	default:
	}

	<-rf.logCh
	log := rf.log
	go func() { rf.logCh <- void{} }()

	if ok {
		if reply.Success {
			<-rf.matchIndexCh
			rf.matchIndex[i] = len(oldLog)
			go func() { rf.matchIndexCh <- void{} }()
			<-rf.nextIndexCh
			rf.nextIndex[i] = len(log) + 1
			go func() { rf.nextIndexCh <- void{} }()
		} else {
			<-rf.nextIndexCh
			rf.nextIndex[i] -= 1
			go func() { rf.nextIndexCh <- void{} }()
			rf.sendOneHB(oldLog, i, oldTerm, commitIndex)
		}
	}
}

func (rf *Raft) sendAllHB() {
	done := <-rf.leaderCtx
	go func() { rf.leaderCtx <- done }()

	select {
	case <-done:
		return
	default:
	}

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dLeader, rf.me, "send out HB, term %v", term)

	<-rf.logCh
	log := rf.log
	go func() { rf.logCh <- void{} }()
	commitIndex := <-rf.commitIndex
	go func() { rf.commitIndex <- commitIndex }()

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendOneHB(log, i, term, commitIndex)
		}
	}
}

func (rf *Raft) becomeLeader() {
	followerCtx := <-rf.followerCtx
	go func() {
		ensureClosed(followerCtx)
		rf.followerCtx <- followerCtx
	}()
	candidateCtx := <-rf.candidateCtx
	go func() {
		ensureClosed(candidateCtx)
		rf.candidateCtx <- candidateCtx
	}()

	// reopen the leaderCtx
	done := <-rf.leaderCtx
	select {
	case <-done:
		go func() { rf.leaderCtx <- make(chan void) }()
	default:
		go func() { rf.leaderCtx <- done }()
		Debug(dError, rf.me, "Already Leader!!!")
		return
	}

	<-rf.matchIndexCh
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	go func() { rf.matchIndexCh <- void{} }()

	<-rf.logCh
	lastLogIndex := len(rf.log)
	go func() { rf.logCh <- void{} }()

	<-rf.nextIndexCh
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	go func() { rf.nextIndexCh <- void{} }()

	go rf.HeartBeatLoop()
	go rf.updateCommitIndex()

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) HeartBeatLoop() {
	go rf.sendAllHB()
	for {
		timeout := timeoutCh(HeartBeatTimeout)

		done := <-rf.leaderCtx
		go func() { rf.leaderCtx <- done }()

		select {
		case <-done:
			return
		case <-timeout:
			go rf.sendAllHB()
		case <-rf.heartbeatTimer:
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for {
		done := <-rf.leaderCtx
		go func() { rf.leaderCtx <- done }()

		select {
		case <-done:
			return
		default:
			commitIndex := <-rf.commitIndex
			<-rf.logCh
			<-rf.matchIndexCh
			term := <-rf.term

			var max int
			for max = len(rf.log); max >= commitIndex+1; max-- {
				cnt := 0
				for _, c := range rf.matchIndex {
					if c >= max {
						cnt++
					}
				}
				if cnt >= len(rf.peers)/2 && rf.log[max-1].Term == term {
					break
				}
			}
			Debug(dCommit, rf.me, "update commitIndex %v -> %v", commitIndex, max)
			go func() { rf.term <- term }()
			go func() { rf.matchIndexCh <- void{} }()
			go func() { rf.logCh <- void{} }()
			go func() { rf.commitIndex <- max }()
		}
		time.Sleep(ApplierSleepTimeout)
	}
}
