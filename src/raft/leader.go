package raft

import (
	"time"
)

func (rf *Raft) sendOneHB(i, oldTerm int) {
	commitIndex := <-rf.commitIndex
	go func() { rf.commitIndex <- commitIndex }()

	<-rf.nextIndexCh
	startIndex := rf.nextIndex[i]
	go func() { rf.nextIndexCh <- void{} }()

	<-rf.logCh
	oldStart := <-rf.snapshotLastIndex
	go func() { rf.snapshotLastIndex <- oldStart }()
	lastTerm := <-rf.snapshotLastTerm
	go func() { rf.snapshotLastTerm <- lastTerm }()
	oldLog := rf.log
	prevLogTerm := lastTerm
	// if startIndex == start + 1, then preTerm == snapshotLastIndex
	if startIndex != oldStart+1 {
		// prev: -1, index to log index: -1 again, exclude snapshot: -start
		prevLogTerm = oldLog[startIndex-1-1-oldStart].Term
	}
	sendLogs := oldLog[startIndex-1-oldStart:]
	go func() { rf.logCh <- void{} }()

	// before send check once more
	leaderDone := <-rf.leaderCtx
	go func() { rf.leaderCtx <- leaderDone }()

	select {
	case <-leaderDone:
		return
	default:
	}

	reply := &AEReply{}
	ok := rf.sendAE(i, &AEArgs{
		Term:         oldTerm,
		Logs:         sendLogs,
		CommitIndex:  commitIndex,
		LeaderID:     rf.me,
		PrevLogIndex: startIndex - 1,
		PrevLogTerm:  prevLogTerm,
	}, reply)

	// must use the latest term
	term := <-rf.term
	go func() { rf.term <- term }()
	if ok && reply.Term > term {
		Debug(dTerm, rf.me, "command reply, newer term:%v", reply.Term)
		rf.becomeFollower(&reply.Term, true)
		return
	}

	done := <-rf.leaderCtx
	go func() { rf.leaderCtx <- done }()

	select {
	case <-done:
		return
	default:
	}

	<-rf.logCh
	start := <-rf.snapshotLastIndex
	go func() { rf.snapshotLastIndex <- start }()
	log := rf.log
	go func() { rf.logCh <- void{} }()

	if ok {
		if reply.Success {
			<-rf.matchIndexCh
			rf.matchIndex[i] = len(oldLog) + oldStart
			go func() { rf.matchIndexCh <- void{} }()
			<-rf.nextIndexCh
			rf.nextIndex[i] = len(log) + 1 + start
			go func() { rf.nextIndexCh <- void{} }()
		} else {
			Debug(dTerm, rf.me, "HB reply with fail, XTerm: %v, XIndex: %v, XLen: %v", reply.XTerm, reply.XIndex, reply.XLen)
			<-rf.logCh
			<-rf.nextIndexCh
			if reply.XTerm == -1 {
				rf.nextIndex[i] = reply.XLen + 1
			} else {
				tailIndex := -1
				for index := rf.nextIndex[i] - 1; index >= 1 && rf.log[index-1-start].Term > reply.XTerm; index-- {
					if rf.log[index-1-start].Term == reply.XTerm {
						tailIndex = index
						break
					}
				}
				if tailIndex != -1 {
					rf.nextIndex[i] = tailIndex + 1
				} else {
					rf.nextIndex[i] = reply.XIndex + 1
				}
			}
			go func() { rf.nextIndexCh <- void{} }()
			go func() { rf.logCh <- void{} }()
			rf.sendOneHB(i, oldTerm)
		}
	}
}

func (rf *Raft) sendAllHB() {
	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dLeader, rf.me, "send out HB, term %v", term)

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendOneHB(i, term)
		}
	}
}

func (rf *Raft) becomeLeader() {
	term := <-rf.term

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

	// must change the phase before release the term
	go func() { rf.term <- term }()

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
	start := <-rf.snapshotLastIndex
	go func() { rf.snapshotLastIndex <- start }()
	lastLogIndex := len(rf.log) + start
	go func() { rf.logCh <- void{} }()

	<-rf.nextIndexCh
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	go func() { rf.nextIndexCh <- void{} }()

	go rf.HeartBeatLoop()
	go rf.updateCommitIndex()

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
			term := <-rf.term
			<-rf.logCh
			commitIndex := <-rf.commitIndex
			<-rf.matchIndexCh
			start := <-rf.snapshotLastIndex
			go func() { rf.snapshotLastIndex <- start }()

			var max int
			for max = len(rf.log) + start; max >= commitIndex+1; max-- {
				cnt := 0
				for _, c := range rf.matchIndex {
					if c >= max {
						cnt++
					}
				}
				if cnt >= len(rf.peers)/2 && rf.log[max-1-start].Term == term {
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
