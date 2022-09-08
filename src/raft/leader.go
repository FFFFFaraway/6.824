package raft

import (
	"time"
)

// must acquire logCh and nextIndexCh before call it
func (rf *Raft) prepare(i, term, commitIndex int) *AEArgs {
	// recompute the AEArgs
	start := rf.snapshotLastIndex
	startIndex := rf.nextIndex[i]
	prevLogTerm := rf.snapshotLastTerm
	// if startIndex == start + 1, then preTerm == snapshotLastIndex
	if startIndex != start+1 {
		// prev: -1, index to log index: -1 again, exclude snapshot: -start
		prevLogTerm = rf.log[startIndex-1-1-start].Term
	}
	sendLogs := rf.log[startIndex-1-start:]
	return &AEArgs{
		Term:         term,
		Logs:         sendLogs,
		CommitIndex:  commitIndex,
		LeaderID:     rf.me,
		PrevLogIndex: startIndex - 1,
		PrevLogTerm:  prevLogTerm,
	}
}

func (rf *Raft) sendAllHB(done chan void) {
	oldTerm := <-rf.term
	go func() { rf.term <- oldTerm }()
	Debug(dLeader, rf.me, "send out HB, term %v", oldTerm)

	// ###### prepare information for child goroutine to send ######
	// so that child goroutines don't have to acquire the resources
	preparation := make([]*AEArgs, len(rf.peers))

	<-rf.logCh
	oldCommitIndex := <-rf.commitIndex
	<-rf.nextIndexCh

	oldStart := rf.snapshotLastIndex
	oldLog := rf.log
	for i := range rf.peers {
		if i != rf.me {
			preparation[i] = rf.prepare(i, oldTerm, oldCommitIndex)
		}
	}

	// release order doesn't matter
	go func() { rf.logCh <- void{} }()
	go func() { rf.nextIndexCh <- void{} }()
	go func() { rf.commitIndex <- oldCommitIndex }()
	// ###### preparation done ######

	var sendOneHB func(i int)
	sendOneHB = func(i int) {
		select {
		case <-done:
			return
		default:
		}

		reply := &AEReply{}
		ok := rf.sendAE(i, preparation[i], reply)

		// must use the latest term
		term := <-rf.term
		go func() { rf.term <- term }()
		if ok && reply.Term > term {
			Debug(dTerm, rf.me, "command reply, newer term:%v", reply.Term)
			rf.becomeFollower(&reply.Term, true)
			return
		}

		select {
		case <-done:
			return
		default:
		}

		if ok {
			if reply.Success {
				<-rf.logCh
				<-rf.matchIndexCh
				<-rf.nextIndexCh

				rf.matchIndex[i] = len(oldLog) + oldStart
				rf.nextIndex[i] = len(rf.log) + 1 + rf.snapshotLastIndex

				go func() { rf.logCh <- void{} }()
				go func() { rf.matchIndexCh <- void{} }()
				go func() { rf.nextIndexCh <- void{} }()
			} else {
				Debug(dTerm, rf.me, "HB reply with fail, XTerm: %v, XIndex: %v, XLen: %v", reply.XTerm, reply.XIndex, reply.XLen)
				<-rf.logCh
				commitIndex := <-rf.commitIndex
				<-rf.nextIndexCh

				// compute rf.nextIndex by reply
				if reply.XTerm == -1 {
					rf.nextIndex[i] = reply.XLen + 1
				} else {
					tailIndex := -1
					for index := rf.nextIndex[i] - 1; index >= 1 && rf.log[index-1-rf.snapshotLastIndex].Term > reply.XTerm; index-- {
						if rf.log[index-1-rf.snapshotLastIndex].Term == reply.XTerm {
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

				// use the rf.nextIndex to recompute the AEArgs
				preparation[i] = rf.prepare(i, term, commitIndex)

				go func() { rf.nextIndexCh <- void{} }()
				go func() { rf.logCh <- void{} }()
				go func() { rf.commitIndex <- commitIndex }()
				sendOneHB(i)
			}
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go sendOneHB(i)
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
	oldDone := <-rf.leaderCtx
	select {
	case <-oldDone:
		go func() { rf.leaderCtx <- make(chan void) }()
	default:
		go func() { rf.leaderCtx <- oldDone }()
		Debug(dError, rf.me, "Already Leader!!!")
		return
	}

	<-rf.matchIndexCh
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	go func() { rf.matchIndexCh <- void{} }()

	<-rf.logCh
	lastLogIndex := len(rf.log) + rf.snapshotLastIndex
	go func() { rf.logCh <- void{} }()

	<-rf.nextIndexCh
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	go func() { rf.nextIndexCh <- void{} }()

	// don't use the oldDone, which belongs to last term
	done := <-rf.leaderCtx
	go func() { rf.leaderCtx <- done }()

	go rf.HeartBeatLoop(done)
	go rf.updateCommitIndex(done)

	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) HeartBeatLoop(done chan void) {
	go rf.sendAllHB(done)
	for {
		timeout := timeoutCh(HeartBeatTimeout)
		select {
		case <-done:
			return
		case <-timeout:
			go rf.sendAllHB(done)
		}
	}
}

func (rf *Raft) updateCommitIndex(done chan void) {
	for {
		select {
		case <-done:
			return
		default:
			term := <-rf.term
			<-rf.logCh
			commitIndex := <-rf.commitIndex
			<-rf.matchIndexCh

			var max int
			for max = len(rf.log) + rf.snapshotLastIndex; max >= commitIndex+1; max-- {
				cnt := 0
				for _, c := range rf.matchIndex {
					if c >= max {
						cnt++
					}
				}
				if cnt >= len(rf.peers)/2 && rf.log[max-1-rf.snapshotLastIndex].Term == term {
					break
				}
			}
			Debug(dCommit, rf.me, "update commitIndex %v -> %v", commitIndex, max)
			go func() { rf.term <- term }()
			go func() { rf.matchIndexCh <- void{} }()
			go func() { rf.logCh <- void{} }()
			go func() { rf.commitIndex <- max }()
		}
		time.Sleep(CommitIndexUpdateTimout)
	}
}
