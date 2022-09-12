package raft

import "time"

// must acquire logCh and nextIndexCh before call it
func (rf *Raft) prepare(done chan void, i, term, commitIndex int) *AEArgs {
	// recompute the AEArgs
	start := rf.snapshotLastIndex
	startIndex := rf.nextIndex[i]
	snapshotLastTerm := rf.snapshotLastTerm
	snapshot := rf.snapshot

	// sendOneSnapshot is out of lock, so can't use rf things like rf.snapshot
	sendOneSnapshot := func() {
		reply := &SnapshotReply{}
		ok := rf.sendSnapshot(done, i, &SnapshotArgs{
			Term:          term,
			LeaderID:      rf.me,
			Snapshot:      snapshot,
			SnapshotTerm:  snapshotLastTerm,
			SnapshotIndex: start,
		}, reply)

		if ok {
			Debug(dSnap, rf.me, "Snapshot reply from <- %v", i)
		}
	}

	var prevLogTerm int
	// if startIndex == start + 1, then preTerm == snapshotLastIndex
	if startIndex == start+1 {
		prevLogTerm = rf.snapshotLastTerm
	} else if startIndex > start+1 {
		// prev: -1, index to log index: -1 again, exclude snapshot: -start
		prevLogTerm = rf.log[startIndex-1-1-start].Term
	} else {
		// need to installSnapshot
		go sendOneSnapshot()
		startIndex = start + 1
		prevLogTerm = rf.snapshotLastTerm
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
			preparation[i] = rf.prepare(done, i, oldTerm, oldCommitIndex)
		}
	}

	// release order doesn't matter
	go func() { rf.logCh <- void{} }()
	go func() { rf.nextIndexCh <- void{} }()
	go func() { rf.commitIndex <- oldCommitIndex }()
	// ###### preparation done ######

	var sendOneHB func(i int)
	sendOneHB = func(i int) {
		reply := &AEReply{}
		ok := rf.sendAE(done, i, preparation[i], reply)
		if ok {
			// must use the latest term
			term := <-rf.term

			if reply.Term > term {
				Debug(dTerm, rf.me, "command reply, newer term:%v", reply.Term)
				rf.becomeFollower(term, &reply.Term, true)
				return
			}
			go func() { rf.term <- term }()

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
					for index := rf.nextIndex[i] - 1; index-rf.snapshotLastIndex >= 1 && rf.log[index-1-rf.snapshotLastIndex].Term > reply.XTerm; index-- {
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
				preparation[i] = rf.prepare(done, i, term, commitIndex)

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
	ensureClosed(followerCtx)
	go func() { rf.followerCtx <- followerCtx }()
	candidateCtx := <-rf.candidateCtx
	ensureClosed(candidateCtx)
	go func() { rf.candidateCtx <- candidateCtx }()
	// only leader can stop the ticker
	tickerCtx := <-rf.tickerCtx
	ensureClosed(tickerCtx)
	go func() { rf.tickerCtx <- tickerCtx }()

	// must change the phase before release the term
	go func() { rf.term <- term }()

	oldDone := <-rf.leaderCtx
	select {
	case <-oldDone:
		Debug(dPhase, rf.me, "become Leader %v", term)
	default:
		go func() { rf.leaderCtx <- oldDone }()
		Debug(dError, rf.me, "Already Leader!!!")
		return
	}
	// don't use the oldDone, which belongs to last term
	// reopen the leaderCtx
	done := make(chan void)
	go func() { rf.leaderCtx <- done }()

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

	go rf.HeartBeatLoop(done)
	go rf.updateCommitIndex(done)
}

func (rf *Raft) HeartBeatLoop(done chan void) {
	go rf.sendAllHB(done)
	for {
		select {
		case <-done:
			return
		case <-timeoutCh(HeartBeatTimeout):
			go rf.sendAllHB(done)
		}
	}
}

func (rf *Raft) updateCommitIndex(done chan void) {
	for {
		time.Sleep(CommitIndexUpdateTimout)

		term := <-rf.term
		<-rf.logCh
		commitIndex := <-rf.commitIndex
		<-rf.matchIndexCh

		// check killed after resource acquirement
		select {
		case <-done:
			go func() { rf.term <- term }()
			go func() { rf.matchIndexCh <- void{} }()
			go func() { rf.logCh <- void{} }()
			go func() { rf.commitIndex <- commitIndex }()
			return
		default:
		}

		Debug(dCommit, rf.me, "before update commitIndex, rf.matchIndex: %v", rf.matchIndex)

		var max int
		for max = len(rf.log) + rf.snapshotLastIndex; max >= commitIndex+1; max-- {
			cnt := 0
			for _, c := range rf.matchIndex {
				if c >= max {
					cnt++
				}
			}
			if cnt >= len(rf.peers)/2 {
				if max < rf.snapshotLastIndex+1 && rf.snapshotLastTerm == term {
					break
				} else if rf.log[max-1-rf.snapshotLastIndex].Term == term {
					break
				}
			}
		}
		Debug(dCommit, rf.me, "update commitIndex %v -> %v", commitIndex, max)
		go func() { rf.term <- term }()
		go func() { rf.matchIndexCh <- void{} }()
		go func() { rf.logCh <- void{} }()
		go func() { rf.commitIndex <- max }()
	}
}
