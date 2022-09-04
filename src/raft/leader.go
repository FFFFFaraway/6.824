package raft

import (
	"time"
)

func (rf *Raft) startAgreement() {
	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dLeader, rf.me, "start agreement, term %v", term)
	<-rf.logCh
	log := rf.log
	go func() { rf.logCh <- void{} }()
	commitIndex := <-rf.commitIndex
	go func() { rf.commitIndex <- commitIndex }()

	cnt := make(chan int)
	need := len(rf.peers) / 2
	suc := make(chan void)

	go func() { cnt <- 0 }()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term, Logs: log, CommitIndex: commitIndex, LeaderID: rf.me}, reply)

				// must use the latest term
				term := <-rf.term
				if ok && reply.Term > term {
					Debug(dTerm, rf.me, "command reply, newer term:%v", reply.Term)
					go func() { rf.term <- reply.Term }()
					go rf.becomeFollower(true)
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

				if ok {
					<-rf.matchIndexCh
					rf.matchIndex[i] = len(log)
					go func() { rf.matchIndexCh <- void{} }()
					cnt <- <-cnt + 1
				}

			}(i)
		}
	}

	// periodically check if cnt satisfy the need
	go func() {
		for {
			done := <-rf.leaderCtx
			go func() { rf.leaderCtx <- done }()

			select {
			case c := <-cnt:
				if c >= need {
					suc <- void{}
					return
				}
				// block here wait for RV ack
				cnt <- c
			case <-done:
				return
			}
		}
	}()

	// press the heartbeatTimer
	go func() { rf.heartbeatTimer <- void{} }()

	done := <-rf.leaderCtx
	go func() { rf.leaderCtx <- done }()

	// wait util exit, no timeout, no retry
	select {
	case <-suc:
		Debug(dElection, rf.me, "agreement success")
	case <-done:
		Debug(dElection, rf.me, "agreement fail, exit")
	}
}

func (rf *Raft) sendHB() {
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
			go func(i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term, Logs: log, CommitIndex: commitIndex, LeaderID: rf.me}, reply)

				term := <-rf.term
				if ok && reply.Term > term {
					Debug(dTerm, rf.me, "HB reply, newer term:%v", reply.Term)
					go func() { rf.term <- reply.Term }()
					go rf.becomeFollower(true)
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

				if ok {
					<-rf.matchIndexCh
					rf.matchIndex[i] = len(log)
					go func() { rf.matchIndexCh <- void{} }()
				}
			}(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	<-rf.phase.Candidate
	// reopen
	<-rf.leaderCtx
	go func() { rf.leaderCtx <- make(chan void) }()
	go func() { rf.phase.Leader <- void{} }()

	go rf.HB()
	go rf.updateCommitIndex()

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) HB() {
	go rf.sendHB()
	for {
		timeout := timeoutCh(HeartBeatTimeout)

		done := <-rf.leaderCtx
		go func() { rf.leaderCtx <- done }()

		select {
		case <-done:
			return
		case <-timeout:
			select {
			// it's possible that both are ready
			case <-done:
				return
			default:
				<-rf.phase.Leader
				go rf.sendHB()
				go func() { rf.phase.Leader <- void{} }()
			}
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
			<-rf.phase.Leader
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
			go func() { rf.phase.Leader <- void{} }()
		}
		time.Sleep(ApplierSleepTimeout)
	}
}
