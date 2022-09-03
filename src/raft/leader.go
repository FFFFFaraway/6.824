package raft

import (
	"context"
)

func (rf *Raft) startAgreement(command interface{}, index int) {
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

	cc := <-rf.leaderCtx
	go func() { rf.leaderCtx <- cc }()

	go func() { cnt <- 0 }()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term, Logs: log, CommitIndex: commitIndex, LeaderID: rf.me}, reply)

				term := <-rf.term
				if ok && reply.Term > term {
					Debug(dTerm, rf.me, "command reply, newer term:%v", reply.Term)
					go func() { rf.term <- reply.Term }()
					cc.cancel()
					go rf.becomeFollower(cc)
					return
				}
				go func() { rf.term <- term }()

				if ok {
					select {
					case <-cc.ctx.Done():
						return
					default:
						cnt <- <-cnt + 1
					}
				}
			}(i)
		}
	}

	// periodically check if cnt satisfy the need
	go func() {
		for {
			select {
			case c := <-cnt:
				if c >= need {
					suc <- void{}
					return
				}
				// block here wait for RV ack
				cnt <- c
			case <-cc.ctx.Done():
				return
			}
		}
	}()

	// press the heartbeatTimer
	go func() { rf.heartbeatTimer <- void{} }()

	// wait util exit, no timeout, no retry
	select {
	case <-suc:
		Debug(dElection, rf.me, "agreement success")
		commitIndex = <-rf.commitIndex
		go func() {
			rf.commitIndex <- commitIndex + 1
			Debug(dApply, rf.me, "commitIndex inc %v -> %v", commitIndex, commitIndex+1)
		}()
	case <-cc.ctx.Done():
		Debug(dElection, rf.me, "agreement fail, exit")
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      command,
			CommandIndex: index,
		}
	}
}

func (rf *Raft) sendHB(cc CtxCancel) {
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
					go rf.becomeFollower(nil)
					cc.cancel()
					return
				}
				go func() { rf.term <- term }()
			}(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	<-rf.phase.Candidate
	go func() { rf.phase.Leader <- void{} }()

	<-rf.leaderCtx
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(context.Background())
	cc := CtxCancel{ctx, cancel}
	go func() { rf.leaderCtx <- &cc }()

	go rf.HB(cc)

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) HB(cc CtxCancel) {
	go rf.sendHB(cc)
	for {
		timeout := timeoutCh(HeartBeatTimeout)

		select {
		case <-cc.ctx.Done():
			return
		case <-timeout:
			select {
			// it's possible that both are ready
			case <-cc.ctx.Done():
				return
			default:
				<-rf.phase.Leader
				go rf.sendHB(cc)
				go func() { rf.phase.Leader <- void{} }()
			}
		case <-rf.heartbeatTimer:
		}
	}
}
