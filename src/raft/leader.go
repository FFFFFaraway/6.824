package raft

import (
	"context"
	"time"
)

func (rf *Raft) startAgreement(command interface{}, index int) {
	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dLeader, rf.me, "start agreement, term %v", term)
	<-rf.logCh
	log := rf.log
	go func() { rf.logCh <- void{} }()

	cnt := make(chan int)
	need := len(rf.peers) / 2
	suc := make(chan void)

	go func() { cnt <- 0 }()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term, Logs: log}, reply)

				term := <-rf.term
				if ok && reply.Term > term {
					go func() { rf.term <- reply.Term }()
					go func() { rf.phase.Exit <- void{} }()
					return
				}
				go func() { rf.term <- term }()

				<-rf.leaderCtxCh
				defer func() { go func() { rf.leaderCtxCh <- void{} }() }()
				if ok {
					select {
					case <-rf.leaderCtx.Done():
						return
					default:
						cnt <- <-cnt + 1
					}
				}
			}(i)
		}
	}

	// periodically check if cnt satisfy the need
	// 不过有一说一，这一坨代码真烂
	go func() {
		for {
			<-rf.leaderCtxCh
			select {
			case c := <-cnt:
				if c >= need {
					suc <- void{}
					go func() { rf.leaderCtxCh <- void{} }()
					return
				}
				go func() { rf.leaderCtxCh <- void{} }()
				// block here wait for RV ack
				cnt <- c
			case <-rf.leaderCtx.Done():
				go func() { rf.leaderCtxCh <- void{} }()
				return
			}
		}
	}()

	<-rf.leaderCtxCh
	defer func() { go func() { rf.leaderCtxCh <- void{} }() }()
	// wait util exit, no timeout, no retry
	select {
	case <-suc:
		Debug(dElection, rf.me, "agreement success")
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
	case <-rf.leaderCtx.Done():
		Debug(dElection, rf.me, "agreement fail, exit")
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      command,
			CommandIndex: index,
		}
	}
}

func (rf *Raft) sendHB() {
	term := <-rf.term
	go func() { rf.term <- term }()
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
clean:
	for {
		select {
		// stop candidate
		case <-rf.phase.Candidate:
		default:
			break clean
		}
	}
	// voteFor unchanged nothing to do
	go func() { rf.phase.Leader <- void{} }()

	var cancel context.CancelFunc
	<-rf.leaderCtxCh
	rf.leaderCtx, cancel = context.WithCancel(context.Background())
	go func() { rf.leaderCtxCh <- void{} }()
	go rf.Leader(cancel)

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) Leader(cancel context.CancelFunc) {
	defer cancel()
	for {
		<-rf.phase.Leader
		if rf.killed() {
			return
		}

		select {
		case <-rf.phase.Exit:
			rf.becomeFollower()
			return
		default:
			go func() { rf.phase.Leader <- void{} }()
		}

		go rf.sendHB()

		time.Sleep(HeartBeatTimeout)
	}
}
