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
				ok := rf.sendAE(i, &AEArgs{Term: term, Logs: log, CommitIndex: commitIndex}, reply)

				term := <-rf.term
				if ok && reply.Term > term {
					go func() { rf.term <- reply.Term }()
					go func() { rf.phase.Exit <- void{} }()
					return
				}
				go func() { rf.term <- term }()

				ctx := <-rf.leaderCtx
				go func() { rf.leaderCtx <- ctx }()

				if ok {
					select {
					case <-ctx.Done():
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
			ctx := <-rf.leaderCtx
			go func() { rf.leaderCtx <- ctx }()

			select {
			case c := <-cnt:
				if c >= need {
					suc <- void{}
					return
				}
				// block here wait for RV ack
				cnt <- c
			case <-ctx.Done():
				return
			}
		}
	}()

	// press the heartbeatTimer
	go func() { rf.heartbeatTimer <- void{} }()

	ctx := <-rf.leaderCtx
	go func() { rf.leaderCtx <- ctx }()
	// wait util exit, no timeout, no retry
	select {
	case <-suc:
		Debug(dElection, rf.me, "agreement success")
		Debug(dApply, rf.me, "apply %v", index)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
		go func() { rf.commitIndex <- <-rf.commitIndex + 1 }()
	case <-ctx.Done():
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

	commitIndex := <-rf.commitIndex
	go func() { rf.commitIndex <- commitIndex }()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term, CommitIndex: commitIndex}, reply)

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
		case <-rf.leaderCtx:
		default:
			break clean
		}
	}
	// voteFor unchanged nothing to do
	go func() { rf.phase.Leader <- void{} }()

	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(context.Background())
	go func() { rf.leaderCtx <- ctx }()

	go rf.Leader(cancel)
	go rf.HB()

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) HB() {
	go rf.sendHB()
	for {
		ctx := <-rf.leaderCtx
		go func() { rf.leaderCtx <- ctx }()

		timeout := timeoutCh(HeartBeatTimeout)

		select {
		case <-ctx.Done():
			return
		case <-timeout:
			go rf.sendHB()
		case <-rf.heartbeatTimer:
		}
	}
}

func (rf *Raft) Leader(cancel context.CancelFunc) {
	defer cancel()
	for {
		if rf.killed() {
			return
		}
		select {
		case <-rf.phase.Exit:
			rf.becomeFollower()
			return
		default:
			time.Sleep(KilledCheckTimeout)
		}
	}
}
