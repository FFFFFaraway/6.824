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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { cnt <- 0 }()

	for i := range rf.peers {
		if i != rf.me {
			go func(ctx context.Context, i int) {
				reply := &AEReply{}
				ok := rf.sendAE(i, &AEArgs{Term: term, Logs: log}, reply)
				term := <-rf.term
				if ok && reply.Term > term {
					go func() { rf.term <- reply.Term }()
					go func() { rf.phase.Exit <- void{} }()
					return
				}
				go func() { rf.term <- term }()

				if ok {
					select {
					case <-ctx.Done():
						return
					default:
						cnt <- <-cnt + 1
					}
				}
			}(ctx, i)
		}
	}

	// periodically check if cnt satisfy the need
	go func(ctx context.Context) {
		for {
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
	}(ctx)

	// wait util exit, no timeout, no retry
	select {
	case <-suc:
		Debug(dElection, rf.me, "agreement success")
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
	// TODO: this is bad, it's possible not grab the Exit (grabbed by the main loop)
	case <-rf.phase.Exit:
		go func() { rf.phase.Exit <- void{} }()
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

	term := <-rf.term
	go func() { rf.term <- term }()
	Debug(dPhase, rf.me, "become Leader %v", term)
}

func (rf *Raft) Leader() {
main:
	for {
		<-rf.phase.Leader
		if rf.killed() {
			return
		}

		select {
		case <-rf.phase.Exit:
			rf.becomeFollower()
			continue main
		default:
			go func() { rf.phase.Leader <- void{} }()
		}

		go rf.sendHB()

		time.Sleep(HeartBeatTimeout)
	}
}
