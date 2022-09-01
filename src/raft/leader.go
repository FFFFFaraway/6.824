package raft

import "time"

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
