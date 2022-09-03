package raft

import "time"

func (rf *Raft) applier() {
	for {
		if rf.killed() {
			return
		}

		commitIndex := <-rf.commitIndex
		go func() { rf.commitIndex <- commitIndex }()

		lastApplied := <-rf.lastApplied
		go func() { rf.lastApplied <- lastApplied }()

		<-rf.logCh
		log := rf.log
		go func() { rf.logCh <- void{} }()

		cnt := 0
		for i := lastApplied + 1; i <= commitIndex; i++ {
			Debug(dApply, rf.me, "apply %v", i)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log[i-1].Command,
				CommandIndex: i,
			}
			cnt += 1
		}
		go func() { rf.lastApplied <- <-rf.lastApplied + cnt }()

		time.Sleep(ApplierSleepTimeout)
	}
}
