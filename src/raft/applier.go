package raft

import "time"

func (rf *Raft) applier() {
	for {
		select {
		case <-rf.dead:
			return
		default:
		}

		commitIndex := <-rf.commitIndex
		go func() { rf.commitIndex <- commitIndex }()

		lastApplied := <-rf.lastApplied
		go func() { rf.lastApplied <- lastApplied }()

		<-rf.logCh
		cnt := 0
		for i := lastApplied + 1; i <= commitIndex; i++ {
			Debug(dApply, rf.me, "apply %v, command: %v", i, rf.log[i-1].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-1].Command,
				CommandIndex: i,
			}
			cnt += 1
		}
		go func() { rf.logCh <- void{} }()
		go func() { rf.lastApplied <- <-rf.lastApplied + cnt }()

		time.Sleep(ApplierSleepTimeout)
	}
}
