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
		start := <-rf.snapshotLastIndex
		go func() { rf.snapshotLastIndex <- start }()

		cnt := 0
	tryLoop:
		for i := lastApplied + 1; i <= commitIndex; i++ {
			Debug(dApply, rf.me, "apply %v, command: %v", i, rf.log[i-1-start].Command)
			select {
			case rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-1-start].Command,
				CommandIndex: i,
			}:
				cnt += 1
			default:
				Debug(dApply, rf.me, "applyCh failed %v, command: %v", i, rf.log[i-1-start].Command)
				break tryLoop
			}
		}
		go func() { rf.logCh <- void{} }()
		go func() { rf.lastApplied <- <-rf.lastApplied + cnt }()

		time.Sleep(ApplierSleepTimeout)
	}
}
