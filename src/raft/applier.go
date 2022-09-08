package raft

import "time"

func (rf *Raft) applier() {
	for {
		select {
		case <-rf.dead:
			return
		default:
		}

		<-rf.logCh
		commitIndex := <-rf.commitIndex
		lastApplied := <-rf.lastApplied
		cnt := 0

		Debug(dDrop, rf.me, "before applying, len(log): %v, need to apply [%v, %v], snapshotLastIndex: %v", len(rf.log), lastApplied+1, commitIndex, rf.snapshotLastIndex)
	tryLoop:
		for i := lastApplied + 1; i <= commitIndex; i++ {
			select {
			case rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-1-rf.snapshotLastIndex].Command,
				CommandIndex: i,
			}:
				cnt += 1
				Debug(dApply, rf.me, "apply %v, command: %v", i, rf.log[i-1-rf.snapshotLastIndex].Command)
			case <-timeoutCh(ApplierSelectWait):
				Debug(dApply, rf.me, "applyCh failed %v, command: %v", i, rf.log[i-1-rf.snapshotLastIndex].Command)
				break tryLoop
			}
		}
		go func() { rf.logCh <- void{} }()
		go func() { rf.commitIndex <- commitIndex }()
		go func() { rf.lastApplied <- lastApplied + cnt }()

		time.Sleep(ApplierSleepTimeout)
	}
}
