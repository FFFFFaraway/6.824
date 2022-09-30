package shardkv

import (
	"6.824/labgob"
	"bytes"
	"time"
)

func (kv *ShardKV) compareSnapshot() {
	for {
		select {
		case <-kv.dead:
			return
		case <-timeoutCh(100 * time.Millisecond):
			<-kv.configCh
			<-kv.dataCh
			if kv.maxraftstate != -1 &&
				kv.commitIndex > kv.snapshotLastIndex &&
				kv.persister.RaftStateSize() > 7*kv.maxraftstate {
				Debug(dSnap, kv.gid-100, "Snapshot before %v", kv.commitIndex)
				kv.persist(kv.commitIndex)
				Debug(dSnap, kv.gid-100, "Done Snapshot before %v", kv.commitIndex)
			}
			go func() { kv.dataCh <- void{} }()
			go func() { kv.configCh <- void{} }()
		}
	}
}

func (kv *ShardKV) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(kv.config)
	e.Encode(kv.data)
	e.Encode(kv.appliedButNotReceived)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	// If the snapshot is out of date
	var lastIndex int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIndex)

	if lastIndex < kv.commitIndex {
		return
	}

	<-kv.dataCh
	kv.snapshotLastIndex = lastIndex
	d.Decode(&kv.config)
	d.Decode(&kv.data)
	d.Decode(&kv.appliedButNotReceived)
	kv.commitIndex = kv.snapshotLastIndex
	go func() { kv.dataCh <- void{} }()
}
