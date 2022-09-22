package shardkv

import "6.824/shardctrler"

type IdErr struct {
	ID  int64
	Err Err
}

func (kv *ShardKV) fetchShard() {
	<-kv.configCh
	config := kv.config
	go func() { kv.configCh <- void{} }()

	<-kv.dataCh
	data := kv.data
	go func() { kv.dataCh <- void{} }()

	responsibleShards := make([]int, 0)
	finished := make([]bool, 0)
	for s := 0; s < shardctrler.NShards; s++ {
		if config.Shards[s] == kv.gid {
			responsibleShards = append(responsibleShards, s)
			// it's possible when duplicate request sent
			// like: restart and duplicate with the backups
			_, exist := data[s][config.Num]
			finished = append(finished, exist)
		}
	}

	allFinished := func(s []bool) bool {
		for _, f := range s {
			if !f {
				return false
			}
		}
		return true
	}

	prevConfig := config
	for !allFinished(finished) {
		<-kv.mckCh
		prevConfig = kv.mck.Query(prevConfig.Num - 1)
		go func() { kv.mckCh <- void{} }()
		for i, s := range responsibleShards {
			if finished[i] {
				continue
			}
			requestGID := prevConfig.Shards[s]
			if requestGID == 0 {
				kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], make(map[string]string))
				finished[i] = true
				continue
			}
			if requestGID == kv.gid {
				if state, exist := data[s][prevConfig.Num]; exist {
					kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], mapCopy(state))
					finished[i] = true
				}
				continue
			}
			state, err := kv.clerk.GetShard(s, requestGID, prevConfig.Num, prevConfig.Groups[requestGID])
			if err == OK {
				kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], mapCopy(state))
				finished[i] = true
			}
		}
	}
	Debug(dInfo, kv.gid-100, kv.me, "Finished fill data %v", data)
}

func (kv *ShardKV) applyCommand(index int, c Op) Err {
	//Debug(dApply, kv.gid-100, kv.me, "Apply Command %+v", c)
	<-kv.configCh
	defer func() { go func() { kv.configCh <- void{} }() }()
	<-kv.dataCh
	defer func() { go func() { kv.dataCh <- void{} }() }()

	// lastSuc received by client, delete it
	delete(kv.appliedButNotReceived, c.LastSuc)
	_, exist := kv.appliedButNotReceived[c.RequestId]
	if exist {
		// have applied before, then don't apply it again
		return OK
	}

	_, leader := kv.rf.GetState()

	switch c.Operator {
	case GetOp:
		shard := key2shard(c.Key)
		if leader {
			Debug(dSnap, kv.gid, kv.me, "Get %v = %v, index %v", c.Key, kv.data[shard][kv.config.Num], index)
		}
	case PutOp:
		shard := key2shard(c.Key)
		specGID := kv.config.Shards[shard]
		if specGID != kv.gid {
			return ErrWrongGroup
		}

		m, exist := kv.data[shard][kv.config.Num]
		if !exist {
			// responsible shard but state not ready
			return ErrWrongLeader
		}
		m[c.Key] = c.Value
		if leader {
			Debug(dSnap, kv.gid, kv.me, "Update %v = %v, index %v", c.Key, m, index)
		}
	case AppendOp:
		shard := key2shard(c.Key)
		specGID := kv.config.Shards[shard]
		if specGID != kv.gid {
			return ErrWrongGroup
		}

		m, exist := kv.data[shard][kv.config.Num]
		if !exist {
			// responsible shard but state not ready
			return ErrWrongLeader
		}

		v, exist := m[c.Key]
		if exist {
			m[c.Key] = v + c.Value
		} else {
			m[c.Key] = c.Value
		}
		if leader {
			Debug(dSnap, kv.gid, kv.me, "Update %v = %v, index %v", c.Key, m, index)
		}
	case GetShardOp:
		// When a GetShard committed, must do not operate this shard anymore
		// there is a G with newer config, need to snapshot the state and give to it.
		if c.ConfigNum == kv.config.Num {
			kv.config.Shards[c.Shard] = -1
		}
		if leader {
			Debug(dSnap, kv.gid, kv.me, "GetShardOp config.Shards: %v", kv.config.Shards)
		}
	case ConfigOp:
		if c.Config.Num > kv.config.Num {
			kv.config = c.Config
			if leader {
				Debug(dSnap, kv.gid, kv.me, "Update config %v", kv.config)
			}
		} else {
			if leader {
				Debug(dSnap, kv.gid, kv.me, "Update config failed, args: %v, hold: %v", c.Config, kv.config)
			}
		}
	case UpdateDataOp:
		kv.data[c.Shard][c.ConfigNum] = mapCopy(c.Data)
		if leader {
			Debug(dSnap, kv.gid, kv.me, "UpdateData s: %v, c: %v, data: %v, %v", c.Shard, c.ConfigNum, c.Data, c.RequestId)
		}
	}
	kv.commitIndex = index

	// only applied when OK
	kv.appliedButNotReceived[c.RequestId] = void{}

	return OK
}

func (kv *ShardKV) waiting() {
	for {
		select {
		case <-kv.dead:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				err := kv.applyCommand(msg.CommandIndex, msg.Command.(Op))

				// after command have been applied, then add notification
				waitChInter, exist := kv.notification.Load(msg.CommandIndex)
				// request timeout (notification deleted), but command finally applied
				// or the server is follower, no request is waiting
				if exist {
					// if existed, there must be someone waiting for it
					waitChInter.(chan IdErr) <- IdErr{ID: msg.Command.(Op).RequestId, Err: err}
				}
			} else if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// exit all waiting request before `SnapshotIndex`
					kv.notification.Range(func(key, value interface{}) bool {
						if key.(int) <= msg.SnapshotIndex {
							value.(chan IdErr) <- IdErr{ID: 0, Err: OK}
						}
						return true
					})
					kv.readPersist(msg.Snapshot)
				}
			}
		}
	}
}
