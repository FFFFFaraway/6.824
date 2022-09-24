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
	var data [shardctrler.NShards]map[int]map[string]string
	for s := range kv.data {
		data[s] = make(map[int]map[string]string)
		for j := range kv.data[s] {
			data[s][j] = mapCopy(kv.data[s][j])
		}
	}
	var dup [shardctrler.NShards]map[int]map[int64]void
	for s := range kv.appliedButNotReceived {
		dup[s] = make(map[int]map[int64]void)
		for j := range kv.appliedButNotReceived[s] {
			dup[s][j] = mapCopy(kv.appliedButNotReceived[s][j])
		}
	}
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
				kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], make(map[string]string), make(map[int64]void), -1)
				finished[i] = true
				continue
			}
			if requestGID == kv.gid {
				if _, exist := data[s][prevConfig.Num]; exist {
					kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], nil, nil, prevConfig.Num)
					finished[i] = true
				}
				continue
			}
			state, dup, err := kv.clerk.GetShard(s, requestGID, prevConfig.Num, prevConfig.Groups[requestGID])
			if err == OK {
				kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], mapCopy(state), mapCopy(dup), -1)
				finished[i] = true
			}
		}
	}
	Debug(dInfo, kv.gid-100, "Finished fill data %v", data)
}

func (kv *ShardKV) applyCommand(index int, c Op) Err {
	<-kv.configCh
	defer func() { go func() { kv.configCh <- void{} }() }()
	<-kv.dataCh
	defer func() { go func() { kv.dataCh <- void{} }() }()

	_, leader := kv.rf.GetState()
	if leader {
		//Debug(dApply, kv.gid-100, "Apply Command %+v", c)
	}

	kv.commitIndex = index

	shardDup := 0
	if c.Operator == GetOp || c.Operator == PutOp || c.Operator == AppendOp {
		shardDup = key2shard(c.Key)
	} else if c.Operator == UpdateDataOp {
		shardDup = c.Shard
	}

	// lastSuc received by client, delete it
	delete(kv.appliedButNotReceived[shardDup][kv.config.Num], c.LastSuc)
	_, exist := kv.appliedButNotReceived[shardDup][kv.config.Num][c.RequestId]
	if exist {
		// have applied before, then don't apply it again
		return OK
	}

	switch c.Operator {
	case GetOp:
		shard := key2shard(c.Key)
		if leader {
			Debug(dSnap, kv.gid-100, "Get %v = %v, index %v", c.Key, kv.data[shard][kv.config.Num][c.Key], index)
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
			Debug(dSnap, kv.gid-100, "Update %v = %v, index %v", c.Key, c.Value, index)
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
			Debug(dSnap, kv.gid-100, "Update %v = %v, index %v", c.Key, m[c.Key], index)
		}
	case GetShardOp:
		// same config num, we need to drop all request after sent Shard state
		// or asked config num is bigger, then we can't hold the shard.
		// [rare] It's possible that we are asked for like C5, but we are at C2, then we can't hold S in C2,
		// and ALSO we may then receive a C4 config then overwrite that forbidden flag (if we just use -1).
		if c.ConfigNum >= kv.config.Num {
			if kv.config.Shards[c.Shard] == kv.gid {
				kv.config.Shards[c.Shard] = -c.ConfigNum
			}
		}
		if leader {
			Debug(dSnap, kv.gid-100, "GetShardOp config.Shards: %v, index %v", kv.config.Shards, index)
		}
	case ConfigOp:
		if c.Config.Num > kv.config.Num {
			kv.config.Num = c.Config.Num
			kv.config.Groups = c.Config.Groups
			// [rare] It's possible that we are asked for like C5, but we are at C2, then we can't hold S in C2,
			// and ALSO we may then receive a C4 config then overwrite that forbidden flag.
			for s := range c.Config.Shards {
				forbiddenNum := 0
				if kv.config.Shards[s] < 0 {
					forbiddenNum = -kv.config.Shards[s]
				}
				if c.Config.Num > forbiddenNum {
					kv.config.Shards[s] = c.Config.Shards[s]
				}
			}
			if leader {
				Debug(dSnap, kv.gid-100, "Update config %v, index %v", kv.config, index)
			}
		} else {
			if leader {
				Debug(dSnap, kv.gid-100, "Update config failed, args: %v, hold: %v, index %v", c.Config, kv.config, index)
			}
		}
	case UpdateDataOp:
		if _, exist := kv.data[c.Shard][c.ConfigNum]; exist {
			if leader {
				Debug(dSnap, kv.gid-100, "UpdateData failed s: %v, c: %v, data: %v, %v, index %v", c.Shard, c.ConfigNum, c.Data, c.RequestId, index)
			}
		} else {
			if c.PrevNum != -1 {
				// self UpdateData
				kv.data[c.Shard][c.ConfigNum] = mapCopy(kv.data[c.Shard][c.PrevNum])
				kv.appliedButNotReceived[c.Shard][c.ConfigNum] = mapCopy(kv.appliedButNotReceived[c.Shard][c.PrevNum])
				if leader {
					Debug(dSnap, kv.gid-100, "Self UpdateData from %v s: %v, c: %v, data: %v, %v, index %v", c.PrevNum, c.Shard, c.ConfigNum, c.Data, c.RequestId, index)
				}
			} else {
				kv.data[c.Shard][c.ConfigNum] = mapCopy(c.Data)
				kv.appliedButNotReceived[c.Shard][c.ConfigNum] = mapCopy(c.Dup)
				if leader {
					Debug(dSnap, kv.gid-100, "UpdateData s: %v, c: %v, data: %v, %v, index %v", c.Shard, c.ConfigNum, c.Data, c.RequestId, index)
				}
			}
		}
	}
	if _, exist := kv.appliedButNotReceived[shardDup][kv.config.Num]; !exist {
		kv.appliedButNotReceived[shardDup][kv.config.Num] = make(map[int64]void)
	}
	// only applied when OK
	kv.appliedButNotReceived[shardDup][kv.config.Num][c.RequestId] = void{}

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
