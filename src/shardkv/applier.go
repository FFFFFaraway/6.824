package shardkv

import (
	"6.824/shardctrler"
	"math/rand"
	"time"
)

type IdErr struct {
	ID  int64
	Err Err
}

func (kv *ShardKV) fetchShard(config shardctrler.Config) {
	Debug(dInfo, kv.gid-100, "fetchShard %v", config)

	responsibleShards := make([]int, 0)
	finished := make(chan int)
	go func() { finished <- 0 }()

	<-kv.dataCh
	for s := 0; s < shardctrler.NShards; s++ {
		if config.Shards[s] == kv.gid {
			// check data already exist: it's possible when duplicate request sent
			// like: restart and duplicate with the backups
			if _, exist := kv.data[s][config.Num]; !exist {
				responsibleShards = append(responsibleShards, s)
			}
		}
	}
	go func() { kv.dataCh <- void{} }()

	need := len(responsibleShards)

	for _, s := range responsibleShards {
		go func(s int, prevConfig shardctrler.Config) {
			for {
				if prevConfig.Num-1 == -1 {
					panic("asking the up to date config instead of previous config")
				}
				prevConfig = kv.getConfig(prevConfig.Num - 1)
				if prevConfig.Num == 0 {
					kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], make(map[string]string), make(map[int64]void))
					go func() { finished <- <-finished + 1 }()
					return
				}
				requestGID := prevConfig.Shards[s]
				Debug(dInfo, kv.gid-100, "try to ask G%v with prevConfig C%v for S%v", requestGID-100, prevConfig.Num, s)
				if requestGID == 0 {
					continue
				}
				for {
					state, dup, err := kv.clerk.GetShard(s, requestGID, prevConfig.Num, prevConfig.Groups[requestGID])
					if err == OK {
						Debug(dInfo, kv.gid-100, "### S%v G%v C%v <- G%v C%v", s, kv.gid-100, config.Num, requestGID-100, prevConfig.Num)
						kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], state, dup)
						Debug(dInfo, kv.gid-100, "### S%v G%v C%v <- G%v C%v done", s, kv.gid-100, config.Num, requestGID-100, prevConfig.Num)
						kv.clerk.DeleteBefore(s, requestGID, prevConfig.Num, prevConfig.Groups[requestGID])
						go func() { finished <- <-finished + 1 }()
						return
					} else if err == ErrRetryLater {
						time.Sleep(RetryLaterTimeout + time.Duration(rand.Intn(RetryLaterSpan))*time.Millisecond)
					} else if err == ErrDeleted {
						Debug(dInfo, kv.gid-100, "### S%v G%v C%v <- G%v C%v, deleted", s, kv.gid-100, config.Num, requestGID-100, prevConfig.Num)
						state = make(map[string]string)
						state[""] = ""
						kv.clerk.UpdateData(s, kv.gid, config.Num, config.Groups[kv.gid], state, make(map[int64]void))
						go func() { finished <- <-finished + 1 }()
						return
					}
				}
			}
		}(s, config)
	}

	for {
		select {
		case <-kv.dead:
			return
		case c := <-finished:
			if c >= need {
				Debug(dInfo, kv.gid-100, "Finished fill data C%v", config.Num)
				return
			}
			finished <- c
		}
	}
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
			//Debug(dSnap, kv.gid-100, "GetShardOp config.Shards: %v, index %v", kv.config.Shards, index)
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
			kv.data[c.Shard][c.ConfigNum] = mapCopy(c.Data)
			kv.appliedButNotReceived[c.Shard][c.ConfigNum] = mapCopy(c.Dup)
			if leader {
				Debug(dSnap, kv.gid-100, "UpdateData s: %v, c: %v, data: %v, %v, index %v", c.Shard, c.ConfigNum, c.Data, c.RequestId, index)
			}
		}
	case DeleteBeforeOp:
		for n := 1; n <= c.ConfigNum; n++ {
			if _, exist := kv.data[c.Shard][n]; exist {
				kv.data[c.Shard][n] = make(map[string]string)
				kv.data[c.Shard][n][""] = ""
				delete(kv.appliedButNotReceived[c.Shard], n)
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
