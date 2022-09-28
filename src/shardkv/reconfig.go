package shardkv

import (
	"6.824/shardctrler"
	"math/rand"
	"time"
)

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

func (kv *ShardKV) getConfig(n int) shardctrler.Config {
	inter, exist := kv.configCache.Load(n)
	// if n == -1, never exist
	if exist {
		return inter.(shardctrler.Config)
	} else {
		<-kv.mckCh
		c := kv.mck.Query(n)
		go func() { kv.mckCh <- void{} }()
		kv.configCache.Store(c.Num, c)
		return c
	}
}

func (kv *ShardKV) updateConfig() {
	for {
		select {
		case <-kv.dead:
			return
		case <-timeoutCh(ConfigurationTimeout):
			_, isLeader := kv.rf.GetState()
			// only the leader need to update the configuration
			if !isLeader {
				continue
			}

			<-kv.mckCh
			newConfig := kv.mck.Query(-1)
			go func() { kv.mckCh <- void{} }()

			//Debug(dInfo, kv.gid-100, "try update %v", newConfig)
			<-kv.configCh
			oldConfig := kv.config
			go func() { kv.configCh <- void{} }()

			if newConfig.Num <= oldConfig.Num {
				continue
			}

			//Debug(dInfo, kv.gid-100, "Need to update %v", newConfig)
			// ensure the config is updated
			queryConfig := newConfig
			for {
				if servers, exist := queryConfig.Groups[kv.gid]; exist {
					kv.clerk.UpdateConfig(kv.gid, newConfig, servers)
					break
				}
				if queryConfig.Num-1 <= 0 {
					break
				}
				queryConfig = kv.getConfig(queryConfig.Num - 1)
			}
		}
	}
}
