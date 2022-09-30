package shardkv

func (kv *ShardKV) cleaner() {
	select {
	case <-kv.dead:
		Debug(dTerm, kv.gid-100, "killed")
		for {
			select {
			case <-kv.configCh:
			case <-kv.dataCh:
			case <-timeoutCh(WaitAllDie):
				return
			}
		}
	}
}
