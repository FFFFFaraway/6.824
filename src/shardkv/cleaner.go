package shardkv

func (kv *ShardKV) cleaner() {
	select {
	case <-kv.dead:
		Debug(dTerm, kv.gid-100, kv.me, "killed")
		for {
			select {
			case <-kv.dataCh:
			default:
				return
			}
		}
	}
}
