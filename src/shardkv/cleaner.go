package shardkv

import "time"

const WaitAllDie = 5 * time.Second

func (kv *ShardKV) cleaner() {
	select {
	case <-kv.dead:
		Debug(dTerm, kv.gid-100, "killed")
		for {
			select {
			case <-kv.configCh:
			case <-kv.mckCh:
			case <-kv.dataCh:
			case <-timeoutCh(WaitAllDie):
				return
			}
		}
	}
}
