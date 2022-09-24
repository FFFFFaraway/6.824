package shardkv

import (
	"time"
)

type void struct{}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	ensureClosed(kv.dead)
	kv.rf.Kill()
}

func ensureClosed(i interface{}) {
	switch ch := i.(type) {
	case chan void:
		if ch != nil {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	}
}

func timeoutCh(t time.Duration) (done chan void) {
	done = make(chan void)
	go func() {
		time.Sleep(t)
		select {
		case done <- void{}:
		// if no one wait for it, then just abort
		default:
		}
	}()
	return
}

func (kv *ShardKV) Commit(command Op, postFunc func() Err) Err {
	specId := command.RequestId
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return ErrWrongLeader
	}

	waitCh := make(chan IdErr)
	kv.notification.Store(index, waitCh)
	defer func() {
		// delete first
		kv.notification.Delete(index)
		// it's possible that the notification has been loaded, but haven't sent signal yet
		// need to wait again
		select {
		case id := <-waitCh:
			Debug(dClean, kv.gid-100, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	var opName string
	switch command.Operator {
	case GetOp:
		opName = "Get"
	case PutOp:
		fallthrough
	case AppendOp:
		opName = "PutAppend"
	case ConfigOp:
		opName = "Update Config"
	case GetShardOp:
		opName = "GetShard"
	case UpdateDataOp:
		opName = "UpdateDataOp"
	}

	select {
	case <-kv.dead:
		Debug(dClean, kv.gid-100, "%v Killed", command.RequestId)
	case realIdErr := <-waitCh:
		if realIdErr.Err != OK {
			Debug(dInfo, kv.gid-100, opName+" wrong group %v", specId)
			return realIdErr.Err
		}
		if realIdErr.ID != specId {
			Debug(dInfo, kv.gid-100, opName+" canceled spec: %v, real: %v", specId, realIdErr.ID)
			return ErrWrongLeader
		}
		if err := postFunc(); err != OK {
			return err
		}
		Debug(dInfo, kv.gid-100, opName+" ok %v", specId)
		return OK
	case <-timeoutCh(RequestWaitTimeout):
		Debug(dInfo, kv.gid-100, opName+" timeout")
	}
	return ErrWrongLeader
}

func mapCopy[K string | int64, V string | void](m map[K]V) (res map[K]V) {
	res = make(map[K]V)
	for k, v := range m {
		res[k] = v
	}
	return
}
