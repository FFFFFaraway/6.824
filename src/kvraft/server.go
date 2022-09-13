package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PutOp = iota
	AppendOp
)

type Command struct {
	Key            string
	Value          string
	Operator       int
	NotificationId int64
}

type void struct{}

type KVServer struct {
	//mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data         sync.Map
	notification sync.Map
}

func (kv *KVServer) applyCommand(c Command) {
	//Debug(dApply, kv.me, "Apply Command %+v", c)
	switch c.Operator {
	case PutOp:
		kv.data.Store(c.Key, c.Value)
	case AppendOp:
		v, exist := kv.data.Load(c.Key)
		if exist {
			kv.data.Store(c.Key, v.(string)+c.Value)
		} else {
			kv.data.Store(c.Key, c.Value)
		}
	}
	waitCh, exist := kv.notification.Load(c.NotificationId)
	if exist {
		ensureClosed(waitCh)
	}
}

func (kv *KVServer) waiting() {
	for {
		select {
		case c := <-kv.applyCh:
			if c.CommandValid {
				kv.applyCommand(c.Command.(Command))
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	v, exist := kv.data.Load(args.Key)
	if !exist {
		reply.Err = ErrNoKey
		return
	}
	reply.Err = OK
	reply.Value = v.(string)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug(dInfo, kv.me, "PutAppend %+v", args)
	defer Debug(dInfo, kv.me, "PutAppend reply %+v", reply)
	Operator := PutOp
	if args.Op == "Put" {
	} else if args.Op == "Append" {
		Operator = AppendOp
	} else {
		Debug(dError, kv.me, "PutAppend unknown op: %v", args.Op)
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	nid := args.RequestId
	ch, exist := kv.notification.Load(nid)
	if exist {
		<-ch.(chan void)
		reply.Err = OK
		return
	}

	waitCh := make(chan void)
	kv.notification.Store(nid, waitCh)

	kv.rf.Start(Command{
		Key:            args.Key,
		Value:          args.Value,
		Operator:       Operator,
		NotificationId: nid,
	})

	<-waitCh
	reply.Err = OK
	go kv.lazyDelete(nid)

	return
}

func (kv *KVServer) lazyDelete(nid int64) {
	time.Sleep(time.Second)
	kv.notification.Delete(nid)
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.waiting()

	return kv
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
