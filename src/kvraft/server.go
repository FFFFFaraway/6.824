package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"time"
)

const (
	GetOp = iota
	PutOp
	AppendOp
)

const (
	waitTimeout = 100 * time.Millisecond
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
	dead    chan void // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data sync.Map
	// map[spec index int]commit NotificationId in spec index, chan int64
	notification sync.Map
}

func (kv *KVServer) applyCommand(visited map[int64]void, msg raft.ApplyMsg) {
	//Debug(dApply, kv.me, "Apply Command %+v", c)
	index := msg.CommandIndex
	c := msg.Command.(Command)

	waitChInter, exist := kv.notification.Load(index)
	if !exist {
		waitChInter = make(chan int64)
		kv.notification.Store(index, waitChInter)
	}
	go func() { waitChInter.(chan int64) <- c.NotificationId }()

	_, exist = visited[c.NotificationId]
	if exist {
		return
	}
	visited[c.NotificationId] = void{}

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
}

func (kv *KVServer) waiting() {
	visited := make(map[int64]void)
	for {
		select {
		case <-kv.dead:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.applyCommand(visited, msg)
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	specId := args.RequestId
	index, _, isLeader := kv.rf.Start(Command{
		Key:            args.Key,
		Operator:       GetOp,
		NotificationId: specId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	waitChInter, exist := kv.notification.Load(index)
	if !exist {
		waitChInter = make(chan int64)
		kv.notification.Store(index, waitChInter)
	}
	waitCh := waitChInter.(chan int64)

	select {
	case <-kv.dead:
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "Killed")
	case realId := <-waitCh:
		go func() { waitCh <- realId }()

		if realId != specId {
			reply.Err = ErrWrongLeader
			Debug(dInfo, kv.me, "Get canceled spec: %v, real: %v", specId, realId)
			return
		}
		v, exist := kv.data.Load(args.Key)
		if !exist {
			reply.Err = ErrNoKey
			return
		}
		reply.Err = OK
		reply.Value = v.(string)
		Debug(dInfo, kv.me, "Get ok %v", specId)
		kv.notification.Delete(specId)
	case <-timeoutCh(waitTimeout):
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "Get timeout")
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//Debug(dInfo, kv.me, "PutAppend %+v", args)
	//defer Debug(dInfo, kv.me, "PutAppend reply %+v", reply)
	Operator := PutOp
	if args.Op == "Put" {
	} else if args.Op == "Append" {
		Operator = AppendOp
	} else {
		Debug(dError, kv.me, "ERROR PutAppend unknown op: %v", args.Op)
		return
	}

	specId := args.RequestId
	index, _, isLeader := kv.rf.Start(Command{
		Key:            args.Key,
		Value:          args.Value,
		Operator:       Operator,
		NotificationId: specId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	waitChInter, exist := kv.notification.Load(index)
	if !exist {
		waitChInter = make(chan int64)
		kv.notification.Store(index, waitChInter)
	}
	waitCh := waitChInter.(chan int64)

	select {
	case <-kv.dead:
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "Killed")
	case realId := <-waitCh:
		go func() { waitCh <- realId }()

		if realId != specId {
			reply.Err = ErrWrongLeader
			Debug(dInfo, kv.me, "PutAppend canceled spec:%v, real: %v", specId, realId)
			return
		}

		reply.Err = OK
		Debug(dInfo, kv.me, "PutAppend ok %v", specId)
		kv.notification.Delete(specId)
	case <-timeoutCh(waitTimeout):
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "PutAppend timeout")
	}
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
	kv.dead = make(chan void)

	go kv.waiting()

	return kv
}
