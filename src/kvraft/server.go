package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"time"
)

const (
	GetOp = iota
	PutOp
	AppendOp
)

const (
	waitTimeout = 300 * time.Millisecond
)

type Command struct {
	Key            string
	Value          string
	Operator       int
	NotificationId int64
	LastSuc        int64
}

type void struct{}

type KVServer struct {
	//mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    chan void // set by Kill()

	maxraftstate int // snapshot if log grows this big

	dataCh chan void
	data   map[string]string
	// each index allocate a channel to inform the waiting request
	// map[specIndex int]committed NotificationId in spec index, chan int64
	notification sync.Map
	// for duplicate apply detection: applied to state machine but haven't received by client yet
	// map[requestId int64]void
	appliedButNotReceived map[int64]void
	persister             *raft.Persister
	// protected by dataCh
	snapshotLastIndex int
	commitIndex       int
}

func (kv *KVServer) applyCommand(index int, c Command) {
	//Debug(dApply, kv.me, "Apply Command %+v", c)

	// lastSuc received by client, delete it
	delete(kv.appliedButNotReceived, c.LastSuc)
	_, exist := kv.appliedButNotReceived[c.NotificationId]
	if exist {
		// have applied before, then don't apply it again
		return
	}
	kv.appliedButNotReceived[c.NotificationId] = void{}

	switch c.Operator {
	case PutOp:
		kv.data[c.Key] = c.Value
	case AppendOp:
		v, exist := kv.data[c.Key]
		if exist {
			kv.data[c.Key] = v + c.Value
		} else {
			kv.data[c.Key] = c.Value
		}
	}
	if v, exist := kv.data[c.Key]; exist {
		if c.Operator == GetOp {
			Debug(dSnap, kv.me, "Get %v = %v, index %v", c.Key, v, index)
		} else {
			Debug(dSnap, kv.me, "Update %v = %v, index %v", c.Key, v, index)
		}
	}
}

func (kv *KVServer) compareSnapshot() {
	for {
		select {
		case <-kv.dead:
			return
		case <-timeoutCh(100 * time.Millisecond):
			<-kv.dataCh
			if kv.maxraftstate != -1 &&
				kv.commitIndex > kv.snapshotLastIndex &&
				kv.persister.RaftStateSize() > 7*kv.maxraftstate {
				Debug(dSnap, kv.me, "Snapshot before %v", kv.commitIndex)
				kv.persist(kv.commitIndex)
				Debug(dSnap, kv.me, "Done Snapshot before %v", kv.commitIndex)
			}
			go func() { kv.dataCh <- void{} }()
		}
	}
}

func (kv *KVServer) waiting() {
	for {
		select {
		case <-kv.dead:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				<-kv.dataCh

				kv.applyCommand(msg.CommandIndex, msg.Command.(Command))
				kv.commitIndex = msg.CommandIndex

				go func() { kv.dataCh <- void{} }()

				// after command have been applied, then add notification
				waitChInter, exist := kv.notification.Load(msg.CommandIndex)
				// request timeout (notification deleted), but command finally applied
				// or the server is follower, no request is waiting
				if exist {
					// if existed, there must be someone waiting for it
					waitChInter.(chan int64) <- msg.Command.(Command).NotificationId
				}
			} else if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// exit all waiting request before `SnapshotIndex`
					kv.notification.Range(func(key, value interface{}) bool {
						if key.(int) <= msg.SnapshotIndex {
							value.(chan int64) <- 0
						}
						return true
					})
					kv.readPersist(msg.Snapshot)
				}
			}
		}
	}
}

func (kv *KVServer) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(kv.data)
	e.Encode(kv.appliedButNotReceived)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	<-kv.dataCh
	d.Decode(&kv.snapshotLastIndex)
	d.Decode(&kv.data)
	d.Decode(&kv.appliedButNotReceived)
	kv.commitIndex = kv.snapshotLastIndex
	go func() { kv.dataCh <- void{} }()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	specId := args.RequestId
	index, _, isLeader := kv.rf.Start(Command{
		Key:            args.Key,
		Operator:       GetOp,
		NotificationId: specId,
		LastSuc:        args.LastSuc,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	waitCh := make(chan int64)
	kv.notification.Store(index, waitCh)
	defer func() {
		// delete first
		kv.notification.Delete(index)
		// it's possible that the notification has been loaded, but haven't sent signal yet
		// need to wait again
		select {
		case id := <-waitCh:
			Debug(dInfo, kv.me, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	select {
	case <-kv.dead:
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			reply.Err = ErrWrongLeader
			Debug(dInfo, kv.me, "Get canceled spec: %v, real: %v", specId, realId)
			return
		}
		<-kv.dataCh
		v, exist := kv.data[args.Key]
		go func() { kv.dataCh <- void{} }()
		if !exist {
			reply.Err = ErrNoKey
			return
		}
		reply.Err = OK
		reply.Value = v
		Debug(dInfo, kv.me, "Get ok %v", specId)
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
		LastSuc:        args.LastSuc,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	waitCh := make(chan int64)
	kv.notification.Store(index, waitCh)
	defer func() {
		kv.notification.Delete(index)
		select {
		case id := <-waitCh:
			Debug(dInfo, kv.me, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	select {
	case <-kv.dead:
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			reply.Err = ErrWrongLeader
			Debug(dInfo, kv.me, "PutAppend canceled spec:%v, real: %v", specId, realId)
			return
		}
		reply.Err = OK
		Debug(dInfo, kv.me, "PutAppend ok %v", specId)
	case <-timeoutCh(waitTimeout):
		reply.Err = ErrWrongLeader
		Debug(dInfo, kv.me, "PutAppend timeout")
	}
}

func (kv *KVServer) cleaner() {
	select {
	case <-kv.dead:
		for {
			select {
			case <-kv.dataCh:
			default:
				return
			}
		}
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
	kv.appliedButNotReceived = make(map[int64]void)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dataCh = make(chan void)
	kv.data = make(map[string]string)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = make(chan void)
	kv.snapshotLastIndex = 0
	kv.commitIndex = 0

	go func() { kv.dataCh <- void{} }()

	kv.readPersist(kv.persister.ReadSnapshot())

	Debug(dInfo, kv.me, "Restarted with lastIndex %v", kv.snapshotLastIndex)

	go kv.waiting()
	go kv.compareSnapshot()
	go kv.cleaner()

	return kv
}
