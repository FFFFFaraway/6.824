package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	GetOp = iota
	PutOp
	AppendOp
	ConfigOp
)

const (
	waitTimeout = 300 * time.Millisecond
)

type Op struct {
	Key       string
	Value     string
	Operator  int
	RequestId int64
	LastSuc   int64
	Config    shardctrler.Config
}

type ShardKV struct {
	// mu protect config only
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config shardctrler.Config
	dead   chan void
	mck    *shardctrler.Clerk
	dataCh chan void
	data   map[string]string
	// each index allocate a channel to inform the waiting request
	// map[specIndex int]committed RequestId in spec index, chan int64
	notification sync.Map
	// for duplicate apply detection: applied to state machine but haven't received by client yet
	// map[requestId int64]void
	appliedButNotReceived map[int64]void
	persister             *raft.Persister
	// protected by dataCh
	snapshotLastIndex int
	commitIndex       int
}

func (kv *ShardKV) applyCommand(index int, c Op) {
	//Debug(dApply, kv.gid, kv.me, "Apply Command %+v", c)

	// lastSuc received by client, delete it
	delete(kv.appliedButNotReceived, c.LastSuc)
	_, exist := kv.appliedButNotReceived[c.RequestId]
	if exist {
		// have applied before, then don't apply it again
		return
	}
	kv.appliedButNotReceived[c.RequestId] = void{}

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
	case ConfigOp:
		kv.mu.Lock()
		kv.config = c.Config
		kv.mu.Unlock()
	}
	//if v, exist := kv.data[c.Key]; exist {
	//	if c.Operator == GetOp {
	//		Debug(dSnap, kv.gid, kv.me, "Get %v = %v, index %v", c.Key, v, index)
	//	} else {
	//		Debug(dSnap, kv.gid, kv.me, "Update %v = %v, index %v", c.Key, v, index)
	//	}
	//}
}

func (kv *ShardKV) compareSnapshot() {
	for {
		select {
		case <-kv.dead:
			return
		case <-timeoutCh(100 * time.Millisecond):
			<-kv.dataCh
			if kv.maxraftstate != -1 &&
				kv.commitIndex > kv.snapshotLastIndex &&
				kv.persister.RaftStateSize() > 7*kv.maxraftstate {
				Debug(dSnap, kv.gid, kv.me, "Snapshot before %v", kv.commitIndex)
				kv.persist(kv.commitIndex)
				Debug(dSnap, kv.gid, kv.me, "Done Snapshot before %v", kv.commitIndex)
			}
			go func() { kv.dataCh <- void{} }()
		}
	}
}

func (kv *ShardKV) updateConfig() {
	for {
		time.Sleep(ConfigurationTimeout)
		_, isLeader := kv.rf.GetState()
		// only the leader need to update the configuration
		if !isLeader {
			continue
		}

		newConfig := kv.mck.Query(-1)
		// There are no others writing it, so we can copy once to avoid waiting lock
		kv.mu.Lock()
		curNum := kv.config.Num
		kv.mu.Unlock()

		if curNum == newConfig.Num {
			continue
		}

		// isolation from client request id
		rid := int64(-newConfig.Num)
		// may fail, but we will continue try it
		// do not update here, must update through the applyCh
		kv.Commit(Op{
			Config:    newConfig,
			Operator:  ConfigOp,
			RequestId: rid,
			LastSuc:   int64(-curNum),
		}, func() {})
	}
}

func (kv *ShardKV) waiting() {
	for {
		select {
		case <-kv.dead:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				<-kv.dataCh

				kv.applyCommand(msg.CommandIndex, msg.Command.(Op))
				kv.commitIndex = msg.CommandIndex

				go func() { kv.dataCh <- void{} }()

				// after command have been applied, then add notification
				waitChInter, exist := kv.notification.Load(msg.CommandIndex)
				// request timeout (notification deleted), but command finally applied
				// or the server is follower, no request is waiting
				if exist {
					// if existed, there must be someone waiting for it
					waitChInter.(chan int64) <- msg.Command.(Op).RequestId
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

func (kv *ShardKV) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(index)
	e.Encode(kv.data)
	e.Encode(kv.appliedButNotReceived)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readPersist(snapshot []byte) {
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

func (kv *ShardKV) Commit(command Op, postFunc func()) (WrongLeader bool) {
	specId := command.RequestId
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return true
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
			Debug(dClean, kv.gid, kv.me, "delete index %v with request id %v", index, id)
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
	}

	select {
	case <-kv.dead:
		Debug(dClean, kv.gid, kv.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			Debug(dInfo, kv.gid, kv.me, opName+" canceled spec: %v, real: %v", specId, realId)
			return true
		}
		postFunc()
		Debug(dInfo, kv.gid, kv.me, opName+" ok %v", specId)
		return false
	case <-timeoutCh(waitTimeout):
		Debug(dInfo, kv.gid, kv.me, opName+" timeout")
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//Debug(dInfo, kv.gid, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.gid, kv.me, "Get reply %+v", reply)
	shard := key2shard(args.Key)
	kv.mu.Lock()
	specGID := kv.config.Shards[shard]
	kv.mu.Unlock()
	if specGID != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.Commit(Op{
		Key:       args.Key,
		Operator:  GetOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() {
		<-kv.dataCh
		v, exist := kv.data[args.Key]
		go func() { kv.dataCh <- void{} }()
		if !exist {
			reply.Err = ErrNoKey
			return
		}
		reply.Err = OK
		reply.Value = v
	}) {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//Debug(dInfo, kv.gid, kv.me, "PutAppend %+v", args)
	//defer Debug(dInfo, kv.gid, kv.me, "PutAppend reply %+v", reply)
	shard := key2shard(args.Key)
	kv.mu.Lock()
	specGID := kv.config.Shards[shard]
	kv.mu.Unlock()
	if specGID != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

	Operator := -1
	switch args.Op {
	case "Put":
		Operator = PutOp
	case "Append":
		Operator = AppendOp
	default:
		Debug(dError, kv.gid, kv.me, "ERROR PutAppend unknown op: %v", args.Op)
		return
	}

	if kv.Commit(Op{
		Key:       args.Key,
		Value:     args.Value,
		Operator:  Operator,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() {}) {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) cleaner() {
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

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

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

	Debug(dInfo, kv.gid, kv.me, "Restarted with lastIndex %v", kv.snapshotLastIndex)

	go kv.waiting()
	go kv.compareSnapshot()
	go kv.cleaner()
	go kv.updateConfig()

	return kv
}
