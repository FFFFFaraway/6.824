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
	GetShardOp
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
	// for GetShard and Internal UpdateConfig
	Config shardctrler.Config
	// for GetShard
	Shard int
}

type IdErr struct {
	ID  int64
	Err Err
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
	clerk  *Clerk
	config shardctrler.Config
	dead   chan void
	mck    *shardctrler.Clerk
	dataCh chan void
	data   [shardctrler.NShards]map[string]string
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
	finishedConfig    int
}

func (kv *ShardKV) applyCommand(index int, c Op) Err {
	//Debug(dApply, kv.gid, kv.me, "Apply Command %+v", c)

	// lastSuc received by client, delete it
	delete(kv.appliedButNotReceived, c.LastSuc)
	_, exist := kv.appliedButNotReceived[c.RequestId]
	if exist {
		// have applied before, then don't apply it again
		return OK
	}
	kv.appliedButNotReceived[c.RequestId] = void{}

	switch c.Operator {
	case PutOp:
		shard := key2shard(c.Key)
		kv.mu.Lock()
		if kv.config.Num > kv.finishedConfig {
			kv.mu.Unlock()
			return ErrWrongLeader
		}
		specGID := kv.config.Shards[shard]
		kv.mu.Unlock()
		if specGID != kv.gid {
			return ErrWrongGroup
		}
		kv.data[shard][c.Key] = c.Value
	case AppendOp:
		shard := key2shard(c.Key)
		kv.mu.Lock()
		if kv.config.Num > kv.finishedConfig {
			kv.mu.Unlock()
			return ErrWrongLeader
		}
		specGID := kv.config.Shards[shard]
		kv.mu.Unlock()
		if specGID != kv.gid {
			return ErrWrongGroup
		}
		v, exist := kv.data[shard][c.Key]
		if exist {
			kv.data[shard][c.Key] = v + c.Value
		} else {
			kv.data[shard][c.Key] = c.Value
		}
	case GetShardOp:
		fallthrough
	case ConfigOp:
		kv.mu.Lock()
		if c.Config.Num <= kv.config.Num {
			kv.mu.Unlock()
			return OK
		}
		lastConfig := kv.config
		kv.config = c.Config
		kv.mu.Unlock()
		// don't block, deadlock
		// according to the new Config, get the shards
		go func(lastConfig shardctrler.Config) {
			if lastConfig.Num == 0 {
				kv.mu.Lock()
				kv.finishedConfig++
				kv.mu.Unlock()
				// shards from scratch
				return
			}

			if lastConfig.Num != c.Config.Num-1 {
				lastConfig = kv.mck.Query(c.Config.Num - 1)
			}
			Debug(dInfo, kv.gid-100, kv.me, "Get lastConfig %v", lastConfig)
			for s := 0; s < shardctrler.NShards; s++ {
				// need to request shard state
				if c.Config.Shards[s] == kv.gid && lastConfig.Shards[s] != kv.gid {
					requestGID := lastConfig.Shards[s]
					Debug(dInfo, kv.gid-100, kv.me, "GetShard %v send to G%v", s, requestGID)
					kv.data[s] = kv.clerk.GetShard(s, requestGID, lastConfig.Groups[requestGID], c.Config)
					Debug(dInfo, kv.gid-100, kv.me, "Received Shard %v from G%v", s, requestGID)
				}
			}
			kv.mu.Lock()
			kv.finishedConfig++
			kv.mu.Unlock()
		}(lastConfig)

	}
	//if v, exist := kv.data[c.Key]; exist {
	//	if c.Operator == GetOp {
	//		Debug(dSnap, kv.gid, kv.me, "Get %v = %v, index %v", c.Key, v, index)
	//	} else {
	//		Debug(dSnap, kv.gid, kv.me, "Update %v = %v, index %v", c.Key, v, index)
	//	}
	//}
	return OK
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
		_, isLeader := kv.rf.GetState()
		// only the leader need to update the configuration
		if !isLeader {
			time.Sleep(ConfigurationTimeout)
			continue
		}

		newConfig := kv.mck.Query(-1)

		//Debug(dInfo, kv.gid-100, kv.me, "try update %v", newConfig)
		// There are no others writing it, so we can copy once to avoid waiting lock
		kv.mu.Lock()
		curNum := kv.config.Num
		kv.mu.Unlock()

		if curNum >= newConfig.Num {
			time.Sleep(ConfigurationTimeout)
			continue
		}
		Debug(dInfo, kv.gid-100, kv.me, "Need to update %v", newConfig)
		// isolation from client request id
		rid := int64(-newConfig.Num)
		// may fail, but we will continue try it
		// do not update here, must update through the applyCh
		kv.Commit(Op{
			Config:    newConfig,
			Operator:  ConfigOp,
			RequestId: rid,
			LastSuc:   int64(-curNum),
		}, func() Err { return OK })
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

				err := kv.applyCommand(msg.CommandIndex, msg.Command.(Op))
				kv.commitIndex = msg.CommandIndex

				go func() { kv.dataCh <- void{} }()

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
	case GetShardOp:
		opName = "GetShard"
	}

	select {
	case <-kv.dead:
		Debug(dClean, kv.gid, kv.me, "Killed")
	case realIdErr := <-waitCh:
		if realIdErr.Err != OK {
			Debug(dInfo, kv.gid-100, kv.me, opName+" wrong group %v", specId)
			return realIdErr.Err
		}
		if realIdErr.ID != specId {
			Debug(dInfo, kv.gid-100, kv.me, opName+" canceled spec: %v, real: %v", specId, realIdErr.ID)
			return ErrWrongLeader
		}
		if err := postFunc(); err != OK {
			return err
		}
		Debug(dInfo, kv.gid-100, kv.me, opName+" ok %v", specId)
		return OK
	case <-timeoutCh(waitTimeout):
		Debug(dInfo, kv.gid-100, kv.me, opName+" timeout")
	}
	return ErrWrongLeader
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	//Debug(dInfo, kv.gid-100, kv.me, "GetShard %+v", args)
	//defer Debug(dInfo, kv.gid-100, kv.me, "GetShard reply %+v", reply)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if args.Config.Num <= kv.config.Num {
		kv.mu.Unlock()
		<-kv.dataCh
		reply.Data = make(map[string]string)
		for k, v := range kv.data[args.Shard] {
			reply.Data[k] = v
		}
		go func() { kv.dataCh <- void{} }()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	reply.Err = kv.Commit(Op{
		Shard:     args.Shard,
		Config:    args.Config,
		Operator:  GetShardOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err {
		<-kv.dataCh
		reply.Data = make(map[string]string)
		for k, v := range kv.data[args.Shard] {
			reply.Data[k] = v
		}
		go func() { kv.dataCh <- void{} }()
		return OK
	})
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//Debug(dInfo, kv.gid-100, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.gid-100, kv.me, "Get reply %+v", reply)
	reply.Err = kv.Commit(Op{
		Key:       args.Key,
		Operator:  GetOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err {
		shard := key2shard(args.Key)
		kv.mu.Lock()
		if kv.config.Num > kv.finishedConfig {
			kv.mu.Unlock()
			return ErrWrongLeader
		}
		specGID := kv.config.Shards[shard]
		kv.mu.Unlock()
		if specGID != kv.gid {
			return ErrWrongGroup
		}
		<-kv.dataCh
		v, exist := kv.data[shard][args.Key]
		go func() { kv.dataCh <- void{} }()
		if !exist {
			return ErrNoKey
		}
		reply.Value = v
		return OK
	})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//Debug(dInfo, kv.gid-100, kv.me, "PutAppend %+v", args)
	//defer Debug(dInfo, kv.gid-100, kv.me, "PutAppend reply %+v", reply)
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

	reply.Err = kv.Commit(Op{
		Key:       args.Key,
		Value:     args.Value,
		Operator:  Operator,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err { return OK })
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
	kv.clerk = MakeClerk(ctrlers, make_end)

	kv.appliedButNotReceived = make(map[int64]void)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dataCh = make(chan void)
	for s := range kv.data {
		kv.data[s] = make(map[string]string)
	}
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = make(chan void)
	kv.snapshotLastIndex = 0
	kv.commitIndex = 0

	go func() { kv.dataCh <- void{} }()

	kv.readPersist(kv.persister.ReadSnapshot())

	Debug(dInfo, kv.gid-100, kv.me, "Restarted with lastIndex %v", kv.snapshotLastIndex)

	go kv.waiting()
	go kv.compareSnapshot()
	go kv.cleaner()
	go kv.updateConfig()

	return kv
}
