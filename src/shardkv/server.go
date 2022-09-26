package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
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
	UpdateDataOp
)

const (
	RequestWaitTimeout   = 300 * time.Millisecond
	ConfigurationTimeout = 100 * time.Millisecond
	fetchShardTimeout    = 100 * time.Millisecond
)

type Op struct {
	Key       string
	Value     string
	Operator  int
	RequestId int64
	LastSuc   int64
	// for Internal UpdateConfig
	Config shardctrler.Config
	// for GetShard and UpdateData
	ConfigNum int
	Shard     int
	// for UpdateData
	Data map[string]string
	Dup  map[int64]void
}

type ShardKV struct {
	//mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clerk       *Clerk
	configCh    chan void
	config      shardctrler.Config
	configCache sync.Map
	dead        chan void
	mckCh       chan void
	mck         *shardctrler.Clerk
	// each index allocate a channel to inform the waiting request
	// map[specIndex int]committed RequestId in spec index, chan int64
	notification sync.Map
	// for duplicate apply detection: applied to state machine but haven't received by client yet
	// map[requestId int64]void
	appliedButNotReceived [shardctrler.NShards]map[int]map[int64]void
	persister             *raft.Persister
	// data: responsible data in "history configuration"
	// data[shard][configNum] -> data
	dataCh chan void
	data   [shardctrler.NShards]map[int]map[string]string
	// protected by dataCh
	snapshotLastIndex int
	commitIndex       int
	finishedConfig    int
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
			// There are no others writing it, so we can copy once to avoid waiting lock
			<-kv.configCh
			if kv.config.Num >= newConfig.Num {
				go func() { kv.configCh <- void{} }()
				continue
			}

			//Debug(dInfo, kv.gid-100, "Need to update %v", newConfig)
			if servers, exist := newConfig.Groups[kv.gid]; exist {
				go func() { kv.configCh <- void{} }()
				kv.clerk.UpdateConfig(kv.gid, newConfig, servers)
			} else if servers, exist := kv.config.Groups[kv.gid]; exist {
				go func() { kv.configCh <- void{} }()
				kv.clerk.UpdateConfig(kv.gid, newConfig, servers)
			} else {
				go func() { kv.configCh <- void{} }()
			}
		}
	}
}

func (kv *ShardKV) UpdateConfig(args *UpdateConfigArgs, reply *UpdateConfigReply) {
	reply.Err = kv.Commit(Op{
		Config:    args.Config,
		Operator:  ConfigOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err { return OK })
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	//Debug(dInfo, kv.gid-100, "GetShard %+v", args)
	//defer Debug(dInfo, kv.gid-100, "GetShard reply %+v", reply)
	reply.Err = kv.Commit(Op{
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
		Operator:  GetShardOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err {
		<-kv.configCh
		configNum := kv.config.Num
		go func() { kv.configCh <- void{} }()

		if configNum < args.ConfigNum {
			Debug(dSnap, kv.gid-100, "GetShardOp I still in C%v, you ask me about C%v", configNum, args.ConfigNum)
			return ErrNoResponsibility
		}

		<-kv.dataCh
		defer func() { go func() { kv.dataCh <- void{} }() }()

		data, exist := kv.data[args.Shard][args.ConfigNum]
		dup := kv.appliedButNotReceived[args.Shard][args.ConfigNum]
		if !exist {
			if configNum > args.ConfigNum {
				Debug(dSnap, kv.gid-100, "GetShardOp data not found S%v", args.Shard)
				return ErrNoResponsibility
			}
			// when finished, this configNum must have state (by this commit, or updateConfig commit)
			// if not, try again
			Debug(dSnap, kv.gid-100, "GetShardOp data not found S%v, but is about to appear", args.Shard)
			return ErrWrongLeader
		}
		reply.Data = mapCopy(data)
		reply.Dup = mapCopy(dup)
		return OK
	})
}

func (kv *ShardKV) UpdateData(args *UpdateDataArgs, reply *UpdateDataReply) {
	//Debug(dInfo, kv.gid-100, "GetShard %+v", args)
	//defer Debug(dInfo, kv.gid-100, "GetShard reply %+v", reply)
	reply.Err = kv.Commit(Op{
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
		Data:      mapCopy(args.Data),
		Dup:       mapCopy(args.Dup),
		Operator:  UpdateDataOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err { return OK })
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//Debug(dInfo, kv.gid-100, "Get %+v", args)
	//defer Debug(dInfo, kv.gid-100, "Get reply %+v", reply)
	reply.Err = kv.Commit(Op{
		Key:       args.Key,
		Operator:  GetOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() Err {
		shard := key2shard(args.Key)
		<-kv.configCh
		defer func() { go func() { kv.configCh <- void{} }() }()
		<-kv.dataCh
		defer func() { go func() { kv.dataCh <- void{} }() }()

		specGID := kv.config.Shards[shard]
		if specGID != kv.gid {
			return ErrWrongGroup
		}

		m, exist := kv.data[shard][kv.config.Num]
		if !exist {
			// responsible shard but state not ready
			return ErrWrongLeader
		}

		v, exist := m[args.Key]
		if !exist {
			return ErrNoKey
		}
		reply.Value = v
		return OK
	})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//Debug(dInfo, kv.gid-100, "PutAppend %+v", args)
	//defer Debug(dInfo, kv.gid-100, "PutAppend reply %+v", reply)
	Operator := -1
	switch args.Op {
	case "Put":
		Operator = PutOp
	case "Append":
		Operator = AppendOp
	default:
		Debug(dError, kv.gid-100, "ERROR PutAppend unknown op: %v", args.Op)
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
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.clerk = MakeClerk(ctrlers, make_end)

	for s := range kv.data {
		kv.appliedButNotReceived[s] = make(map[int]map[int64]void)
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dataCh = make(chan void)
	kv.configCh = make(chan void)
	kv.mckCh = make(chan void)
	for s := range kv.data {
		kv.data[s] = make(map[int]map[string]string)
	}
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = make(chan void)
	kv.snapshotLastIndex = 0
	kv.commitIndex = 0

	go func() { kv.mckCh <- void{} }()
	go func() { kv.configCh <- void{} }()
	go func() { kv.dataCh <- void{} }()

	kv.readPersist(kv.persister.ReadSnapshot())

	Debug(dInfo, kv.gid-100, "Restarted with lastIndex %v", kv.snapshotLastIndex)
	//Debug(dInfo, kv.gid-100, "Restarted with data %v", kv.data)
	//Debug(dInfo, kv.gid-100, "Restarted with appliedButNotReceived %v", kv.appliedButNotReceived)

	go kv.waiting()
	go kv.compareSnapshot()
	go kv.cleaner()
	go kv.updateConfig()
	go kv.fetchShard()

	return kv
}
