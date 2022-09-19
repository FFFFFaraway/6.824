package shardctrler

import (
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	dead                  chan void
	notification          sync.Map
	appliedButNotReceived map[int64]void
}

const (
	joinOp = iota
	LeaveOp
	QueryOp
	MoveOp
)

type Op struct {
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Query
	Num int
	// Move
	Shard int
	GID   int
	// All
	Operator  int
	RequestId int64
	LastSuc   int64
}

const (
	waitTimeout = 500 * time.Millisecond
)

func (sc *ShardCtrler) balance(groups map[int][]string) (shards [10]int) {
	if len(groups) == 0 {
		return
	}

	// deterministic
	var keys []int
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	i := 0
	for {
		for _, gid := range keys {
			shards[i] = gid
			i++
			if i >= NShards {
				return
			}
		}
	}
}

func mapCopy(m map[int][]string) (res map[int][]string) {
	res = make(map[int][]string)
	for k, v := range m {
		res[k] = v
	}
	return
}

func listCopy(l [10]int) (res [10]int) {
	for i := range l {
		res[i] = l[i]
	}
	return res
}

func (sc *ShardCtrler) applyCommand(index int, c Op) {
	//Debug(dApply, sc.me, "Apply Command %+v", c)

	delete(sc.appliedButNotReceived, c.LastSuc)
	_, exist := sc.appliedButNotReceived[c.RequestId]
	if exist {
		return
	}
	sc.appliedButNotReceived[c.RequestId] = void{}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	switch c.Operator {
	case joinOp:
		lastConfig := sc.configs[len(sc.configs)-1]
		if len(lastConfig.Groups) == 0 {
			newConfig := Config{
				Num:    lastConfig.Num + 1,
				Shards: sc.balance(c.Servers),
				Groups: c.Servers,
			}
			sc.configs = append(sc.configs, newConfig)
			return
		}
		//low := NShards / len(lastConfig.Groups)
		//high := low
		//if NShards%len(lastConfig.Groups) != 0 {
		//	high++
		//}
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: mapCopy(lastConfig.Groups),
		}
		for gid, servers := range c.Servers {
			newConfig.Groups[gid] = servers
		}
		newConfig.Shards = sc.balance(newConfig.Groups)
		sc.configs = append(sc.configs, newConfig)
	case LeaveOp:
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: mapCopy(lastConfig.Groups),
		}
		for _, gid := range c.GIDs {
			delete(newConfig.Groups, gid)
		}
		newConfig.Shards = sc.balance(newConfig.Groups)
		sc.configs = append(sc.configs, newConfig)
	case MoveOp:
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Shards: listCopy(lastConfig.Shards),
			Groups: mapCopy(lastConfig.Groups),
		}
		newConfig.Num = lastConfig.Num + 1
		newConfig.Shards[c.Shard] = c.GID
		sc.configs = append(sc.configs, newConfig)
	}
}

func (sc *ShardCtrler) waiting() {
	for {
		select {
		case <-sc.dead:
			return
		case msg := <-sc.applyCh:
			sc.applyCommand(msg.CommandIndex, msg.Command.(Op))
			Debug(dInfo, sc.me, "%v", sc.configs)
			// after command have been applied, then add notification
			waitChInter, exist := sc.notification.Load(msg.CommandIndex)
			// request timeout (notification deleted), but command finally applied
			// or the server is follower, no request is waiting
			if exist {
				// if existed, there must be someone waiting for it
				waitChInter.(chan int64) <- msg.Command.(Op).RequestId
			}
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	//Debug(dInfo, sc.me, "Get %+v", args)
	//defer Debug(dInfo, sc.me, "Get reply %+v", reply)
	specId := args.RequestId
	index, _, isLeader := sc.rf.Start(Op{
		Servers:   args.Servers,
		Operator:  joinOp,
		RequestId: specId,
		LastSuc:   args.LastSuc,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	waitCh := make(chan int64)
	sc.notification.Store(index, waitCh)
	defer func() {
		// delete first
		sc.notification.Delete(index)
		// it's possible that the notification has been loaded, but haven't sent signal yet
		// need to wait again
		select {
		case id := <-waitCh:
			Debug(dInfo, sc.me, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	select {
	case <-sc.dead:
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			reply.WrongLeader = true
			Debug(dInfo, sc.me, "Join canceled spec: %v, real: %v", specId, realId)
			return
		}
		reply.Err = OK
		reply.WrongLeader = false
		Debug(dInfo, sc.me, "Join ok %v", specId)
	case <-timeoutCh(waitTimeout):
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Join timeout")
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	//Debug(dInfo, sc.me, "Leave %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	specId := args.RequestId
	index, _, isLeader := sc.rf.Start(Op{
		GIDs:      args.GIDs,
		Operator:  LeaveOp,
		RequestId: specId,
		LastSuc:   args.LastSuc,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	waitCh := make(chan int64)
	sc.notification.Store(index, waitCh)
	defer func() {
		// delete first
		sc.notification.Delete(index)
		// it's possible that the notification has been loaded, but haven't sent signal yet
		// need to wait again
		select {
		case id := <-waitCh:
			Debug(dInfo, sc.me, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	select {
	case <-sc.dead:
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			reply.WrongLeader = true
			Debug(dInfo, sc.me, "Leave canceled spec: %v, real: %v", specId, realId)
			return
		}
		reply.Err = OK
		Debug(dInfo, sc.me, "Leave ok %v", specId)
	case <-timeoutCh(waitTimeout):
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Leave timeout")
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	specId := args.RequestId
	index, _, isLeader := sc.rf.Start(Op{
		Shard:     args.Shard,
		GID:       args.GID,
		Operator:  MoveOp,
		RequestId: specId,
		LastSuc:   args.LastSuc,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	waitCh := make(chan int64)
	sc.notification.Store(index, waitCh)
	defer func() {
		// delete first
		sc.notification.Delete(index)
		// it's possible that the notification has been loaded, but haven't sent signal yet
		// need to wait again
		select {
		case id := <-waitCh:
			Debug(dInfo, sc.me, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	select {
	case <-sc.dead:
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			reply.WrongLeader = true
			Debug(dInfo, sc.me, "Move canceled spec: %v, real: %v", specId, realId)
			return
		}
		reply.Err = OK
		Debug(dInfo, sc.me, "Move ok %v", specId)
	case <-timeoutCh(waitTimeout):
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Move timeout")
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	specId := args.RequestId
	index, _, isLeader := sc.rf.Start(Op{
		Num:       args.Num,
		Operator:  QueryOp,
		RequestId: specId,
		LastSuc:   args.LastSuc,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	waitCh := make(chan int64)
	sc.notification.Store(index, waitCh)
	defer func() {
		// delete first
		sc.notification.Delete(index)
		// it's possible that the notification has been loaded, but haven't sent signal yet
		// need to wait again
		select {
		case id := <-waitCh:
			Debug(dInfo, sc.me, "delete index %v with request id %v", index, id)
		default:
		}
	}()

	select {
	case <-sc.dead:
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			reply.WrongLeader = true
			Debug(dInfo, sc.me, "Query canceled spec: %v, real: %v", specId, realId)
			return
		}
		sc.mu.Lock()
		if args.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
		reply.Err = OK
		Debug(dInfo, sc.me, "Query ok %v", specId)
	case <-timeoutCh(waitTimeout):
		reply.WrongLeader = true
		Debug(dInfo, sc.me, "Query timeout")
	}
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	ensureClosed(sc.dead)
	sc.rf.Kill()
	// Your code here, if desired.
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = make(chan void)
	sc.appliedButNotReceived = make(map[int64]void)
	go sc.waiting()

	return sc
}
