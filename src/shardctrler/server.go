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

func (sc *ShardCtrler) balance(groups map[int][]string, before [NShards]int) (after [NShards]int) {
	if len(groups) == 0 {
		return
	}

	// deterministic
	var gids []int
	for k := range groups {
		gids = append(gids, k)
	}
	sort.Ints(gids)

	canHold := make(map[int]int)
	N := NShards
	NG := len(gids)

	for _, gid := range gids {
		canHold[gid] = N / NG
		N -= canHold[gid]
		NG -= 1
	}

	valid := 0
	for _, h := range canHold {
		valid += h
	}
	if valid != NShards {
		panic("balance error: canHold count not equal to NShards")
	}

	for s := 0; s < NShards; s++ {
		if canHold[before[s]] > 0 {
			after[s] = before[s]
			canHold[before[s]] -= 1
		}
	}

	for s := 0; s < NShards; s++ {
		if after[s] == 0 {
			for _, gid := range gids {
				if canHold[gid] > 0 {
					after[s] = gid
					canHold[gid] -= 1
					break
				}
			}
		}
		if after[s] == 0 {
			panic("balance error: some shard can't allocate to RG")
		}
	}
	return
}

func mapCopy(m map[int][]string) (res map[int][]string) {
	res = make(map[int][]string)
	for k, v := range m {
		res[k] = v
	}
	return
}

func listCopy(l [NShards]int) (res [NShards]int) {
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
				Shards: sc.balance(c.Servers, lastConfig.Shards),
				Groups: c.Servers,
			}
			sc.configs = append(sc.configs, newConfig)
			return
		}
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: mapCopy(lastConfig.Groups),
		}
		for gid, servers := range c.Servers {
			newConfig.Groups[gid] = servers
		}
		newConfig.Shards = sc.balance(newConfig.Groups, lastConfig.Shards)
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
		newConfig.Shards = sc.balance(newConfig.Groups, lastConfig.Shards)
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

func (sc *ShardCtrler) Commit(command Op, postFunc func()) (WrongLeader bool) {
	//Debug(dInfo, sc.me, "Get %+v", args)
	//defer Debug(dInfo, sc.me, "Get reply %+v", reply)
	specId := command.RequestId
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		return true
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

	var opName string
	switch command.Operator {
	case joinOp:
		opName = "Join"
	case LeaveOp:
		opName = "Leave"
	case QueryOp:
		opName = "Query"
	case MoveOp:
		opName = "Move"
	}

	select {
	case <-sc.dead:
		Debug(dInfo, sc.me, "Killed")
	case realId := <-waitCh:
		if realId != specId {
			Debug(dInfo, sc.me, opName+" canceled spec: %v, real: %v", specId, realId)
			return true
		}
		postFunc()
		Debug(dInfo, sc.me, opName+" ok %v", specId)
		return false
	case <-timeoutCh(waitTimeout):
		Debug(dInfo, sc.me, opName+" timeout")
	}
	return true
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	//Debug(dInfo, sc.me, "Get %+v", args)
	//defer Debug(dInfo, sc.me, "Get reply %+v", reply)
	reply.WrongLeader = sc.Commit(Op{
		Servers:   args.Servers,
		Operator:  joinOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() {})
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	//Debug(dInfo, sc.me, "Leave %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	reply.WrongLeader = sc.Commit(Op{
		GIDs:      args.GIDs,
		Operator:  LeaveOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() {})
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	reply.WrongLeader = sc.Commit(Op{
		Shard:     args.Shard,
		GID:       args.GID,
		Operator:  MoveOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() {})
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	//Debug(dInfo, kv.me, "Get %+v", args)
	//defer Debug(dInfo, kv.me, "Get reply %+v", reply)
	reply.WrongLeader = sc.Commit(Op{
		Num:       args.Num,
		Operator:  QueryOp,
		RequestId: args.RequestId,
		LastSuc:   args.LastSuc,
	}, func() {
		sc.mu.Lock()
		if args.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
			if reply.Config.Num != args.Num {
				panic("Number wrong!!!")
			}
		}
		sc.mu.Unlock()
	})
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
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
