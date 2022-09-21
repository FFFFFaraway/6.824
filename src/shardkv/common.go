package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	RequestId int64
	LastSuc   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	RequestId int64
	LastSuc   int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	Shard     int
	Config    shardctrler.Config
	RequestId int64
	LastSuc   int64
}

type GetShardReply struct {
	Err  Err
	Data map[string]string
}
