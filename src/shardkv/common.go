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
	// ErrRetryLater in GetShard to avoid the deadlock
	ErrRetryLater = "ErrRetryLater"
	ErrDeleted    = "ErrDeleted"
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
	ConfigNum int
	RequestId int64
	LastSuc   int64
}

type GetShardReply struct {
	Err  Err
	Data map[string]string
	Dup  map[int64]void
}

type UpdateDataArgs struct {
	Shard     int
	ConfigNum int
	Data      map[string]string
	Dup       map[int64]void
	RequestId int64
	LastSuc   int64
}

type UpdateDataReply struct {
	Err Err
}

type UpdateConfigArgs struct {
	Config    shardctrler.Config
	RequestId int64
	LastSuc   int64
}

type UpdateConfigReply struct {
	Err Err
}

type DeleteBeforeArgs struct {
	Shard     int
	ConfigNum int
	RequestId int64
	LastSuc   int64
}

type DeleteBeforeReply struct {
	Err Err
}
