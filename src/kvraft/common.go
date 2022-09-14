package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
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
