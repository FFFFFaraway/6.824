package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	TryServerTimeout = 100 * time.Millisecond
)

type Clerk struct {
	// read only
	servers []*labrpc.ClientEnd
	leader  chan int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = make(chan int)
	go func() { ck.leader <- 0 }()
	return ck
}

func (ck *Clerk) nextServer(server int) int {
	return (server + 1) % len(ck.servers)
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	reply := &GetReply{}
	for {
		try := <-ck.leader
		go func() { ck.leader <- try }()

		ok := ck.servers[try].Call("KVServer.Get", &GetArgs{key}, reply)
		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
			}
		}
		// no others modify it
		if try == <-ck.leader {
			go func() { ck.leader <- ck.nextServer(try) }()
		}
		time.Sleep(TryServerTimeout)
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reply := PutAppendReply{}
	rid := nrand()
	for {
		try := <-ck.leader
		go func() { ck.leader <- try }()

		ok := ck.servers[try].Call("KVServer.PutAppend", &PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestId: rid,
		}, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
			}
		}
		// no others modify it
		if try == <-ck.leader {
			go func() { ck.leader <- ck.nextServer(try) }()
		}
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
