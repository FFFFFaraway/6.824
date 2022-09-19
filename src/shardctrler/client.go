package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

const (
	TryServerTimeout = 100 * time.Millisecond
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// clerk doesn't have many goroutines
	leader  int
	lastSuc int64
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
	// Your code here.
	ck.leader = 0
	ck.lastSuc = 0
	return ck
}

func (ck *Clerk) nextServer(server int) int {
	return (server + 1) % len(ck.servers)
}

func (ck *Clerk) Query(num int) Config {
	rid := nrand()
	for {
		reply := &QueryReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Query", &QueryArgs{
			Num:       num,
			RequestId: rid,
			LastSuc:   ck.lastSuc,
		}, reply)
		if ok && !reply.WrongLeader {
			ck.lastSuc = rid
			return reply.Config
		}
		// there must no others modify it
		ck.leader = ck.nextServer(ck.leader)
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	rid := nrand()
	for {
		reply := &JoinReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Join", &JoinArgs{
			Servers:   servers,
			RequestId: rid,
			LastSuc:   ck.lastSuc,
		}, reply)
		if ok && !reply.WrongLeader {
			ck.lastSuc = rid
			return
		}
		// there must no others modify it
		ck.leader = ck.nextServer(ck.leader)
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) Leave(gids []int) {
	rid := nrand()
	for {
		reply := &LeaveReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", &LeaveArgs{
			GIDs:      gids,
			RequestId: rid,
			LastSuc:   ck.lastSuc,
		}, reply)
		if ok && !reply.WrongLeader {
			ck.lastSuc = rid
			return
		}
		// there must no others modify it
		ck.leader = ck.nextServer(ck.leader)
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	rid := nrand()
	for {
		reply := &MoveReply{}
		ok := ck.servers[ck.leader].Call("ShardCtrler.Move", &MoveArgs{
			Shard:     shard,
			GID:       gid,
			RequestId: rid,
			LastSuc:   ck.lastSuc,
		}, reply)
		if ok && !reply.WrongLeader {
			ck.lastSuc = rid
			return
		}
		// there must no others modify it
		ck.leader = ck.nextServer(ck.leader)
		time.Sleep(TryServerTimeout)
	}
}
