package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

const (
	TryServerTimeout = 100 * time.Millisecond
	TryGroupTimeout  = 100 * time.Millisecond
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leader  map[int]int   // gid -> last leader index
	lastSuc map[int]int64 // gid -> lastSuc
}

func nextServer(server, groupSize int) int {
	return (server + 1) % groupSize
}

// MakeClerk
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leader = make(map[int]int)
	ck.lastSuc = make(map[int]int64)
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	rid := nrand()
	for {
		gid := ck.config.Shards[shard]
		if gid != 0 {
			if servers, ok := ck.config.Groups[gid]; ok {
				for {
					leaderIndex, exist := ck.leader[gid]
					if !exist || leaderIndex >= len(servers) {
						ck.leader[gid] = 0
						leaderIndex = 0
					}
					lastSuc := ck.lastSuc[gid]
					if !exist {
						ck.lastSuc[gid] = 0
						lastSuc = 0
					}
					reply := &GetReply{}
					if ck.make_end(servers[leaderIndex]).Call("ShardKV.Get", &GetArgs{
						Key:       key,
						RequestId: rid,
						LastSuc:   lastSuc,
					}, &reply) {
						if reply.Err == OK || reply.Err == ErrNoKey {
							ck.lastSuc[gid] = rid
							return reply.Value
						}
						if reply.Err == ErrWrongGroup {
							break
						}
					}
					ck.leader[gid] = nextServer(leaderIndex, len(servers))
					time.Sleep(TryServerTimeout)
				}
			}
		}
		time.Sleep(TryGroupTimeout)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	rid := nrand()
	for {
		gid := ck.config.Shards[shard]
		if gid != 0 {
			if servers, ok := ck.config.Groups[gid]; ok {
				for {
					leaderIndex, exist := ck.leader[gid]
					if !exist || leaderIndex >= len(servers) {
						ck.leader[gid] = 0
						leaderIndex = 0
					}
					lastSuc := ck.lastSuc[gid]
					if !exist {
						ck.lastSuc[gid] = 0
						lastSuc = 0
					}
					reply := &PutAppendReply{}
					if ck.make_end(servers[leaderIndex]).Call("ShardKV.PutAppend", &PutAppendArgs{
						Key:       key,
						Value:     value,
						Op:        op,
						RequestId: rid,
						LastSuc:   lastSuc,
					}, &reply) {
						if reply.Err == OK {
							ck.lastSuc[gid] = rid
							return
						}
						if reply.Err == ErrWrongGroup {
							break
						}
					}
					ck.leader[gid] = nextServer(leaderIndex, len(servers))
					time.Sleep(TryServerTimeout)
				}
			}
		}
		time.Sleep(TryGroupTimeout)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) GetShard(shard, gid, configNum int, servers []string) (map[string]string, Err) {
	rid := nrand()
	for {
		leaderIndex, exist := ck.leader[gid]
		if !exist || leaderIndex >= len(servers) {
			ck.leader[gid] = 0
			leaderIndex = 0
		}
		lastSuc := ck.lastSuc[gid]
		if !exist {
			ck.lastSuc[gid] = 0
			lastSuc = 0
		}
		reply := &GetShardReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.GetShard", &GetShardArgs{
			Shard:     shard,
			ConfigNum: configNum,
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc[gid] = rid
				return reply.Data, reply.Err
			}
		}
		ck.leader[gid] = nextServer(leaderIndex, len(servers))
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) UpdateData(shard, gid, configNum int, servers []string, data map[string]string) {
	rid := nrand()
	for {
		leaderIndex, exist := ck.leader[gid]
		if !exist || leaderIndex >= len(servers) {
			ck.leader[gid] = 0
			leaderIndex = 0
		}
		lastSuc := ck.lastSuc[gid]
		if !exist {
			ck.lastSuc[gid] = 0
			lastSuc = 0
		}
		reply := &UpdateDataReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.UpdateData", &UpdateDataArgs{
			Shard:     shard,
			ConfigNum: configNum,
			Data:      data,
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc[gid] = rid
				return
			}
		}
		ck.leader[gid] = nextServer(leaderIndex, len(servers))
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) UpdateConfig(gid int, config shardctrler.Config, servers []string) {
	rid := nrand()
	for {
		leaderIndex, exist := ck.leader[gid]
		if !exist || leaderIndex >= len(servers) {
			ck.leader[gid] = 0
			leaderIndex = 0
		}
		lastSuc := ck.lastSuc[gid]
		if !exist {
			ck.lastSuc[gid] = 0
			lastSuc = 0
		}
		reply := &UpdateConfigReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.UpdateConfig", &UpdateConfigArgs{
			Config:    config,
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc[gid] = rid
				return
			}
		}
		ck.leader[gid] = nextServer(leaderIndex, len(servers))
		time.Sleep(TryServerTimeout)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
