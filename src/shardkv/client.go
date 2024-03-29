package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

const (
	TryServerTimeout = 100 * time.Millisecond
	TryGroupTimeout  = 100 * time.Millisecond
	InternalTimeout  = 10 * time.Millisecond
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

	leader  sync.Map // gid -> last leader index
	lastSuc sync.Map // gid -> lastSuc
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
					leaderIndex := 0
					inter, exist := ck.leader.Load(gid)
					if !exist || leaderIndex >= len(servers) {
						ck.leader.Store(gid, 0)
					} else {
						leaderIndex = inter.(int)
					}

					lastSuc := int64(0)
					inter, exist = ck.lastSuc.Load(gid)
					if !exist {
						ck.lastSuc.Store(gid, int64(0))
					} else {
						lastSuc = inter.(int64)
					}
					reply := &GetReply{}
					if ck.make_end(servers[leaderIndex]).Call("ShardKV.Get", &GetArgs{
						Key:       key,
						RequestId: rid,
						LastSuc:   lastSuc,
					}, &reply) {
						if reply.Err == OK || reply.Err == ErrNoKey {
							ck.lastSuc.Store(gid, rid)
							return reply.Value
						}
						if reply.Err == ErrWrongGroup {
							break
						}
					}
					ck.leader.Store(gid, nextServer(leaderIndex, len(servers)))
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
					leaderIndex := 0
					inter, exist := ck.leader.Load(gid)
					if !exist || leaderIndex >= len(servers) {
						ck.leader.Store(gid, 0)
					} else {
						leaderIndex = inter.(int)
					}

					lastSuc := int64(0)
					inter, exist = ck.lastSuc.Load(gid)
					if !exist {
						ck.lastSuc.Store(gid, int64(0))
					} else {
						lastSuc = inter.(int64)
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
							ck.lastSuc.Store(gid, rid)
							return
						}
						if reply.Err == ErrWrongGroup {
							break
						}
					}
					ck.leader.Store(gid, nextServer(leaderIndex, len(servers)))
					time.Sleep(TryServerTimeout)
				}
			}
		}
		time.Sleep(TryGroupTimeout)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) GetShard(shard, gid, configNum int, servers []string) (map[string]string, map[int64]void, Err) {
	rid := nrand()
	for {
		leaderIndex := 0
		inter, exist := ck.leader.Load(gid)
		if !exist || leaderIndex >= len(servers) {
			ck.leader.Store(gid, 0)
		} else {
			leaderIndex = inter.(int)
		}

		lastSuc := int64(0)
		inter, exist = ck.lastSuc.Load(gid)
		if !exist {
			ck.lastSuc.Store(gid, int64(0))
		} else {
			lastSuc = inter.(int64)
		}
		reply := &GetShardReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.GetShard", &GetShardArgs{
			Shard:     shard,
			ConfigNum: configNum,
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc.Store(gid, rid)
				return reply.Data, reply.Dup, reply.Err
			}
		}
		ck.leader.Store(gid, nextServer(leaderIndex, len(servers)))
		time.Sleep(InternalTimeout)
	}
}

func (ck *Clerk) UpdateData(shard, gid, configNum int, servers []string, data map[string]string, dup map[int64]void) {
	rid := nrand()
	for {
		leaderIndex := 0
		inter, exist := ck.leader.Load(gid)
		if !exist || leaderIndex >= len(servers) {
			ck.leader.Store(gid, 0)
		} else {
			leaderIndex = inter.(int)
		}

		lastSuc := int64(0)
		inter, exist = ck.lastSuc.Load(gid)
		if !exist {
			ck.lastSuc.Store(gid, int64(0))
		} else {
			lastSuc = inter.(int64)
		}
		reply := &UpdateDataReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.UpdateData", &UpdateDataArgs{
			Shard:     shard,
			ConfigNum: configNum,
			Data:      mapCopy(data),
			Dup:       mapCopy(dup),
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc.Store(gid, rid)
				return
			}
		}
		ck.leader.Store(gid, nextServer(leaderIndex, len(servers)))
		time.Sleep(InternalTimeout)
	}
}

func (ck *Clerk) DeleteBefore(shard, gid, configNum int, servers []string) {
	rid := nrand()
	for {
		leaderIndex := 0
		inter, exist := ck.leader.Load(gid)
		if !exist || leaderIndex >= len(servers) {
			ck.leader.Store(gid, 0)
		} else {
			leaderIndex = inter.(int)
		}

		lastSuc := int64(0)
		inter, exist = ck.lastSuc.Load(gid)
		if !exist {
			ck.lastSuc.Store(gid, int64(0))
		} else {
			lastSuc = inter.(int64)
		}
		reply := &DeleteBeforeReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.DeleteBefore", &DeleteBeforeArgs{
			Shard:     shard,
			ConfigNum: configNum,
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc.Store(gid, rid)
				return
			}
		}
		ck.leader.Store(gid, nextServer(leaderIndex, len(servers)))
		time.Sleep(InternalTimeout)
	}
}

func (ck *Clerk) UpdateConfig(gid int, config shardctrler.Config, servers []string) {
	rid := nrand()
	for {
		leaderIndex := 0
		inter, exist := ck.leader.Load(gid)
		if !exist || leaderIndex >= len(servers) {
			ck.leader.Store(gid, 0)
		} else {
			leaderIndex = inter.(int)
		}

		lastSuc := int64(0)
		inter, exist = ck.lastSuc.Load(gid)
		if !exist {
			ck.lastSuc.Store(gid, int64(0))
		} else {
			lastSuc = inter.(int64)
		}
		reply := &UpdateConfigReply{}
		if ck.make_end(servers[leaderIndex]).Call("ShardKV.UpdateConfig", &UpdateConfigArgs{
			Config:    config,
			RequestId: rid,
			LastSuc:   lastSuc,
		}, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.lastSuc.Store(gid, rid)
				return
			}
		}
		ck.leader.Store(gid, nextServer(leaderIndex, len(servers)))
		time.Sleep(InternalTimeout)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
