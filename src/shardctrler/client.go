package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seqNum   int64
	leaderId int32
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
	ck.clientId = nrand()

	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.SeqNum = atomic.AddInt64(&ck.seqNum, 1)
	srvId := ck.leaderId
	for {
		// try each known server.
		srv := ck.servers[srvId]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		if ok {
			SdcPrintf("Query %s -------------\n %v", reply.Err, reply.Config)
		}
		if ok && !reply.WrongLeader {
			ck.leaderId = srvId
			if reply.Err == OK {
				return reply.Config
			}
		} else {
			srvId = (srvId + 1) % int32(len(ck.servers))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqNum = atomic.AddInt64(&ck.seqNum, 1)
	srvId := ck.leaderId
	for {
		// try each known server.
		srv := ck.servers[srvId]
		// for _, srv := range ck.servers {
		var reply JoinReply
		ok := srv.Call("ShardCtrler.Join", args, &reply)
		if ok {
			SdcPrintf("Join %s", reply.Err)
		}
		if ok && !reply.WrongLeader {
			ck.leaderId = srvId
			if reply.Err == OK || reply.Err == Duplicate {
				return
			}
		} else {
			srvId = (srvId + 1) % int32(len(ck.servers))
		}
		// }
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.SeqNum = atomic.AddInt64(&ck.seqNum, 1)
	srvId := ck.leaderId
	for {
		// try each known server.
		srv := ck.servers[srvId]
		// for _, srv := range ck.servers {
		var reply LeaveReply
		ok := srv.Call("ShardCtrler.Leave", args, &reply)
		if ok {
			SdcPrintf("Leave %s", reply.Err)
		}
		if ok && !reply.WrongLeader {
			ck.leaderId = srvId
			if reply.Err == OK || reply.Err == Duplicate {
				return
			}
		} else {
			srvId = (srvId + 1) % int32(len(ck.servers))
		}
		// }
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.SeqNum = atomic.AddInt64(&ck.seqNum, 1)
	srvId := ck.leaderId
	for {
		// try each known server.
		srv := ck.servers[srvId]
		// for _, srv := range ck.servers {
		var reply MoveReply
		ok := srv.Call("ShardCtrler.Move", args, &reply)
		if ok {
			SdcPrintf("Move %s", reply.Err)
		}
		if ok && !reply.WrongLeader {
			ck.leaderId = srvId
			if reply.Err == OK || reply.Err == Duplicate {
				return
			}
		} else {
			srvId = (srvId + 1) % int32(len(ck.servers))
		}
		// }
		time.Sleep(100 * time.Millisecond)
	}
}
