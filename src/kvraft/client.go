package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	seqNum   int64
	clientId int32
	// lastAck  LastReply // last ack seq num
	// mu       sync.RWMutex
}

func max(a int64, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var totalClerks int32 = 0

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0 // 初始默认是0
	ck.seqNum = 0
	ck.clientId = totalClerks
	atomic.AddInt32(&totalClerks, 1)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqNum++
	// atomic.AddInt64(&ck.seqNum, 1)
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
		// LastAck:  ck.lastAck,
	}
	rpcFunc := "KVServer.Get"
	serverId := ck.leaderId
	for {
		reply := GetReply{}
		ok := ck.servers[serverId].Call(rpcFunc, &args, &reply)
		if ok {
			var errno int
			kvPrintf("ERRINFO: client %d, server:%d , seq %d, Get %v", ck.clientId, serverId, args.SeqNum, reply.Err)
			fmt.Sscanf(string(reply.Err), "Error %d:", &errno)
			switch errno {
			case 0: // no error
				ck.leaderId = serverId

				// if args.SeqNum > ck.lastAck.seqNum {
				// 	ck.lastAck.seqNum = args.SeqNum
				// 	ck.lastAck.reply = reply
				// }
				return reply.Value
			case 3: // timeout 不用切换，否则时延会很高，导致某些测试错误
				//time.Sleep(time.Duration(250) * time.Millisecond)
			case 2: // the KVServer is not the leader
				var newLeaderId int
				fmt.Sscanf(string(reply.Err), "Error 2: Not Current Leader, try %d instead.", &newLeaderId)
				// if newLeaderId != -1 && newLeaderId < len(ck.servers) && newLeaderId != serverId {
				// 	serverId = newLeaderId
				// 	continue
				// }
				fallthrough
			// case 1: // the KVServer is killed
			// 	fallthrough
			default:
				serverId = (serverId + 1) % len(ck.servers)
			}
		} else {
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var rpcFunc string
	if op == "Put" {
		rpcFunc = "KVServer.Put"
	} else if op == "Append" {
		rpcFunc = "KVServer.Append"
	}
	// atomic.AddInt64(&ck.seqNum, 1)
	ck.seqNum++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
		// LastAck:  ck.lastAck,
	}
	serverId := ck.leaderId
	for {
		reply := PutAppendReply{} // 必须每个循环重新定义
		// args.ServerId = serverId
		ok := ck.servers[serverId].Call(rpcFunc, &args, &reply)
		if ok {
			var errno int
			kvPrintf("ERRINFO: client %d, server:%d , seq %d, PutAppend %v", ck.clientId, serverId, args.SeqNum, reply.Err)
			fmt.Sscanf(string(reply.Err), "Error %d:", &errno)
			switch errno {
			case 0: // no error
				ck.leaderId = serverId
				// if args.SeqNum > ck.lastAck.seqNum {
				// 	ck.lastAck.seqNum = args.SeqNum
				// 	ck.lastAck.reply = GetReply{}
				// }
				return
			case 3: // timeout 不用切换，否则时延会很高，导致某些测试错误
				//time.Sleep(time.Duration(50) * time.Millisecond)
			case 2: // the KVServer is not the leader
				var newLeaderId int
				fmt.Sscanf(string(reply.Err), "Error 2: Not Current Leader, try %d instead.", &newLeaderId)
				// if newLeaderId != -1 && newLeaderId < len(ck.servers) && newLeaderId != serverId {
				// 	serverId = newLeaderId // serverId和数组下标不一一对应
				// 	continue
				// }
				fallthrough
			case 1: // the KVServer is killed
				fallthrough
			default:
				serverId = (serverId + 1) % len(ck.servers)
			}
		} else {
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
