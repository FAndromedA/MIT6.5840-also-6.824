package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const kvDebug = false

func kvPrintf(format string, a ...interface{}) (n int, err error) {
	if kvDebug {
		log.Printf(format, a...)
	}
	return
}

type OpType uint8

const (
	opGet OpType = iota
	opPut
	opAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpType
	Key      string
	Value    string
	SeqNum   int64
	CLientId int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap   map[string]string
	lastAck map[int32]int64
	waitCh  map[int]chan Op
	// clientChannel
}

const timeoutLimit = 200

func (kv *KVServer) getWaitChannel(logIndex int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	waitChan, exists := kv.waitCh[logIndex]
	if !exists {
		waitChan = make(chan Op)
		kv.waitCh[logIndex] = waitChan
	}
	return waitChan
}

func (kv *KVServer) updateLastAck(lastAck int64, exists bool, opt *Op) {
	if !exists {
		kv.lastAck[int32(opt.CLientId)] = opt.SeqNum

	} else {
		lastAck = max(lastAck, opt.SeqNum)
		kv.lastAck[int32(opt.CLientId)] = lastAck
	}

}

func (kv *KVServer) handleMsgFromRaft() {
	for {
		if kv.killed() {
			return
		}
		select {
		case rfMsg := <-kv.applyCh:
			if rfMsg.CommandValid {
				index := rfMsg.CommandIndex
				kv.mu.Lock()

				// waitChan := kv.getWaitChannel(index)
				// kv.mu.Lock()
				opt := rfMsg.Command.(Op)
				//kvPrintf("((((( %d, type: %d, %s, %s, %d", opt.CLientId, opt.OpType, opt.Key, opt.Value, opt.SeqNum)
				lastAck, exists2 := kv.lastAck[int32(opt.CLientId)]
				kvPrintf("Server %d client %d, index %d, type: %v, bool: %v", kv.me, opt.CLientId, index, opt.OpType, !exists2 || lastAck < opt.SeqNum || opt.OpType == opGet)
				if !exists2 || lastAck < opt.SeqNum || opt.OpType == opGet {
					switch opt.OpType {
					case opGet:
						value, exists := kv.kvMap[opt.Key]
						if exists {
							opt.Value = value
						} else {
							opt.Value = ""
						}
						kv.updateLastAck(lastAck, exists2, &opt)
					case opPut:
						kv.kvMap[opt.Key] = opt.Value
						kv.updateLastAck(lastAck, exists2, &opt)
					case opAppend:
						kv.kvMap[opt.Key] += opt.Value
						kv.updateLastAck(lastAck, exists2, &opt)
					default:
						kv.mu.Unlock()
						continue
					}
					kvPrintf("----- client: %d,type: %d,\n key: %s,value: %s, preValue: %s,\n seq: %d", opt.CLientId, opt.OpType, opt.Key, kv.kvMap[opt.Key], opt.Value, opt.SeqNum)
				}
				//kvPrintf("))))) %d, %d, %s, %s, %d", opt.CLientId, opt.OpType, opt.Key, opt.Value, opt.SeqNum)
				waitChan, exists1 := kv.waitCh[index]
				if !exists1 { // 不存在的直接continue，不然没有接收方，后面发送会卡死
					// 并且必须在后面continue，因为先要根据日志恢复kvmap
					kv.mu.Unlock()
					continue
				}
				kv.mu.Unlock()
				waitChan <- opt
			} else if rfMsg.SnapshotValid {
				continue
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = Err(fmt.Sprintf("Error 1: The KvServer %d has been killed", kv.me))
		return
	}
	_, isLeader := kv.rf.GetRole()
	if !isLeader {
		reply.Err = Err(fmt.Sprintf("Error 2: Not Current Leader, try %d instead.", kv.rf.GetVotedFor()))
		return
	}
	kvPrintf("1: Server: %d, client: %d, seq: %d Get key:%v", kv.me, args.ClientId, args.SeqNum, args.Key)
	// kv.mu.Lock()
	// lastAck, exists := kv.lastAck[args.ClientId]
	// kv.mu.Unlock()
	// if exists && args.SeqNum <= lastAck.seqNum {
	// 	reply.Err = lastAck.reply.Err
	// 	reply.Value = lastAck.reply.Value
	// 	return
	// } // 重复Get直接返回最新的，不需要返回之前的, 肯定不能用之前保存的来改啊，因为请求的key都不一定一样
	logIndex, _, _ := kv.rf.Start(Op{OpType: opGet, Key: args.Key, SeqNum: args.SeqNum, CLientId: args.ClientId})
	kvPrintf("2: Server: %d, client: %d, seq: %d Get key:%v index:%d", kv.me, args.ClientId, args.SeqNum, args.Key, logIndex)
	waitChan := kv.getWaitChannel(logIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, logIndex)
		kv.mu.Unlock()
	}()
	timeout := time.NewTicker(time.Duration(timeoutLimit) * time.Millisecond)
	defer timeout.Stop()
	select {
	case opt := <-waitChan:
		if opt.CLientId != args.ClientId || opt.SeqNum != args.SeqNum { // 虽然等到了且index刚好相同，但是我提交的log被覆盖掉了
			// 譬如同步的是leader发过来的日志，刚好index相同
			reply.Err = Err(fmt.Sprintf("Error 2: Not Current Leader, try %d instead.", kv.rf.GetVotedFor()))
			return
		}
		reply.Value = opt.Value
		kvPrintf("Get return Server %d client %d seq %d index %d value %v", kv.me, args.ClientId, args.SeqNum, logIndex, opt.Value)
		reply.Err = Err("Error 0:")
	case <-timeout.C:
		reply.Err = Err("Error 3: timeout")
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = Err(fmt.Sprintf("Error 1: The KvServer %d has been killed", kv.me))
		return
	}
	_, isLeader := kv.rf.GetRole()
	if !isLeader {
		reply.Err = Err(fmt.Sprintf("Error 2: Not Current Leader, try %d instead.", kv.rf.GetVotedFor()))
		return
	}
	kvPrintf("1: Server %d, client: %d, seq: %d Put key:%v value:%v", kv.me, args.ClientId, args.SeqNum, args.Key, args.Value)
	kv.mu.Lock()
	lastAck, exists := kv.lastAck[args.ClientId]
	kv.mu.Unlock()
	if exists && args.SeqNum <= lastAck {
		reply.Err = Err("Error 0:")
		return
	}
	logIndex, _, _ := kv.rf.Start(Op{OpType: opPut, Key: args.Key, Value: args.Value, SeqNum: args.SeqNum, CLientId: args.ClientId})
	kvPrintf("2: Server %d, client: %d, seq: %d Put key:%v value:%v, index:%d", kv.me, args.ClientId, args.SeqNum, args.Key, args.Value, logIndex)
	waitChan := kv.getWaitChannel(logIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, logIndex)
		kv.mu.Unlock()
	}()
	timeout := time.NewTicker(time.Duration(timeoutLimit) * time.Millisecond)
	defer timeout.Stop()
	select {
	case opt := <-waitChan:
		if opt.CLientId != args.ClientId || opt.SeqNum != args.SeqNum { // 虽然等到了且index刚好相同，但是我提交的log被覆盖掉了
			// 譬如同步的是leader发过来的日志，刚好index相同
			reply.Err = Err(fmt.Sprintf("Error 2: Not Current Leader, try %d instead.", kv.rf.GetVotedFor()))
			return
		}
		reply.Err = Err("Error 0:")
	case <-timeout.C:
		// kvPrintf("index:%d timeout", logIndex)
		reply.Err = Err("Error 3: timeout")
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = Err(fmt.Sprintf("Error 1: The KvServer %d has been killed", kv.me))
		return
	}
	_, isLeader := kv.rf.GetRole()
	if !isLeader {
		reply.Err = Err(fmt.Sprintf("Error 2: Not Current Leader, try %d instead.", kv.rf.GetVotedFor()))
		return
	}
	kvPrintf("client: %d, seq: %d Append key:%v value:%v", args.ClientId, args.SeqNum, args.Key, args.Value)
	kv.mu.Lock()
	lastAck, exists := kv.lastAck[args.ClientId]
	kv.mu.Unlock()
	if exists && args.SeqNum <= lastAck {
		reply.Err = Err("Error 0:")
		return
	}
	logIndex, _, _ := kv.rf.Start(Op{OpType: opAppend, Key: args.Key, Value: args.Value, SeqNum: args.SeqNum, CLientId: args.ClientId})
	waitChan := kv.getWaitChannel(logIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, logIndex)
		kv.mu.Unlock()
	}()
	timeout := time.NewTicker(time.Duration(timeoutLimit) * time.Millisecond)
	defer timeout.Stop()
	select {
	case opt := <-waitChan:
		if opt.CLientId != args.ClientId || opt.SeqNum != args.SeqNum { // 虽然等到了且index刚好相同，但是我提交的log被覆盖掉了
			// 譬如同步的是leader发过来的日志，刚好index相同
			reply.Err = Err(fmt.Sprintf("Error 2: Not Current Leader, try %d instead.", kv.rf.GetVotedFor()))
			return
		}
		reply.Err = Err("Error 0:")
	case <-timeout.C:
		reply.Err = Err("Error 3: timeout")
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvMap = make(map[string]string)
	kv.lastAck = make(map[int32]int64)
	kv.waitCh = make(map[int]chan Op)
	// You may need initialization code here.
	go kv.handleMsgFromRaft()
	return kv
}
