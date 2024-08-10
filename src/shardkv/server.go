package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const timeoutLimit = time.Duration(100) * time.Millisecond

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
	ClientId int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd // config controller
	maxraftstate int                 // snapshot if log grows this big

	// Your definitions here.
	kvMap    map[string]string // snapshot
	lastAck  map[int64]int64   // snapshot
	waitChan map[int]chan Op

	mck  *shardctrler.Clerk // to communicate with the shard ctrler
	conf shardctrler.Config
	dead int32
}

func (kv *ShardKV) updateLastAck(clientId int64, seqNum int64) {
	lastAck, exists := kv.lastAck[clientId]
	if !exists {
		kv.lastAck[clientId] = seqNum
	} else if lastAck < seqNum {
		kv.lastAck[clientId] = seqNum
	}
}

func (kv *ShardKV) duplicateSeq(clientId int64, seqNum int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeq, exists := kv.lastAck[clientId]
	if !exists || lastSeq < seqNum {
		return false
	}
	return true
}

func (kv *ShardKV) getWaitChan(index int, create bool) (chan Op, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	waitChan, exists := kv.waitChan[index]
	if exists {
		return waitChan, true
	}
	if create {
		kv.waitChan[index] = make(chan Op)
		return kv.waitChan[index], true
	}
	return waitChan, false
}

func (kv *ShardKV) persist(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(kv.kvMap)
	encoder.Encode(kv.lastAck)
	kv.rf.Snapshot(index, writer.Bytes())
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	writer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(writer)
	kvmap := make(map[string]string)
	lastack := make(map[int64]int64)
	if decoder.Decode(&kvmap) != nil || decoder.Decode(&lastack) != nil {
		skvPrintf("WARNING: KVServer %d failed to load snapshot form leader.", kv.me)
	} else {
		skvPrintf("kvServer %d receive snapshot", kv.me)
		kv.mu.Lock()
		kv.kvMap = kvmap
		kv.lastAck = lastack
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) handleMsgFromRaft() {
	for {
		if kv.killed() {
			return
		}
		select {
		case rfMsg := <-kv.applyCh:
			if rfMsg.CommandValid {
				index := rfMsg.CommandIndex
				opt := rfMsg.Command.(Op)
				if opt.OpType == opGet || !kv.duplicateSeq(opt.ClientId, opt.SeqNum) {
					switch opt.OpType {
					case opGet:
						value, exists := kv.kvMap[opt.Key]
						if exists {
							opt.Value = value
						} else {
							opt.Value = ""
						}
						kv.updateLastAck(opt.ClientId, opt.SeqNum)
					case opPut:
						kv.kvMap[opt.Key] = opt.Value
						kv.updateLastAck(opt.ClientId, opt.SeqNum)
					case opAppend:
						kv.kvMap[opt.Key] += opt.Value
						kv.updateLastAck(opt.ClientId, opt.SeqNum)
					default:
						continue
					}
				}
				if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
					// raft state超过限额，自动保存快照
					skvPrintf("kvServer %d self save", kv.me)
					kv.persist(index)
				}
				waitCh, exists := kv.getWaitChan(index, false)
				if exists {
					waitCh <- opt
				}
			} else if rfMsg.SnapshotValid {
				kv.readPersist(rfMsg.Snapshot)
				continue
			}
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.gid != kv.conf.Shards[shard] {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: opGet, ClientId: args.ClientId, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	waitChan, _ := kv.getWaitChan(index, true)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChan, index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(timeoutLimit)
	defer timer.Stop()
	select {
	case opt := <-waitChan:
		if opt.ClientId != op.ClientId || opt.SeqNum != op.SeqNum { // 我提交的log被覆盖掉了说明我现在很可能不是leader了
			reply.Err = ErrWrongLeader
			return
		}
		reply.Value = opt.Value
		reply.Err = OK
	case <-timer.C:
		reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.gid != kv.conf.Shards[shard] {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.duplicateSeq(args.ClientId, args.SeqNum) {
		reply.Err = OK //Duplicate
		return
	}
	var opTyp OpType = opAppend
	if args.Op == "Put" {
		opTyp = opPut
	}
	op := Op{OpType: opTyp, ClientId: args.ClientId, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	waitChan, _ := kv.getWaitChan(index, true)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChan, index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(timeoutLimit)
	defer timer.Stop()
	select {
	case opt := <-waitChan:
		if opt.ClientId != op.ClientId || opt.SeqNum != op.SeqNum { // 我提交的log被覆盖掉了说明我现在很可能不是leader了
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-timer.C:
		reply.Err = ErrTimeOut
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardKV) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardKV) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.kvMap = make(map[string]string)
	kv.lastAck = make(map[int64]int64)
	kv.waitChan = make(map[int]chan Op)
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.conf = kv.mck.Query(-1)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleMsgFromRaft()
	return kv
}
