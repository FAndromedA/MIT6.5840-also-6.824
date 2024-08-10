package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs  []Config // indexed by config num
	lastAck  map[int64]int64
	waitChan map[int]chan Op
	dead     int32 // set by Kill()
}

const timeoutLimit = time.Duration(100) * time.Millisecond

type OpType uint8

const (
	opJoin = iota + 1
	opLeave
	opMove
	opQuery
)

type Op struct {
	// Your data here.
	OpType   OpType
	ClientId int64
	SeqNum   int64
	Servers  map[int][]string
	GIDorNum int
	Shard    int
	GIDs     []int
	Query    Config
}

// add new group if join = true, else means delete
func (sc *ShardCtrler) rebanlance(newConfig *Config, join bool) {
	shardCnt := make(map[int]int) // GID -> shard count
	notAllocate := make([]int, 0) // shard that does not allocate
	groupNum := len(newConfig.Groups)
	if groupNum == 0 {
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = 0
		}
		return
	}
	workload, mod := NShards/groupNum, NShards%groupNum
	for i := 0; i < NShards; i++ {
		gid := newConfig.Shards[i]
		_, exists := newConfig.Groups[gid]
		if exists {
			shardCnt[gid]++
		} else {
			notAllocate = append(notAllocate, i)
		}
	}
	// 因为底层是hash map因此不同的server遍历k的顺序不一定一样，所以不能直接遍历map
	// 需要先得到map所有的key再排序，之后遍历排好序的keys
	keys := make([]int, 0, groupNum)
	for k := range newConfig.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	if join {
		// NShards = (workload + 1) * mod + workload * (groupNum - mod)
		vis1 := make(map[int]bool)
		for _, k := range keys {
			if mod > 0 && shardCnt[k] >= workload+1 {
				mod--
				vis1[k] = true
			}
		}
		for i := 0; i < NShards; i++ {
			gid := newConfig.Shards[i]
			_, exists := newConfig.Groups[gid]
			if exists {
				if (vis1[gid] && shardCnt[gid] > workload+1) || (!vis1[gid] && shardCnt[gid] > workload) {
					notAllocate = append(notAllocate, i)
					shardCnt[gid]--
				}
			}
		}
		offset := 0
		for _, k := range keys {
			if shardCnt[k] == 0 {
				give := workload
				if mod > 0 {
					give = workload + 1
					mod--
				}
				for i := 1; i <= give; i++ {
					shard := notAllocate[offset]
					offset++
					newConfig.Shards[shard] = k
				}
			}
		}
	} else {
		for i := 0; i < len(notAllocate); i++ {
			shard := notAllocate[i]
			for _, k := range keys {
				if shardCnt[k] == workload && mod > 0 {
					mod--
					shardCnt[k]++
					newConfig.Shards[shard] = k
					break
				} else if shardCnt[k] < workload {
					shardCnt[k]++
					newConfig.Shards[shard] = k
					break
				}
			}
		}
	}
}

func (sc *ShardCtrler) updateLastAck(clientId int64, seqNum int64) {
	lastAck, exists := sc.lastAck[clientId]
	if !exists {
		sc.lastAck[clientId] = seqNum
	} else if lastAck < seqNum {
		sc.lastAck[clientId] = seqNum
	}
}

func (sc *ShardCtrler) duplicateSeq(clientId int64, seqNum int64) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeq, exists := sc.lastAck[clientId]
	if !exists || lastSeq < seqNum {
		return false
	}
	return true
}

func (sc *ShardCtrler) getWaitChan(index int, create bool) (chan Op, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	waitChan, exists := sc.waitChan[index]
	if exists {
		return waitChan, true
	}
	if create {
		sc.waitChan[index] = make(chan Op)
		return sc.waitChan[index], true
	}
	return waitChan, false
}

func (sc *ShardCtrler) deepCopyConfig(previousOne *Config) Config {
	var newConfig Config
	newConfig.Groups = make(map[int][]string)
	newConfig.Num = previousOne.Num
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = previousOne.Shards[i]
	}
	for k, v := range previousOne.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (sc *ShardCtrler) handleMsgFromRaft() {
	for {
		if sc.killed() {
			return
		}
		select {
		case rfMsg := <-sc.applyCh:
			if rfMsg.CommandValid {
				index := rfMsg.CommandIndex
				opt := rfMsg.Command.(Op)
				if opt.OpType == opQuery || !sc.duplicateSeq(opt.ClientId, opt.SeqNum) {
					SdcPrintf("Server %d commit type %v index %d", sc.me, opt.OpType, index)
					sc.mu.Lock()
					switch opt.OpType {
					case opJoin:
						var newConfig Config = sc.deepCopyConfig(&sc.configs[len(sc.configs)-1])
						newConfig.Num = len(sc.configs)
						for k, v := range opt.Servers {
							newConfig.Groups[k] = v
						}
						sc.rebanlance(&newConfig, true)
						sc.configs = append(sc.configs, newConfig)
						SdcPrintf("Server %d index %d, join newConfig %v", sc.me, index, newConfig)

					case opLeave:
						var newConfig Config = sc.deepCopyConfig(&sc.configs[len(sc.configs)-1])
						newConfig.Num = len(sc.configs)
						for _, gid := range opt.GIDs {
							delete(newConfig.Groups, gid)
						}
						sc.rebanlance(&newConfig, false)
						sc.configs = append(sc.configs, newConfig)
						SdcPrintf("Server %d index %d, leave newConfig %v", sc.me, index, newConfig)

					case opMove:
						var newConfig Config = sc.deepCopyConfig(&sc.configs[len(sc.configs)-1])
						newConfig.Num = len(sc.configs)
						newConfig.Shards[opt.Shard] = opt.GIDorNum
						sc.configs = append(sc.configs, newConfig)
						SdcPrintf("Server %d index %d, move newConfig %v", sc.me, index, newConfig)

					case opQuery:
						if opt.GIDorNum == -1 || opt.GIDorNum >= len(sc.configs) {
							opt.Query = sc.deepCopyConfig(&sc.configs[len(sc.configs)-1])
						} else {
							opt.Query = sc.deepCopyConfig(&sc.configs[opt.GIDorNum])
						}
					default:
						sc.mu.Unlock()
						continue
					}
					sc.updateLastAck(opt.ClientId, opt.SeqNum)
					sc.mu.Unlock()
				}
				waitCh, exists := sc.getWaitChan(index, false)
				if exists {
					waitCh <- opt
				}
			}
		}
	}
}

/*
The Join RPC is used by an administrator to add new replica groups.
Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names.
The shardctrler should react by creating a new configuration that includes the new replica groups.
The new configuration should divide the shards as evenly as possible among the full set of groups,
and should move as few shards as possible to achieve that goal.
The shardctrler should allow re-use of a GID if it's not part of the current configuration
(i.e. a GID should be allowed to Join, then Leave, then Join again).
*/
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sc.killed() {
		reply.WrongLeader = true
		return
	}
	if sc.duplicateSeq(args.ClientId, args.SeqNum) {
		reply.WrongLeader = false
		reply.Err = Duplicate
		return
	}
	op := Op{OpType: opJoin, ClientId: args.ClientId, SeqNum: args.SeqNum, Servers: args.Servers}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	SdcPrintf("Server %d start a join index %d, args %v", sc.me, index, args)
	waitChan, _ := sc.getWaitChan(index, true)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChan, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(timeoutLimit)
	defer timer.Stop()
	reply.WrongLeader = false
	select {
	case opt := <-waitChan:
		if opt.ClientId != op.ClientId || opt.SeqNum != op.SeqNum { // 我提交的log被覆盖掉了说明我现在很可能不是leader了
			reply.WrongLeader = true
			return
		}
		reply.Err = OK
	case <-timer.C:
		reply.Err = Timeout
	}
}

/*
The Leave RPC's argument is a list of GIDs of previously joined groups. The shardctrler should create
a new configuration that does not include those groups, and that assigns those groups' shards to the remaining groups.
The new configuration should divide the shards as evenly as possible among the groups,
and should move as few shards as possible to achieve that goal.
*/
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sc.killed() {
		reply.WrongLeader = true
		return
	}
	if sc.duplicateSeq(args.ClientId, args.SeqNum) {
		reply.WrongLeader = false
		reply.Err = Duplicate
		return
	}
	op := Op{OpType: opLeave, ClientId: args.ClientId, SeqNum: args.SeqNum, GIDs: args.GIDs}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	SdcPrintf("Server %d start a leave index %d, args %v", sc.me, index, args)
	waitChan, _ := sc.getWaitChan(index, true)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChan, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(timeoutLimit)
	defer timer.Stop()
	reply.WrongLeader = false
	select {
	case opt := <-waitChan:
		if opt.ClientId != op.ClientId || opt.SeqNum != op.SeqNum {
			reply.WrongLeader = true
			return
		}
		reply.Err = OK
	case <-timer.C:
		reply.Err = Timeout
	}
}

/*
The Move RPC's arguments are a shard number and a GID.
The shardctrler should create a new configuration in which
the shard is assigned to the group. The purpose of Move is
to allow us to test your software. A Join or Leave following
a Move will likely un-do the Move, since Join and Leave re-balance.
*/
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sc.killed() {
		reply.WrongLeader = true
		return
	}
	if sc.duplicateSeq(args.ClientId, args.SeqNum) {
		reply.WrongLeader = false
		reply.Err = Duplicate
		return
	}
	op := Op{OpType: opMove, ClientId: args.ClientId, SeqNum: args.SeqNum, GIDorNum: args.GID, Shard: args.Shard}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	SdcPrintf("Server %d start a move index %d, args %v", sc.me, index, args)
	waitChan, _ := sc.getWaitChan(index, true)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChan, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(timeoutLimit)
	defer timer.Stop()
	reply.WrongLeader = false
	select {
	case opt := <-waitChan:
		if opt.ClientId != op.ClientId || opt.SeqNum != op.SeqNum {
			reply.WrongLeader = true
			return
		}
		reply.Err = OK
	case <-timer.C:
		reply.Err = Timeout
	}
}

/*
The Query RPC's argument is a configuration number. The shardctrler
replies with the configuration that has that number. If the number
is -1 or bigger than the biggest known configuration number,
the shardctrler should reply with the latest configuration.
The result of Query(-1) should reflect every Join, Leave,
or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
*/
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sc.killed() {
		reply.WrongLeader = true
		return
	}
	op := Op{OpType: opQuery, ClientId: args.ClientId, SeqNum: args.SeqNum, GIDorNum: args.Num}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	SdcPrintf("Server %d start a query index %d", sc.me, index)
	waitChan, _ := sc.getWaitChan(index, true)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChan, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(timeoutLimit)
	defer timer.Stop()
	reply.WrongLeader = false
	select {
	case opt := <-waitChan:
		if opt.ClientId != op.ClientId || opt.SeqNum != op.SeqNum {
			reply.WrongLeader = true
			return
		}
		reply.Config = opt.Query
		reply.Err = OK
	case <-timer.C:
		reply.Err = Timeout
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastAck = make(map[int64]int64)
	sc.waitChan = make(map[int]chan Op)
	go sc.handleMsgFromRaft()
	return sc
}
