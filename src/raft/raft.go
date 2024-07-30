package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command  interface{}
	LogIndex int
	LogTerm  int
}

var emptyHeartbeatLog = LogEntry{LogIndex: -1, LogTerm: -1}

type Role int8

const (
	follower Role = iota + 1
	candidate
	leader
)

type VoteResult int8

const (
	vDisagree VoteResult = iota
	vAgree
	vOutOfDate
)

const heartbeatTimeout = 120 * time.Millisecond

// in 'probe' we don't know exactly the next and match index of this peer,
// so we send one entry in each rpc
// in 'replicate' we know exactly the next and match index of the peer,
// so we send entries in batch with all the entries it need
// in 'snapshot' we send a snapshot before, and we should
// stop sending appendEntries(except empty heartbeat) until we got a response

type ProgressStateType int8

const (
	pProbe ProgressStateType = iota
	pReplicate
	pSnapshot
)

// https://youjiali1995.github.io/raft/etcd-raft-log-replication/
type Progress struct {
	// Match, Next     int // in Raft.nextIndex and Raft.matchIndex
	State           ProgressStateType
	PendingSnapshot int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role
	// persistent state on all servers:
	currentTerm      int
	votedFor         int // candidate voted for in this term, if rf is a candidate itself then represent the vote num got
	logs             []LogEntry
	snapshot         []byte
	LastIncludedTerm int
	startIndex       int // the lastLogIndex of the snapshot, also the index of placeholder logs[0] (important)
	// the latest log index should be [rf.startIndex + len(rf.logs) - 1]
	// startTerm int // the lastLog's Term of the snapshot, also the term of placeholder logs[0]
	// we don't need to save it, just read it from logs[0]

	// volatile state on all servers:
	commitIndex int // index of highest log entry to be committed
	lastApplied int // index of highest log entry applied to state machine
	// volatile state on leaders:
	// Reinitialized after election
	nextIndex []int // index of next log entry to send to that server
	// (initialized to leader last log index + 1, 一致性检查会自动调整到对应位置)
	matchIndex []int // index of hignest log entry known to be replicated on server
	// (initialized to 0, increases monotonically单调的)
	progress []Progress

	// my customize variables
	// timer            *time.Ticker // used for detect heartbeat timeout
	electionTimeout  *time.Ticker
	heartbeatTimeout *time.Ticker
	applyCh          chan ApplyMsg
}

func (rf *Raft) resetElection() {
	ms := 250 + (rand.Int63() % 250)
	rf.electionTimeout.Reset(time.Millisecond * time.Duration(ms))
}

func (rf *Raft) resetHeartbeat() {
	rf.heartbeatTimeout.Reset(heartbeatTimeout)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.startIndex)
	e.Encode(rf.LastIncludedTerm)
	e.Encode(rf.logs)
	raftState := writer.Bytes()
	if len(rf.snapshot) == 0 {
		rf.persister.Save(raftState, nil)
	} else {
		rf.persister.Save(raftState, rf.snapshot) //只在installSnapshot里面的persist直接调用
	}

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	receiver := bytes.NewBuffer(data)
	d := labgob.NewDecoder(receiver)
	var currentTerm, votedFor, startIndex, LastIncludedTerm int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&startIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil || d.Decode(&logs) != nil {

		DPrintf("Server %d failed to readPersist.", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.startIndex = startIndex
		rf.LastIncludedTerm = LastIncludedTerm
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.logs = logs
		rf.logs[0].LogIndex = startIndex
		rf.logs[0].LogTerm = LastIncludedTerm
	}
}

// func (rf *Raft) readSnapshot(data []byte) {
// 	receiver := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(receiver)

// 	var startIndex int
// 	var logs []interface{}
// 	if d.Decode(&startIndex) != nil || d.Decode(&logs) != nil {
// 		DPrintf("Server %d failed to readSnapshot.", rf.me)
// 	} else {
// 		rf.mu.Lock()
// 		defer rf.mu.Unlock()
// 		rf.snapshot = SnapShot{
// 			lastIncludedIdx: startIndex,
// 			data:            data,
// 		}
// 		rf.startIndex = startIndex
// 		rf.lastApplied = startIndex
// 		rf.commitIndex = startIndex
// 	}
// }

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if rf.killed() {
		return
	}
	DPrintf("%d %d ____________________________________", rf.me, index)
	rf.mu.Lock()
	if rf.startIndex >= index || index > rf.commitIndex {
		return
	}
	if len(snapshot) > 0 {

		rf.logs = rf.logs[index-rf.startIndex:] // 保留最后一个作为logs[0]占位符
		rf.snapshot = snapshot
		rf.startIndex = index
		rf.LastIncludedTerm = rf.logs[0].LogTerm
		rf.persist()
		// rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	}
	rf.mu.Unlock()
	DPrintf("%d ++++++++++++++++++++++++++++++++++++", rf.me)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 自己当前任期号
	CandidateId  int // 自己的ID
	LastLogIndex int // 自己最后一个日志号
	LastLogTerm  int // 自己最后一个日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int        // 自己当前任期号
	VoteGranted VoteResult // 自己会不会投票给这个candidate
}

func compLog(term1 int, id1 int, term2 int, id2 int) bool {
	if term1 > term2 || (term1 == term2 && id1 >= id2) {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.VoteGranted = vDisagree
		return
	}
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term { // 新term，votedFor重置
		// if rf.role == leader {
		// 	DPrintf("%d becomes follower", rf.me)
		// }
		rf.role = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.votedFor != -1 { // 已经投过票就不投了
		reply.VoteGranted = vDisagree
		return
	}
	// DPrintf("Candidate %d, Server %d, %d %d %d %d %v\n", args.CandidateId, rf.me, args.LastLogTerm,
	// 	rf.logs[len(rf.logs)-1].LogTerm, args.LastLogIndex, rf.startIndex+len(rf.logs)-1,
	// 	compLog(args.LastLogTerm, args.LastLogIndex, rf.logs[len(rf.logs)-1].LogTerm, rf.startIndex+len(rf.logs)-1))

	if args.Term >= rf.currentTerm && compLog(args.LastLogTerm, args.LastLogIndex, rf.logs[len(rf.logs)-1].LogTerm, rf.startIndex+len(rf.logs)-1) { // 同时满足两个条件才同意
		// if rf.role == leader { // 脑裂
		// 	DPrintf("????C %d S %d, CT %d, ST %d", args.CandidateId, rf.me, args.Term, rf.currentTerm)
		// }
		reply.VoteGranted = vAgree
		rf.votedFor = args.CandidateId
		// 重置倒计时
		rf.resetElection()
	} else { // the sender is out of date
		reply.VoteGranted = vOutOfDate
	}
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != candidate || rf.killed() { //must be a candidate in the following switch
		return false
	}
	if ok {
		switch reply.VoteGranted {
		case vOutOfDate:
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = follower
			rf.persist()
			rf.resetElection()
			return false
		case vAgree:
			rf.votedFor++
			DPrintf("Server %d got a vote from %d, total %d/%d.", rf.me, server, rf.votedFor, len(rf.peers))
			if rf.votedFor > len(rf.peers)/2 {
				// rf.votedFor = -1
				// 如果不注释掉这一行的话，会导致同一时间选举的candidate能获得这个leader的选票（term相同，votedFor=-1）
				rf.role = leader
				DPrintf("<---    New Leader %d elected with Term %d    --->", rf.me, rf.currentTerm)
				// no-op 优化
				if len(rf.logs) > 1 {
					rf.logs[len(rf.logs)-1].LogTerm = rf.currentTerm
				}
				rf.persist()
				for i := 0; i < len(rf.peers); i++ {
					rf.progress[i].State = pProbe
					rf.progress[i].PendingSnapshot = 0
					rf.nextIndex[i] = rf.startIndex + len(rf.logs) // init to be the last index + 1
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = rf.startIndex + len(rf.logs) - 1
				rf.resetHeartbeat()
				//rf.timer.Reset(heartbeatTimeout)
			}
		}
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int // use for redirect
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) applier() {
	applyList := make([]ApplyMsg, 0)
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
		if rf.lastApplied+1-rf.startIndex <= 0 { // 有可能创建applier之后 收到来自leader的快照了，导致log被删，越界
			rf.lastApplied++ // 之前没加这个，导致卡死
			continue
		}
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied+1-rf.startIndex].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.lastApplied++
		applyList = append(applyList, applyMsg)
	}
	rf.mu.Unlock()
	for _, applyMsg := range applyList {
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		return
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.votedFor = args.LeaderId // 方便 redirect to leader
	rf.currentTerm = args.Term
	rf.persist()
	rf.role = follower
	rf.resetElection()
	// if args.Entries == nil { // just a heart beat // 就算只是heartbeat，也要有最后的根据新的leader commit提交
	// 	reply.Success = true
	// 	return
	// }
	if args.Entries == nil || (args.Entries[0].LogIndex != emptyHeartbeatLog.LogIndex && args.Entries[0].LogTerm != emptyHeartbeatLog.LogTerm) { // not empty heart beat
		DPrintf("%d %d %d %d", rf.me, rf.startIndex, args.PreLogIndex, len(args.Entries))
		if len(args.Entries) > 0 {
			DPrintf("%d %d %v", args.Entries[0].LogIndex, args.Entries[0].LogTerm, rf.startIndex+len(rf.logs)-1 < args.PreLogIndex || (args.PreLogIndex > rf.startIndex && rf.logs[args.PreLogIndex-rf.startIndex].LogTerm != args.PreLogTerm))
		}
		if rf.startIndex+len(rf.logs)-1 < args.PreLogIndex ||
			(args.PreLogIndex > rf.startIndex && rf.logs[args.PreLogIndex-rf.startIndex].LogTerm != args.PreLogTerm) {
			// args.PreLogIndex 可能小于 rf.startIndex
			// 继续往前找
			reply.Success = false
			return
		}
		if args.Entries != nil {

			for i, entry := range args.Entries {
				if entry.LogIndex <= rf.startIndex { // 0 不能被替换
					continue
				}
				Assert(entry.LogIndex-rf.startIndex <= len(rf.logs), fmt.Sprintf("preIndex %d entry.LogIndex %d startIndex %d Len logs %d Len Entry %d %v", args.PreLogIndex, entry.LogIndex, rf.startIndex, len(rf.logs), len(args.Entries), args.Entries))
				rf.logs = rf.logs[:entry.LogIndex-rf.startIndex]
				if rf.startIndex+len(rf.logs)-1 < entry.LogIndex { //没有的日志，append
					rf.logs = append(rf.logs, args.Entries[i:]...)
					break
				}
				if rf.logs[entry.LogIndex-rf.startIndex].LogTerm != entry.LogTerm { //有但是不相同的日志，替换
					rf.logs = rf.logs[:entry.LogIndex-rf.startIndex] // 保留 0 - entry.LogIndex-1的日志，后面的都冲突了去掉
					// 这里不去掉的话，而是一个一个替换的话，因为旧的log数可能超过了我和当前leader同步的log数，
					// 然而这个同步数又可能比leader的commit index小，因为我们是把小于leader commit的都提交了，所以有可能提交错误的。
					rf.logs = append(rf.logs, args.Entries[i:]...)
					break
				}
				rf.logs = append(rf.logs, entry)
			}
		}
		rf.persist()
		rf.commitIndex = min(rf.startIndex+len(rf.logs)-1, args.LeaderCommit) // 必须放在里面，放在外面可能还没同步
		go rf.applier()
	}
	DPrintf("Server %d LastLog %d LeaderCommit %d serverCommit %d", rf.me, rf.startIndex+len(rf.logs)-1, args.LeaderCommit, rf.commitIndex)
	reply.Success = true
}

func (rf *Raft) leaderUpdateCommitIndex() {
	tmpMatchIndex := make([]int, len(rf.peers))
	copy(tmpMatchIndex, rf.matchIndex)
	// sort.Ints(tmpMatchIndex) // this is minimum first but it should be maximum first
	sort.Slice(tmpMatchIndex, func(i, j int) bool { return tmpMatchIndex[i] > tmpMatchIndex[j] })
	halfPos := (len(rf.peers)+2)/2 - 1 // overhalf position, indexed from 0 so minus 1
	// rf.commitIndex = tmpMatchIndex[halfPos] 超过一半的节点有了就commit，
	// 因为有安全性限制，只能提交term等于当前term的日志，而且我们没用no-op优化
	// 也是这里使得last applied 和 commit index不一样
	if tmpMatchIndex[halfPos] > rf.startIndex && rf.logs[tmpMatchIndex[halfPos]-rf.startIndex].LogTerm == rf.currentTerm {
		rf.commitIndex = tmpMatchIndex[halfPos]
	}
	DPrintf("Leader %d TermId %d commit %d, matchIndex List %v \n sorted matchIndex List %v", rf.me, rf.currentTerm, rf.commitIndex, rf.matchIndex, tmpMatchIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// max_retry := 2 // 自己加的
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok && !rf.killed() { // && max_retry > 0
		// max_retry--
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && (rf.role == leader && rf.currentTerm == args.Term) && !rf.killed() {
		if reply.Success {
			if args.Entries == nil || (args.Entries[0].LogIndex != emptyHeartbeatLog.LogIndex && args.Entries[0].LogTerm != emptyHeartbeatLog.LogTerm) { // not empty heart beat
				rf.progress[server].State = pReplicate
				if args.Entries != nil {
					rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1 //我们一定会发送leader的最后一个log// //rf.matchIndex[rf.me] + 1
					rf.matchIndex[server] = args.Entries[len(args.Entries)-1].LogIndex    //    //rf.matchIndex[rf.me]
				} else { // 发送的是 nil，即发送的nextIndex超过了leader的logs，preIndex是logs的最后一项
					rf.nextIndex[server] = args.PreLogIndex + 1
					rf.matchIndex[server] = args.PreLogIndex
				}

				// DPrintf("Leader %d TermID %d. append entry to server %d, preCommitIndex %d, preLogIndex %d, appendLen %d, newNextIndex %d",
				//	rf.me, rf.currentTerm, server, rf.commitIndex, args.PreLogIndex, len(args.Entries), rf.nextIndex[server])
				rf.leaderUpdateCommitIndex()
				go rf.applier()
			}
			DPrintf("Server %d Len %d not Empty %v preLogIndex %d startIndex %d", server, len(args.Entries), args.Entries == nil || (args.Entries[0].LogIndex != emptyHeartbeatLog.LogIndex && args.Entries[0].LogTerm != emptyHeartbeatLog.LogTerm),
				args.PreLogIndex, rf.startIndex)
			//DPrintf("Leader %d Newest commit ID: %d.", rf.me, rf.commitIndex)
			//}
		} else {
			if reply.Term > rf.currentTerm { // rejected because term out of date
				rf.role = follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.resetElection()
			} else if reply.Term != -1 { // rejected because it does not have preLog
				// rf.nextIndex[server] = max(1, args.PreLogIndex/2)
				rf.progress[server].State = pProbe
				if rf.snapshot != nil && args.PreLogIndex <= rf.startIndex { // 如果不用二分，一次只减少1就不用特判，但是这里不得不特判
					rf.nextIndex[server] = rf.startIndex
					// if rf.nextIndex[server] == rf.startIndex 需要快照, 因为如果preLogIndex=startIndex都没一致
					// 而startIndex代表的是snapshot的最后一个日志的index，说明 nextIndex应该是snapshot里的
				} else {
					rf.nextIndex[server] = max(rf.startIndex+1, (args.PreLogIndex+rf.startIndex)/2) // 二分查找
				}
				DPrintf("Leader %d TermId %d, Consistence check failed: server %d, preLogIndex %d, newNextIndex: %d",
					rf.me, rf.currentTerm, server, args.PreLogIndex, rf.nextIndex[server])
			} // reply.Term == -1 means the server is killed
		}
	}

	return ok
}

// invoked by the leader to sned chunks of snapshot to a follower
// leader always send chunks in order
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int // the snapshot replaces all the entries up through and including this index
	LastIncludedTerm  int // term of the last included index
	//Offset            int    // bytes offset where chunk is positioned in the snapshot file
	// in this lab, we send the entrie snapshot at once
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	// Done bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("Server %d startIndex %d, receive snapshot %d", rf.me, rf.startIndex, args.LastIncludedIndex)
	rf.mu.Lock()
	if rf.killed() {
		reply.Term = -1
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term || rf.startIndex > args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.role = follower
	rf.resetElection()
	// rf.startIndex = args.LastIncludedIndex 不能放在这里，之后还要用
	if len(rf.logs)+rf.startIndex-1 >= args.LastIncludedIndex && rf.logs[args.LastIncludedIndex-rf.startIndex].LogTerm == args.LastIncludedTerm {
		rf.logs = rf.logs[args.LastIncludedIndex-rf.startIndex:]
	} else {
		rf.logs = make([]LogEntry, 1)
	}
	rf.startIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.logs[0] = LogEntry{ // as a placeholder
		LogIndex: args.LastIncludedIndex,
		LogTerm:  args.LastIncludedTerm,
	}
	rf.persist()
	rf.commitIndex = args.LastIncludedIndex //max(rf.commitIndex, rf.startIndex)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	go func() {
		rf.applyCh <- applyMsg
		rf.mu.Lock()
		rf.lastApplied = args.LastIncludedIndex
		rf.mu.Unlock()
	}()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for !ok && !rf.killed() {
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && (rf.role == leader && rf.currentTerm == args.Term) && !rf.killed() {
		if rf.currentTerm < reply.Term { // retire
			rf.role = follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
			rf.resetElection()
		} else if reply.Term != -1 { // the receiver not killed
			rf.progress[server].State = pProbe
			rf.matchIndex[server] = rf.startIndex    //max(rf.matchIndex[server], rf.startIndex)
			rf.nextIndex[server] = rf.startIndex + 1 //max(rf.nextIndex[server], rf.startIndex+1)
			rf.progress[server].PendingSnapshot = rf.startIndex
			DPrintf("Leader %d TermId %d, send snapshot to %d", rf.me, rf.currentTerm, server)
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.role == leader
	if rf.killed() || !isLeader { // if this
		// server isn't the leader, returns false.
		return index, term, false
	}
	// Your code here (3B).
	appendLog := LogEntry{
		Command:  command,
		LogIndex: rf.startIndex + len(rf.logs), // before append, so don't need minus 1
		LogTerm:  rf.currentTerm,
	}
	rf.matchIndex[rf.me] = appendLog.LogIndex
	rf.logs = append(rf.logs, appendLog)
	rf.persist()
	index = appendLog.LogIndex
	term = rf.currentTerm
	Assert(index == rf.logs[len(rf.logs)-2].LogIndex+1, fmt.Sprintf("Leader %d Logs %v", rf.me, rf.logs))
	DPrintf("Leader %d TermId %d, new log index %d", rf.me, rf.currentTerm, index)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimeout.C:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			switch rf.role {
			case follower: // 一段时间内没有收到heartbeat，变成candidate
				rf.role = candidate
				fallthrough
			case candidate:
				rf.currentTerm++
				rf.votedFor = 1 // 这里作为计数器使用
				rf.persist()
				DPrintf("Server %d start an election with termId: %d\n", rf.me, rf.currentTerm)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					reqVoteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.logs[len(rf.logs)-1].LogIndex,
						LastLogTerm:  rf.logs[len(rf.logs)-1].LogTerm,
					}
					reqVoteReply := RequestVoteReply{}
					go rf.sendRequestVote(i, &reqVoteArgs, &reqVoteReply)
				}
				rf.resetElection()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimeout.C:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			if rf.role == leader {
				// heartbeat
				rf.resetHeartbeat()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me { // 不用发给自己
						continue
					}
					if (rf.progress[i].State == pSnapshot || rf.nextIndex[i] > rf.startIndex) && (rf.matchIndex[i] < rf.startIndex+len(rf.logs)-1 || rf.snapshot == nil || rf.progress[i].PendingSnapshot >= rf.startIndex) {
						// DPrintf("Leader %d Server %d : 1", rf.me, i)
						preLogIndex := max(rf.nextIndex[i]-1, rf.startIndex)
						appendArgs := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PreLogIndex:  preLogIndex, // there is no chance for nextIndex to be less than 1
							PreLogTerm:   rf.logs[preLogIndex-rf.startIndex].LogTerm,
							Entries:      make([]LogEntry, 1), // 心跳, 心跳不能是 nil，因为Probe状态下一开始未知next初始化为没有的log也是nil
							LeaderCommit: rf.commitIndex,
						}
						appendArgs.Entries[0] = emptyHeartbeatLog
						appendReply := AppendEntriesReply{}
						if rf.startIndex+len(rf.logs)-1 >= rf.nextIndex[i] && rf.nextIndex[i] > rf.startIndex { // 筛掉leader没有nextIndex的日志
							if rf.progress[i].State == pReplicate { // 定位到了match和next，一次发完
								appendArgs.Entries = rf.logs[appendArgs.PreLogIndex+1-rf.startIndex:]
							} else if rf.progress[i].State == pProbe { // 没定位到 match和next，一次只发一条
								// appendArgs.Entries[0] = rf.logs[appendArgs.PreLogIndex+1-rf.startIndex] 调试不好，还是一次发所有吧
								appendArgs.Entries = rf.logs[appendArgs.PreLogIndex+1-rf.startIndex:]
							} // 如果是pSnapshot不发entries，只发心跳
						} else if rf.progress[i].State != pSnapshot && rf.nextIndex[i] > rf.startIndex { //没有nextIndex的日志,也有可能是nil
							appendArgs.Entries = nil
						}

						go rf.sendAppendEntries(i, &appendArgs, &appendReply)
					} else if (rf.snapshot != nil && rf.progress[i].PendingSnapshot < rf.startIndex) && (rf.nextIndex[i] <= rf.startIndex ||
						rf.matchIndex[i] == rf.startIndex+len(rf.logs)-1) {
						// DPrintf("Leader %d Server %d : 2", rf.me, i)
						snapshotArgs := InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.startIndex,
							LastIncludedTerm:  rf.LastIncludedTerm,
							Data:              rf.snapshot,
						}
						snapshotReply := InstallSnapshotReply{}
						rf.progress[i].State = pSnapshot
						// rf.progress[i].PendingSnapshot = rf.startIndex
						go rf.sendInstallSnapshot(i, &snapshotArgs, &snapshotReply)
					}

				}
			}
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.role = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1) // 0占位
	rf.startIndex = 0             // 始终用startIndex的log占位能避免很多边界情况
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.progress = make([]Progress, len(peers))
	rf.applyCh = applyCh
	ms := 150 + (rand.Int63() % 200)
	rf.electionTimeout = time.NewTicker(time.Duration(ms) * time.Millisecond)
	rf.heartbeatTimeout = time.NewTicker(heartbeatTimeout)
	atomic.StoreInt32(&rf.dead, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	DPrintf("Server %d initialization done.\n", me)
	DPrintf("Server %d initialization done.\n", me)
	go rf.ticker()

	return rf
}
