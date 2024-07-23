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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	currentTerm int
	votedFor    int // candidate voted for in this term, if rf is a candidate itself then represent the vote num got
	logs        []LogEntry
	// volatile state on all servers:
	commitIndex int // index of highest log entry to be committed
	lastApplied int // index of highest log entry applied to state machine
	// volatile state on leaders:
	// Reinitialized after election
	nextIndex []int // index of next log entry to send to that server
	// (initialized to leader last log index + 1, 一致性检查会自动调整到对应位置)
	matchIndex []int // index of hignest log entry known to be replicated on server
	// (initialized to 0, increases monotonically单调的)

	// my customize variables
	timer   *time.Ticker // used for detect heartbeat timeout
	applyCh chan ApplyMsg
}

func max(a, b int) int {
	if a > b {
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	var myLastTerm int = 0
	if len(rf.logs) > 1 { // 除去index=0的占位
		myLastTerm = rf.logs[len(rf.logs)-1].LogTerm
		// DPrintf("Candidate %d, Server %d, %d %d %d %d %v\n", args.CandidateId, rf.me, args.LastLogTerm, myLastTerm, args.LastLogIndex, len(rf.logs)-1, compLog(args.LastLogTerm, args.LastLogIndex, myLastTerm, len(rf.logs)-1))
	}
	if args.Term >= rf.currentTerm && compLog(args.LastLogTerm, args.LastLogIndex, myLastTerm, len(rf.logs)-1) { // 同时满足两个条件才同意
		reply.VoteGranted = vAgree
		rf.votedFor = args.CandidateId
		// 重置倒计时
		ms := 150 + (rand.Int63() % 200)
		rf.timer.Reset(time.Duration(ms) * time.Millisecond)
	} else {
		reply.VoteGranted = vOutOfDate
	}
	rf.currentTerm = max(rf.currentTerm, args.Term)
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
	if rf.role != candidate {
		return false
	}
	if ok {
		switch reply.VoteGranted {
		case vOutOfDate:
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = follower
			return false
		case vAgree:
			rf.votedFor++
			DPrintf("Server %d got a vote from %d, total %d/%d.", rf.me, server, rf.votedFor, len(rf.peers))
			if rf.votedFor > len(rf.peers)/2 {
				if rf.role == leader {
					return ok
				}
				rf.votedFor = -1
				rf.role = leader
				// no-op 优化
				// no_op := LogEntry{
				// 	Command:  0,
				// 	LogIndex: len(rf.logs), // 因为有0这个占位
				// 	LogTerm:  rf.currentTerm,
				// }
				// rf.matchIndex[rf.me] = len(rf.logs) // before append, so don't need minus 1
				// rf.logs = append(rf.logs, no_op)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.logs[len(rf.logs)-1].LogIndex + 1
					rf.matchIndex[i] = 0
				}
				rf.timer.Reset(heartbeatTimeout)
			}
		}
	}
	return ok
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

	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	rf.role = follower
	ms := 150 + (rand.Int63() % 200)
	rf.timer.Reset(time.Duration(ms) * time.Millisecond)
	// if args.Entries == nil { // just a heart beat // 就算只是heartbeat，也要有最后的根据新的leader commit提交
	// 	reply.Success = true
	// 	return
	// }
	if len(rf.logs)-1 < args.PreLogIndex || rf.logs[args.PreLogIndex].LogTerm != args.PreLogTerm {
		// 继续往前找
		reply.Success = true
		return
	}
	var diff bool = false
	for _, entry := range args.Entries {
		if len(rf.logs)-1 < entry.LogIndex { //没有的日志，append
			rf.logs = append(rf.logs, entry)
			continue
		}
		if diff || rf.logs[entry.LogIndex].LogTerm != entry.LogTerm { //有但是不相同的日志，替换
			diff = true
			rf.logs[entry.LogIndex] = entry
			continue
		}
	}
	for rf.lastApplied < args.LeaderCommit && rf.lastApplied+1 < len(rf.logs) {
		//if rf.logs[rf.lastApplied].Command != 0 { // commit except no-op
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied+1].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.applyCh <- applyMsg
		//}
		rf.lastApplied++
		rf.commitIndex = rf.lastApplied
		DPrintf("Server %d commit %d", rf.me, rf.commitIndex)
	}
	reply.Success = true
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
	if ok {
		if reply.Success {
			if args.Entries != nil { // 等于nil是空心跳
				DPrintf("Leader %d append entry to server %d, preCommitIndex %d, preLogIndex %d", rf.me, server, rf.commitIndex, args.PreLogIndex)
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].LogIndex
				tmpMatchIndex := make([]int, len(rf.peers))
				copy(tmpMatchIndex, rf.matchIndex)
				sort.Ints(tmpMatchIndex) // sort minimum first
				halfPos := (len(rf.peers) + 2) / 2
				// rf.commitIndex = tmpMatchIndex[halfPos] 超过一半的节点有了就commit，
				// 因为有安全性限制，只能提交term等于当前term的日志，而且我们没用no-op优化
				// 也是这里使得last applied 和 commit index不一样
				if rf.logs[tmpMatchIndex[halfPos]].LogTerm == rf.currentTerm {
					rf.commitIndex = tmpMatchIndex[halfPos]
				}
				// 但是这里不能直接赋值，要在下面的循环里面一个一个加
				for rf.lastApplied < rf.commitIndex {
					//if rf.logs[rf.lastApplied].Command != 0 { // commit except no-op
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied+1].Command,
						CommandIndex: rf.lastApplied + 1,
					}
					rf.applyCh <- applyMsg
					rf.lastApplied++
					//}
				}
				DPrintf("Leader Newest commit ID: %d.", rf.commitIndex)
			}
		} else {
			if reply.Term > rf.currentTerm { // rejected because term out of date
				rf.role = follower
				rf.currentTerm = reply.Term
				ms := 150 + (rand.Int63() % 200)
				rf.timer.Reset(time.Duration(ms) * time.Millisecond)
			} else if reply.Term != -1 { // rejected because it does not have preLog
				DPrintf("Leader: %d, Consistence check: server %d nextIndex: %d, newNextIndex: %d", rf.me, server, rf.nextIndex[server], args.PreLogIndex)
				rf.nextIndex[server] = args.PreLogIndex
			} // reply.Term == -1 means the server is killed
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
		LogIndex: len(rf.logs), // 因为有0这个占位
		LogTerm:  rf.currentTerm,
	}
	rf.matchIndex[rf.me] = len(rf.logs) // before append, so don't need minus 1
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs) - 1
	term = rf.currentTerm
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
		case <-rf.timer.C:
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
				ms := 150 + (rand.Int63() % 200)
				rf.timer.Reset(time.Duration(ms) * time.Millisecond)
			case leader:
				// heartbeat
				rf.timer.Reset(heartbeatTimeout)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me { // 不用发给自己
						continue
					}
					appendArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PreLogIndex:  0,
						PreLogTerm:   0,
						Entries:      nil, // 心跳是 nil
						LeaderCommit: rf.commitIndex,
					}
					appendReply := AppendEntriesReply{}

					// haven't implement batch sending yet
					if rf.nextIndex[i] > 0 {
						appendArgs.PreLogIndex = rf.nextIndex[i] - 1
					}
					appendArgs.PreLogTerm = rf.logs[appendArgs.PreLogIndex].LogTerm
					if len(rf.logs) > appendArgs.PreLogIndex+1 { // 因为有0，即使等于len也越界
						// appendArgs.Entries = make([]LogEntry, 0)
						// appendArgs.Entries = append(appendArgs.Entries, rf.logs[appendArgs.PreLogIndex+1])
						appendArgs.Entries = rf.logs[appendArgs.PreLogIndex+1:]
					}

					go rf.sendAppendEntries(i, &appendArgs, &appendReply)
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
	rf.logs = make([]LogEntry, 1) // 0 号 占位
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	ms := 150 + (rand.Int63() % 200)
	rf.timer = time.NewTicker(time.Duration(ms) * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	DPrintf("Server %d initialization done.\n", me)
	go rf.ticker()

	return rf
}
