package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int32
	SeqNum   int64
	ServerId int
	// LastAck  LastReply
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int32
	SeqNum   int64
	// LastAck  LastReply
}

type GetReply struct {
	Err   Err
	Value string
}

type LastReply struct {
	seqNum int64
	reply  GetReply // for get or append this should be nil
}
