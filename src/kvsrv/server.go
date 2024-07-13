package kvsrv

import (
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type entry struct {
	reply      string
	createTime time.Time
}

type recordKey struct {
	clientId int16
	seqNum   int64
}

type KVServer struct {
	mu         sync.Mutex
	maxHistory time.Duration
	kvMap      map[string]string
	lastReply  map[recordKey]entry // recordId = <clientId> + "-" + <seqNum>
	// Your definitions here.
}

// func getRecordId(ClientId int64, SeqNum int64) string {
// 	return fmt.Sprintf("%d-%d", ClientId, SeqNum)
// }

// func splitRecordId(recordId string) (int64, int64) {
// 	parts := strings.Split(recordId, "-")
// 	clientId, _ := strconv.ParseInt(parts[0], 10, 64)
// 	seqNum, _ := strconv.ParseInt(parts[1], 10, 64)
// 	return clientId, seqNum
// }

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//recordId := getRecordId(args.ClientId, args.SeqNum)
	// recordId := recordKey{args.ClientId, args.SeqNum}
	// preReply, exist := kv.lastReply[recordId]
	// if exist {
	// 	reply.Value = preReply.reply
	// 	return
	// }
	value, exist := kv.kvMap[args.Key]
	if exist {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	// kv.lastReply[recordId] = entry{
	// 	reply:      reply.Value,
	// 	createTime: time.Now(),
	// }
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// recordId := getRecordId(args.ClientId, args.SeqNum)
	recordId := recordKey{args.ClientId, args.SeqNum}
	_, exist := kv.lastReply[recordId]
	if exist { // we have done this record before
		//reply.Value = preReply.reply
		return
	}
	// value, exist := kv.kvMap[args.Key]
	kv.kvMap[args.Key] = args.Value
	// cause the test do not need put operation to have a reply
	// if exist {
	// 	reply.Value = value
	// } else {
	// 	reply.Value = ""
	// }
	kv.lastReply[recordId] = entry{
		//	reply:      reply.Value,
		createTime: time.Now(),
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//recordId := getRecordId(args.ClientId, args.SeqNum)
	recordId := recordKey{args.ClientId, args.SeqNum}
	preReply, exist := kv.lastReply[recordId]
	if exist { // we have done this record before
		reply.Value = preReply.reply
		return
	}
	value, exist := kv.kvMap[args.Key]
	kv.kvMap[args.Key] = value + args.Value
	if exist {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.lastReply[recordId] = entry{
		reply:      reply.Value,
		createTime: time.Now(),
	}
}

func (kv *KVServer) cleanCache() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//cnt := 0
	for recordId, record := range kv.lastReply {
		now := time.Now()
		if now.After(record.createTime.Add(kv.maxHistory)) {
			delete(kv.lastReply, recordId)
			//cnt++
		}
	}
	//fmt.Println(cnt)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.lastReply = make(map[recordKey]entry)
	kv.maxHistory = time.Millisecond * 75
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			kv.cleanCache()
		}
	}()
	return kv
}
