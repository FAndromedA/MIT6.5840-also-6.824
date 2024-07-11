package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// Task Type Options
type TaskTypeNum uint8

const (
	NoneTask     TaskTypeNum = 0
	MapTask      TaskTypeNum = 1
	ReduceTask   TaskTypeNum = 2
	FinishedTask TaskTypeNum = 3
)

type AskForTaskArgs struct {
	WorkerId    int
	AckTaskId   int
	AckTaskType TaskTypeNum
}

type TaskInfo struct {
	TaskId    string
	TaskIndex int
	WorkerId  int
	TaskType  TaskTypeNum
	FileName  string
	DeadLine  time.Time
}

// https://www.topgoer.com/%E5%BE%AE%E6%9C%8D%E5%8A%A1/RPC.html
type AskForTaskReply struct { // rpc 要求所有都是exported 包括 TaskInfo里的属性也应该都是exported
	// error: gob: type mr.AskForTask Args has no exported fields
	Task    TaskInfo
	NReduce int
	NMap    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
