package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock           sync.RWMutex
	nMap           int // the number of map tasks, we do not divide file in this simple implement
	nReduce        int // the number of reduce tasks
	runningTasks   map[string]*TaskInfo
	mapTaskPool    chan *TaskInfo
	reduceTaskPool chan *TaskInfo
	mapDone        int
	reduceDone     int
	finished       bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) initReduceTask() {
	// cause we only call it in the begining part of AskForTask
	// which already ask for a write lock,
	// so we don't need to set write lock here
	for i := 0; i < c.nReduce; i++ {
		task := &TaskInfo{
			TaskId:    generateTaskID(ReduceTask, i),
			TaskIndex: i,
			TaskType:  ReduceTask,
		}
		c.reduceTaskPool <- task
	}
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.AckTaskId != -1 { // commit Task
		taskId := generateTaskID(args.AckTaskType, args.AckTaskId)
		task, ok := c.runningTasks[taskId]
		if ok && task.WorkerId == args.WorkerId {
			if args.AckTaskType == MapTask { // handle MapTask finished
				//rename all the tempfile
				for i := 0; i < c.nReduce; i++ {
					tempFileName := GenTempMapName(task.TaskIndex, args.WorkerId, i)
					mapOutputName := GenMapOutputName(task.TaskIndex, i)
					err := os.Rename(tempFileName, mapOutputName)
					if err != nil {
						err = fmt.Errorf("failed to rename the %s, err: %e", tempFileName, err)
						return err
					}
				}
				c.mapDone++
				if c.mapDone == c.nMap {
					c.initReduceTask()
				}
			} else if args.AckTaskType == ReduceTask { // handle ReduceTask finished
				tempFileName := GenTempReduceName(args.WorkerId, task.TaskIndex)
				reduceOutputName := GenReduceOutputName(task.TaskIndex)
				err := os.Rename(tempFileName, reduceOutputName)
				if err != nil {
					err = fmt.Errorf("failed to rename the %s, err: %e", tempFileName, err)
					return err
				}
				c.reduceDone++
			}
			delete(c.runningTasks, taskId)
		}
	}
	// get the nextTask
	var task *TaskInfo
	var ok bool = false
	// fmt.Println(len(c.mapTaskPool), c.nMap, c.nReduce, c.mapDone, c.reduceDone)
	if len(c.mapTaskPool) != 0 { // allocate map task first
		task, ok = <-c.mapTaskPool
	} else if len(c.reduceTaskPool) != 0 {
		task, ok = <-c.reduceTaskPool
	}
	if !ok {
		return errors.New("Coordinator allocate task failed")
	}
	task.WorkerId = args.WorkerId
	task.DeadLine = time.Now().Add(time.Second * 10)
	c.runningTasks[task.TaskId] = task
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	reply.Task = *task
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.lock.Lock()
	defer c.lock.Unlock()
	// Your code here.
	if c.nReduce == c.reduceDone {
		ret = true
		if !c.finished {
			close(c.mapTaskPool) // close the channel
			close(c.reduceTaskPool)
		}
		c.finished = true
	}
	return ret
}

// check if any running task run out of time
func (c *Coordinator) checkWorker() {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for key, task := range c.runningTasks {
		if time.Now().After(task.DeadLine) {
			fmt.Printf("%v worker's %s run out of time\n", task.WorkerId, task.TaskId)
			delete(c.runningTasks, key)
			task.WorkerId = 0
			if task.TaskType == MapTask {
				c.mapTaskPool <- task
			} else if task.TaskType == ReduceTask {
				c.reduceTaskPool <- task
			}
		}
	}
}

// func max(x, y int) int {
// 	if x > y {
// 		return x
// 	} else {
// 		return y
// 	}
// }

func generateTaskID(typ TaskTypeNum, idx int) string {
	var taskName string = "None"
	if typ == MapTask {
		taskName = "Map"
	} else if typ == ReduceTask {
		taskName = "Reduce"
	}
	return fmt.Sprintf("%s-%v", taskName, idx)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:        nReduce,
		nMap:           len(files),
		mapDone:        0,
		reduceDone:     0,
		runningTasks:   make(map[string]*TaskInfo),
		mapTaskPool:    make(chan *TaskInfo, len(files)),
		reduceTaskPool: make(chan *TaskInfo, nReduce),
		finished:       false,
	}

	// Your code here.
	for idx, file := range files {
		task := &TaskInfo{
			TaskId:    generateTaskID(MapTask, idx),
			TaskIndex: idx,
			TaskType:  MapTask,
			FileName:  file,
		}
		c.mapTaskPool <- task
	}
	c.server()
	go func() {
		for {
			if c.Done() {
				return
			}
			c.checkWorker()
			time.Sleep(time.Second)
		}
	}()
	return &c
}
