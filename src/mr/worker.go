package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []string

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i] < a[j] }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GenTempMapName(taskIndex int, workerId int, reducerId int) string {
	return fmt.Sprintf("mr-map-%v-%v-%v", taskIndex, workerId, reducerId)
}

func doMapTask(reply *AskForTaskReply, workerId int, mapf func(string, string) []KeyValue) error {
	task := reply.Task
	srcFile, err := os.Open(task.FileName)
	if err != nil {
		err = fmt.Errorf("%v failed to open the source file %s, err: %e", workerId, task.FileName, err)
		return err
	}
	content, err := io.ReadAll(srcFile)
	if err != nil {
		err = fmt.Errorf("%v failed to read the source file %s, err: %e", workerId, task.FileName, err)
		return err
	}
	results := mapf(task.FileName, string(content)) // 返回[]KeyValue
	imMap := make(map[int][]KeyValue)
	for _, kv := range results {
		index := ihash(kv.Key) % reply.NReduce
		imMap[index] = append(imMap[index], kv)
	}
	for i := 0; i < reply.NReduce; i++ {
		tempFileName := GenTempMapName(task.TaskIndex, workerId, i)
		intermediateFile, _ := os.Create(tempFileName)
		for _, kv := range imMap[i] {
			_, err := fmt.Fprintf(intermediateFile, "%s\t%s\n", kv.Key, kv.Value)
			if err != nil {
				err = fmt.Errorf("write file: %s failed, err: %e", tempFileName, err)
				intermediateFile.Close()
				return err
			}
		}
		intermediateFile.Close()
	}
	return nil
}

func GenMapOutputName(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-map-%v-%v", mapId, reduceId)
}

func GenTempReduceName(workerId int, reduceId int) string {
	return fmt.Sprintf("mr-reduce-%v-%v", workerId, reduceId)
}

func GenReduceOutputName(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}

func doReduceTask(reply *AskForTaskReply, workerId int,
	reducef func(string, []string) string) error {
	task := reply.Task
	var files []string
	for i := 0; i < reply.NMap; i++ { // traverse all the map task
		inputFileName := GenMapOutputName(i, task.TaskIndex)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			err = fmt.Errorf("%v failed to open the redudce input file %s, err: %e", workerId, inputFileName, err)
			inputFile.Close()
			return err
		}
		content, err := io.ReadAll(inputFile)
		if err != nil {
			err = fmt.Errorf("%v failed to read the input file %s, err: %e", workerId, inputFileName, err)
			inputFile.Close()
			return err
		}
		files = append(files, string(content))
		inputFile.Close()
	}
	imMap := make(map[string][]string)
	var keyArray []string
	for _, file := range files {
		lines := strings.Split(file, "\n")
		for _, line := range lines {
			parts := strings.Split(line, "\t")
			if len(parts) == 1 {
				continue
			}
			if _, ok := imMap[parts[0]]; !ok {
				keyArray = append(keyArray, parts[0])
			}
			imMap[parts[0]] = append(imMap[parts[0]], parts[1])
		}
	}
	sort.Sort(ByKey(keyArray))
	tempFileName := GenTempReduceName(workerId, task.TaskIndex)
	tempOutputFile, err := os.Create(tempFileName)
	if err != nil {
		err = fmt.Errorf("%v failed to open the redudce temp output file %s, err: %e", workerId, tempFileName, err)
		tempOutputFile.Close()
		return err
	}
	for _, key := range keyArray {
		output := reducef(key, imMap[key])
		// fmt.Printf("%s \\ %d \\\\", key, len(imMap[key]))
		_, err = fmt.Fprintf(tempOutputFile, "%s %s\n", key, output)
		if err != nil {
			err = fmt.Errorf("%v failed to write the redudce temp output file %s, err: %e", workerId, tempFileName, err)
			tempOutputFile.Close()
			return err
		}
	}
	tempOutputFile.Close()
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := os.Getpid()
	fmt.Printf("Worker %v start!\n", workerId)
	// Your worker implementation here.
	previousTaskType := NoneTask
	previousTaskId := -1
	for {
		args := AskForTaskArgs{
			WorkerId:    workerId,
			AckTaskId:   previousTaskId,
			AckTaskType: previousTaskType,
		}
		reply := AskForTaskReply{}
		ok := call("Coordinator.AskForTask", &args, &reply)

		if ok {
			// fmt.Println(reply.Task.TaskId, reply.Task.TaskType, reply.Task.WorkerId, workerId)
			switch reply.Task.TaskType {
			case FinishedTask:
				break
			case MapTask:
				{
					err := doMapTask(&reply, workerId, mapf)
					if err != nil {
						fmt.Printf("%v failed to execute task %s, err: %e\n", workerId, reply.Task.TaskId, err)
						continue
					}
					// 成功执行才提交
					previousTaskType = reply.Task.TaskType
					previousTaskId = reply.Task.TaskIndex
				}
			case ReduceTask:
				{
					err := doReduceTask(&reply, workerId, reducef)
					if err != nil {
						fmt.Printf("%v failed to execute task %s, err: %e\n", workerId, reply.Task.TaskId, err)
						continue
					}
					previousTaskType = reply.Task.TaskType
					previousTaskId = reply.Task.TaskIndex
				}
			case NoneTask:
				fallthrough
			default:
				{
					fmt.Println("Failed to Get or Commit Task from Coordinator, retry 1 second later")
					time.Sleep(time.Second)
					continue
				}
			}

		} else {
			fmt.Println("Failed to Get Task from Coordinator, retry 1 second later")
			time.Sleep(time.Second)
			continue
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
