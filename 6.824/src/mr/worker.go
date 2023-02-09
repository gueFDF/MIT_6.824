package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//

// hash 根据key进行hash,从而分配给哪一个reduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	resquest := Request{}
	response := Response{}
	ack1 := Ack{}
	ack2 := Ack{}
	for {
		p := call("Coordinator.GetTask", resquest, &response)
		task := response.Task
		switch task.TaskType {
		case "map":
			log.Println("拿到了map任务", task.TaskType)
			ack1.Time = task.T
			call("Coordinator.SendAck", ack1, &ack2)
		case "reduce":
			log.Println("拿到了reduce任务", response.Task.TaskType)
			ack1.Time = task.T
			call("Coordinator.SendAck", ack1, &ack2)
		case "wait":
			time.Sleep(time.Second * 2)
		case "exit":
			log.Println("All tasks is finished")
			return
		default:
			log.Println("TaskType is err 1", p, response.Task.TaskType)
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
