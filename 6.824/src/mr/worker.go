package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		response = Response{}
		p := call("Coordinator.GetTask", resquest, &response)
		task := response.Task
		switch task.TaskType {
		case "map":
			log.Println("拿到了map任务", task.TaskType, " ", task.Id)
			ack1.Id = task.Id
			doMap(mapf, task)
			call("Coordinator.SendAck", ack1, &ack2)
		case "reduce":
			log.Println("拿到了reduce任务", task.TaskType, " ", task.Id)
			log.Println(task)
			ack1.Id = task.Id
			time.Sleep(time.Second)
			call("Coordinator.SendAck", ack1, &ack2)
		case "wait":
			log.Println("拿到了wait任务", task.TaskType)
			time.Sleep(time.Second * 2)
		case "exit":
			log.Println("All tasks is finished")
			return
		default:
			log.Println("TaskType is err 1", p, response.Task.TaskType)
		}
	}

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

func doMap(mapf func(string, string) []KeyValue, task MRTask) {
	//oname := "mr-" + strconv.Itoa(int(task.Id))
	oname := "mr-" + "temp"
	var files []*json.Encoder
	for i := 0; i < task.Nreduce; i++ {
		name := oname + strconv.Itoa(i)
		ofile, e := ioutil.TempFile("./","")
		os.Rename(ofile.Name(),name)

	//	defer os.Remove(name)
		if e != nil {
			log.Panic("Creat file err int map")
		}
		enc := json.NewEncoder(ofile)
		files = append(files, enc)
	}
	var argMap Arg_map
	err := json.Unmarshal([]byte(task.Arg), &argMap)
	if err != nil {
		log.Println("arg Decod err in map")
	}
	arrkv := mapf(argMap.FileName, argMap.Content)
	kvs := make([]KeyValue, 0)
	for _, kv := range arrkv {
		kvs = append(kvs, kv)
	}

	//排序
	sort.Sort(ByKey(kvs))
	for _, kv := range kvs {
		err := files[ihash(kv.Key)%task.Nreduce].Encode(kv)
		if err != nil {
			log.Println("encode is err in map")
		}
	}

}
