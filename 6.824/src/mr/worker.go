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
		call("Coordinator.GetTask", resquest, &response)
		task := response.Task
		switch task.TaskType {
		case "map":
			ack1.Id = task.Id
			filename := doMap(mapf, task)
			ack1.Filenames = filename
			call("Coordinator.SendAck", ack1, &ack2)
		case "reduce":
			ack1.Id = task.Id
			doreduce(reducef, &task)
			call("Coordinator.SendAck", ack1, &ack2)
		case "wait":
			//time.Sleep(time.Second*1)
		case "exit":
			return
		default:
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

func doMap(mapf func(string, string) []KeyValue, task MRTask) []string {
	oname := "mr-" + strconv.Itoa(int(task.Id)) + "-"
	filenames := make([]string, 0)
	//oname := "mr-" + "temp"
	var files []*json.Encoder
	for i := 0; i < task.Nreduce; i++ {
		name := oname + strconv.Itoa(i)
		ofile, e := ioutil.TempFile("./", "")
		os.Rename(ofile.Name(), name)

		filenames = append(filenames, name)
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

	for _, kv := range arrkv {
		err := files[ihash(kv.Key)%task.Nreduce].Encode(kv)
		if err != nil {
			log.Println("encode is err in map")
		}
	}

	return filenames
}

func doreduce(reducef func(string, []string) string, task *MRTask) {

	var argReduce Arg_reduce
	err := json.Unmarshal([]byte(task.Arg), &argReduce)
	if err != nil {
		log.Println("arg Decod err in map")
	}

	var kvs []KeyValue
	for _, filename := range argReduce.Filenames {
		defer os.Remove(filename) //删除临时文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	name := "mr-out-" + strconv.Itoa(task.Nreduce) //最终输出文件名

	ofile, _ := os.Create(name) //创建最终文件
	defer ofile.Close()

	//排序
	sort.Sort(ByKey(kvs))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		//进行reduce
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

}
