package mr

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// 枚举任务类型
const (
	TYPE_MAP    = "map"
	TYPE_REDUCE = "reduce"
	TYPE_WAIT   = "wait"
	TYPE_EXIT   = "exit"
)

// 枚举任务状态
const (
	ST_WAIT = iota
	ST_ING
	ST_FIN
)

// 封装任务
type MRTask struct {
	TaskType string //任务类型
	Status   string //任务状态
	Arg      string //参数
	Nreduce  int
}
type arg_map struct {
	FileName string
	Content  string
}

// 用于等待的空任务
var WaitTask = &MRTask{
	TaskType: TYPE_WAIT,
}

// 用于退出的空任务
var ExitTask = &MRTask{
	TaskType: TYPE_EXIT,
}

type arg_reduce struct {
	//TODO:后面实现
}

// 任务队列
type Task_queue struct {
	mu    sync.Mutex
	tasks []*MRTask //任务队列
}

// 添加任务
func (t *Task_queue) add(new *MRTask) {
	if new == nil {
		log.Println("add task is nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks = append(t.tasks, new)
}

// 获取任务
func (t *Task_queue) getTask() (*MRTask, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.tasks) == 0 {
		return nil, errors.New("暂无任务")
	} else {
		task := t.tasks[0]
		t.tasks = t.tasks[1:]
		return task, nil
	}
}

type Coordinator struct {
	map_queue    Task_queue //map任务队列
	reduce_queue Task_queue //reduce任务队列
	Nreduce      int
	isMap        bool //Map任务是否已经全部完成
	isReduce     bool //Reduce任务是否已经全部完成
}

// 用于RPC远程调用
func (c *Coordinator) GetTask(request Request, response *Response) error {
	var err error
	if !c.isMap {
		//先判断map任务是否已经全部完成
		//未全部完成
		task, err := c.map_queue.getTask()
		if err == nil {
			request.Task = *WaitTask
		} else {
			request.Task = *task
		}
	} else if !c.isReduce {
		//map任务已经全部完成，reduce任务未全部完成
		task, err := c.map_queue.getTask()
		if err != nil {
			request.Task = *WaitTask
		} else {
			request.Task = *task
		}
	} else {
		request.Task = *ExitTask
	}
	return err
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
	ret := true

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Nreduce = nReduce
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		pra := arg_map{
			FileName: filename,
			Content:  string(content),
		}
		c.map_queue.add(&MRTask{
			
		})
	}

	c.server()
	return &c
}
