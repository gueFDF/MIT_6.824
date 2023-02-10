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
	"time"
)

// 枚举任务类型
const (
	TYPE_MAP    = "map"
	TYPE_REDUCE = "reduce"
	TYPE_WAIT   = "wait"
	TYPE_EXIT   = "exit"
)

// 任务ID
var ID uint32 = 0

// 封装任务
type MRTask struct {
	TaskType string //任务类型
	Id       uint32 //任务ID
	Arg      string //参数
	Nreduce  int
	Iserr    chan struct{} //判断是否超时
}
type Arg_map struct {
	FileName string
	Content  string
}
type Arg_reduce struct {
	Filenames []string
}

// 用于等待的空任务
var WaitTask = &MRTask{
	TaskType: TYPE_WAIT,
}

// 用于退出的空任务
var ExitTask = &MRTask{
	TaskType: TYPE_EXIT,
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

// 获取长度
func (t *Task_queue) getLen() uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return uint32(len(t.tasks))
}

type Coordinator struct {
	map_queue Task_queue         //map任务队列
	ReduceMap map[uint32]*MRTask //存放reduce任务
	Nreduce   int
	isMap     bool //Map任务是否已经全部完成
	isReduce  bool //Reduce任务是否已经全部完成
	Map_ing   map[uint32]*MRTask
	mu        sync.Mutex //用来维护map_ing
}

// 用于RPC远程调用
func (c *Coordinator) GetTask(request Request, response *Response) error {
	var err error
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isMap {
		//先判断map任务是否已经全部完成
		//未全部完成
		task, err := c.map_queue.getTask()
		if err != nil {
			response.Task = *WaitTask
		} else {
			c.Map_ing[task.Id] = task
			response.Task = *task
			go c.timeoutTask(time.Second*10, task) //子协程处理任务超时
		}
	} else if !c.isReduce {
		//map任务已经全部完成，reduce任务未全部完成

		// len := len(c.ReduceMap)
		// task, ok := c.ReduceMap[uint32(len-1)]
		var task *MRTask=nil

		for k, m := range c.ReduceMap {
			delete(c.ReduceMap, uint32(k))
			task=m
			break
		}
		if task==nil {
			response.Task = *WaitTask
		} else {
			c.Map_ing[task.Id] = task
			response.Task = *task
			go c.timeoutTask(time.Second*10, task) //子协程处理任务超时
		}
	} else {
		response.Task = *ExitTask
	}
	return err
}

// 处理任务超时
func (c *Coordinator) timeoutTask(t time.Duration, task *MRTask) {
	select {
	case <-time.After(t):
		//超时
		c.mu.Lock()
		delete(c.Map_ing, task.Id)     //在进行队列中删除
		if task.TaskType == TYPE_MAP { //判断任务类型，加回相应任务
			c.map_queue.add(task)
		} else if task.TaskType == TYPE_REDUCE {
			c.ReduceMap[uint32(task.Nreduce)] = task
		}
		c.mu.Unlock()
	case <-task.Iserr:
		return
	}
}


//用来任务确认的RPC调用
func (c *Coordinator) SendAck(request Ack, response *Ack) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.Map_ing[request.Id]
	if !ok {
		log.Println(request.Id, " SendAck error,未找到对应任务")
		return nil
	}
	task.Iserr <- struct{}{}
	//判断是什么任务
	switch task.TaskType {
	case TYPE_MAP:
		for i := 0; i < c.Nreduce; i++ {
			task := c.ReduceMap[uint32(i)]
			var arg Arg_reduce
			err := json.Unmarshal([]byte(task.Arg), &arg)
			if err != nil {
				log.Println(err, " unmarshal err in SendAck")
			}
			arg.Filenames = append(arg.Filenames, request.Filenames[i])
			pra, _ := json.Marshal(arg)
			task.Arg = string(pra)
			c.ReduceMap[uint32(i)] = task
		}
		delete(c.Map_ing, request.Id)
		// log.Println(task.Id, " Map 被删除，加入Reduce")
	case TYPE_REDUCE:
		delete(c.Map_ing, request.Id)
	}

	response = nil

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", "localhost:9999")
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	if !c.isMap {
		if c.map_queue.getLen() == 0 && len(c.Map_ing) == 0 {
			c.isMap = true
		}
	} else if !c.isReduce {
		if len(c.ReduceMap) == 0 && len(c.Map_ing) == 0 {
			c.isReduce = true
		}
	} else {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		isMap:     false,
		isReduce:  false,
		Map_ing:   make(map[uint32]*MRTask),
		Nreduce:   nReduce,
		ReduceMap: make(map[uint32]*MRTask),
	}

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
		pra := Arg_map{
			FileName: filename,
			Content:  string(content),
		}
		temp, _ := json.Marshal(pra)
		c.map_queue.add(&MRTask{
			TaskType: TYPE_MAP,
			Id:       ID,
			Arg:      string(temp),
			Nreduce:  nReduce,
			Iserr:    make(chan struct{}, 1),
		})

		ID++
	}
	for i := 0; i < nReduce; i++ {
		arg := Arg_reduce{make([]string, 0)}
		pra, _ := json.Marshal(arg)
		task := &MRTask{
			TaskType: TYPE_REDUCE,
			Arg:      string(pra),
			Id:       ID,
			Nreduce:  i,
			Iserr:    make(chan struct{}, 1),
		}
		c.ReduceMap[uint32(i)] = task
		ID++
	}

	c.server()
	return &c
}
