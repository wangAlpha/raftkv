package mr

import (
	"hash/fnv"
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Worker definition
type Worker struct {
	mutex      sync.Mutex
	id         int
	state      TaskPhase
	shutdown   bool
	final_chan chan bool
	taskType   string
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (wk *Worker) Register() {
	log.Println("Worker register")
	var args RPCArgs
	var reply RPCReply
	call("Master.Register", args, &reply)
	log.Printf("Worker id: %v, state: %v\n", reply.id, reply.state)
}

// Request task
func (wk *Worker) RequestTask() {
	args := RPCArgs{}
	reply := RPCReply{}
	call("Master.AssignTask", args, &reply)
	// TODO Get request tasks
	log.Printf("return code: %t\n", reply.return_code == Ok)
}

func (wk *Worker) UpdateStatus() {
	log.Printf("Update worker %v status\n", wk.id)
	args := RPCArgs{}
	reply := RPCReply{}
	call("Master.UpdateWorkerState", args, &reply)
	// Update worker status
}

func (wk *Worker) ExecuteTask() {
	// if
	// 判断是mapF还是reduceF任务
	// 执行
}

func (wk *Worker) Done() {

	args := RPCArgs{}
	reply := RPCReply{}
	args.id = wk.id
	args.state = wk.state
	// 更新任务数据结构
	count_state := 0
	call("Master.DoneWork", args, &reply)
}

// 3. Request tasks
// 4. Finsh tasks
func (wk *Worker) Run() {
	log.Printf("Worker %d is runing\n", wk.id)
	for {
		// Request asynchronous tasks
		wk.RequestTask() // 请求的啥任务
		if wk.shutdown {
			break
		}
		// 得到任务执行
		// 假如不是任务是shutdown，则发送关机命令
		wk.ExecuteTask()
		wk.Done() // Send a done signal to master
	}
}

// Send heartbreak signal to master every 2s.
func (wk *Worker) HeartBreak() {
	timeoutchan := make(chan bool)
loop:
	for {
		go func() {
			<-time.After(2 * time.Second)
			timeoutchan <- true
		}()

		select {
		case <-timeoutchan:
			go func() {
				args := RPCArgs{}
				reply := RPCReply{}
				call("Master.HeartBreak", args, &reply)
				// Register worker if worker is crashed.
				if reply.return_code != Ok {
					call("Master.RegisterWorker", args, &reply)
				}
			}()
		case <-wk.final_chan:
			log.Printf("Work id %+v recevce shutdown signal\n", wk.id)
			break loop
		}
	}
}

func MakeWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Worker {
	worker := new(Worker)

	worker.reducef = reducef
	worker.mapf = mapf

	return worker
}

func (wk *Worker) Shutdown() {
	// wk.CleanupFiles()
	args := RPCArgs{}
	reply := RPCReply{}
	call("Master.FinalAck", args, &reply)
}

//
// main/mrworker.go calls this function.
//
func RunWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("RunWorker...")
	worker := MakeWorker(mapf, reducef)
	worker.Register()      // Register worker
	go worker.HeartBreak() // Start send heartbreak to worker
	worker.Run()
	worker.Shutdown()
	// Sort file and send sorted file to the master.
	log.Println("Run Worker is close")
}

//
// Send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
