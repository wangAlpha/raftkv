package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Worker definition
type Worker struct {
	mutex    sync.Mutex
	id       int
	shutdown chan bool
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
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
	call("Master.Register", &args, &reply)
	// Print worker id and status
}

// Request task
func (wk *Worker) RequestTask() {
	args := RPCArgs{}
	reply := RPCReply{}
	call("Master.AssignTask", args, &reply)
}

// 3. Request tasks
// 4. Finsh tasks
func (wk *Worker) Run() {
	for {
		// Request tasks
		// Wait Master shutdown throught request structure
	}
}

// Send heartbreak signal to master every 2s.
func (wk *Worker) HeartBreak() {
	timeoutchan := make(chan bool)
	args := RPCArgs{}
	reply := RPCReply{}
	// TODO Add some worker status information to args.
loop:
	for {
		go func() {
			<-time.After(2 * time.Second)
			timeoutchan <- true
		}()

		select {
		case <-timeoutchan:
			go call("Master.HeartBreak", args, &reply)
		case <-wk.shutdown:
			log.Println("Work id %d recevce shutdown signal", wk.id)
			break loop
		}
	}
}

func main() {
	timeoutchan := make(chan bool)

	go func() {
		<-time.After(2 * time.Second)
		timeoutchan <- true
	}()

	select {
	case <-timeoutchan:
		break
	case <-time.After(10 * time.Second):
		break
	}

	fmt.Println("Hello, playground")
}

func MakeWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *Worker {
	worker := new(Worker)

	worker.reducef = reducef
	worker.mapf = mapf

	return worker
}

//
// main/mrworker.go calls this function.
//
func RunWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("RunWorker...")
	worker := MakeWorker(mapf, reducef)
	worker.Register()   // Register worker
	worker.HeartBreak() // Start send heartbreak to worker
	worker.Run()
	// Sort file and send sorted file to the master.
	log.Println("Run Worker is close")
}

//
// send an RPC request to the master, wait for the response.
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
