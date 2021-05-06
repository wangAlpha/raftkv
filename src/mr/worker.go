package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Worker definition
type Worker struct {
	mutex      sync.Mutex
	id         int
	status     TaskPhase // Execution status
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
	args := RPCArgs{}
	reply := RPCReply{}
	call("Master.Register", &args, &reply)
	log.Printf("%+v", reply)
	wk.id = reply.Id

	log.Printf("Worker id: %v, state: %v\n", reply.Id, reply.WorkerState.TaskState)
}

// Request task
func (wk *Worker) RequestTask() []string {
	log.Println("Request a task")
	args := RPCArgs{}
	reply := RPCReply{}
	call("Master.AssignTask", args, &reply)
	// TODO Get request tasks
	wk.taskType = reply.Taskf
	if reply.Taskf == "shutdown" {
		wk.shutdown = true
	}
	log.Printf("return code: %t\n", reply.ReturnCode == Ok)
	return reply.Files
}

func (wk *Worker) ExecuteTask(files []string) {
	log.Printf("Worker %d execute %s task.\n", wk.id, wk.taskType)
	if wk.taskType == "mapf" {
		time.Sleep(time.Second * 1)
	} else if wk.taskType == "reducef" {
		time.Sleep(time.Second * 1)
	}
	log.Printf("files: %+v", files)
}

// Return work id and output file
func (wk *Worker) Done() {
	args := RPCArgs{}
	reply := RPCReply{}

	args.Id = wk.id
	args.State = InCompleted
	// 更新任务数据结构
	call("Master.WorkDone", args, &reply)
}

// 3. Request tasks
// 4. Finsh tasks
func (wk *Worker) Run() {
	log.Printf("Worker %d is runing\n", wk.id)
	for {
		// Request asynchronous tasks
		files := wk.RequestTask() // 请求的啥任务
		if !wk.shutdown {
			wk.ExecuteTask(files)
			wk.Done() // Send a done signal to master
		} else {
			break
		}
		// 得到任务执行
		// 假如不是任务是shutdown，则发送关机命令
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
				args.Id = wk.id
				call("Master.HeartBreak", args, &reply)
				// Register worker if worker is crashed.
				if reply.ReturnCode != Ok {
					call("Master.Register", args, &reply)
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

	worker.id = 1
	worker.reducef = reducef
	worker.mapf = mapf

	return worker
}

func (wk *Worker) Shutdown() {
	// wk.CleanupFiles()
	args := RPCArgs{}
	reply := RPCReply{}
	args.Id = wk.id
	call("Master.FinalAck", args, &reply)
	wk.final_chan <- true
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

func doMap(jobName string,
	mapTask int,
	inFile string,
	nReduce int,
	mapF func(filename string, contents string)) {

	file, err := os.Open(inFile)
	checkError(err, fmt.Sprintf("Failed to open input file %s.", inFile))
	info, err := file.Stat()
	checkError(err, "Failed to get input file info.")
	contents := make([]byte, info.Size())
	file.Read(contents)
	file.Close()

	mappedResult := Map(inFile, string(contents))

	encoders := make([]*json.Encoder, nReduce)
	for i := range encoders {
		// filename := "mr-tmp-map-" + strconv.Itoa(ihash(kv.Key)%s.NReduce+1)
		filename := "mr-tmp-map" + jobName + string(mapTask) + string(i)
		// filename := reduceName(jobName, mapTask, i)
		file, err := os.Create(filename)
		checkError(err, "Failed to create intermediate file.")
		defer file.Close()
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range mappedResult {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Failed to encode key-value pair.")
	}
}

func Map(filename string, contents string) []KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
