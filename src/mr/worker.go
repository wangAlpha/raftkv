package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Worker definition
type Worker struct {
	// mutex      sync.Mutex
	id       int32
	status   TaskPhase // Execution status
	taskType string
	nReduce  int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string

	shutdown   bool
	final_chan chan bool

	map_files    []string // map phase intermediate files
	reduce_files []string // reduce phase intermediate files
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
	wk.nReduce = reply.NReduce

	log.Printf("Worker id: %d, state: %v\n", reply.Id, reply.WorkerState.TaskState)
}

// Request task
func (wk *Worker) RequestTask() []string {
	log.Println("Request a task")
	args := RPCArgs{}
	reply := RPCReply{}
	args.Id = wk.id
	call("Master.AssignTask", args, &reply)
	// TODO Get request tasks
	wk.taskType = reply.Taskf
	if reply.Taskf == "shutdown" {
		wk.shutdown = true
	}
	log.Printf("Worker id:%v, taskf:%s, files:%s", wk.id, wk.taskType, reply.Files)
	log.Printf("return code: %t\n", reply.ReturnCode == Ok)
	return reply.Files
}

func (wk *Worker) ExecuteTask(files []string) []string {
	log.Printf("Worker %d execute %s task.\n", wk.id, wk.taskType)
	outFiles := []string{}
	if wk.taskType == "mapf" {
		wk.map_files = append(wk.map_files, files...)
		for _, file := range files {
			outFile := doMap(wk.id, file, wk.nReduce)
			outFiles = append(outFiles, outFile...)
		}
	} else if wk.taskType == "reducef" {
		wk.reduce_files = append(wk.reduce_files, files...)
		outFile := doReduce(files, wk.id)
		if len(outFile) != 0 {
			outFiles = append(outFiles, outFile)
		}
	}
	return outFiles
}

// Return work id and intermediate file
func (wk *Worker) Done(inFiles []string) {
	args := RPCArgs{}
	reply := RPCReply{}

	wk.status = InCompleted
	args.Id = wk.id
	args.State = InCompleted
	args.OutPath = inFiles
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
			outFiles := wk.ExecuteTask(files)
			wk.Done(outFiles) // Send a done signal to master
			log.Printf("Worker %d, len: %d, outFiles %v", wk.id, len(outFiles), outFiles)
		} else {
			break
		}
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

func (wk *Worker) CleanupFiles() {
	for _, f := range wk.map_files {
		os.Remove(f)
	}
	for _, f := range wk.reduce_files {
		os.Remove(f)
	}
	wk.map_files = wk.map_files[:0]
	wk.reduce_files = wk.reduce_files[:0]
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
	log.Printf("Worker id:%d shutdown.", wk.id)
	// args := RPCArgs{}
	// reply := RPCReply{}
	// args.Id = wk.id
	wk.CleanupFiles()
	// call("Master.FinalAck", args, &reply)
	// wk.final_chan <- true
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

// Map to nReduce intermediate files
func doMap(workId int32, inFile string, nReduce int) []string {
	file, err := os.Open(inFile)
	checkError(err, fmt.Sprintf("Failed to open input file %s", inFile))
	info, err := file.Stat()
	checkError(err, "Failed to read file info.")
	contents := make([]byte, info.Size())
	file.Read(contents)
	file.Close()

	mapResult := Map(inFile, string(contents))
	encoders := make([]*json.Encoder, nReduce)

	files := make([]string, nReduce)
	for i := range encoders {
		filename := fmt.Sprintf("mr-map-out-%d-%d", workId, i)
		ff, err := os.Create(filename)
		checkError(err, "Failed to create a intermeditate file.")
		files = append(files, filename)
		defer ff.Close()
		encoders[i] = json.NewEncoder(ff)
	}

	for _, kv := range mapResult {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Failed to encoder intermediate file.")
	}
	return files
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

// doReduce, input a set of KV, output a count file
func doReduce(inFiles []string, workId int32) string {
	if len(inFiles) == 0 {
		return ""
	}
	keyValues := make(map[string][]string)
	log.Printf("Worker ID:%d, inFiles: %v, len: %d", workId, inFiles, len(inFiles))
	for _, file := range inFiles {
		ff, err := os.Open(file)
		checkError(err, fmt.Sprintf("Failed to open file %s", inFiles))
		defer ff.Close()
		decoder := json.NewDecoder(ff)
		kv := KeyValue{}
		for decoder.More() {
			err := decoder.Decode(&kv)
			checkError(err, "Failed to decoder kv.")
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	filename := fmt.Sprintf("mr-out-%d", workId)
	ff, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	checkError(err, "Failed to open a append file.")

	for k, list := range keyValues {
		ff.WriteString(fmt.Sprintf("%s %s\n", k, Reduce(k, list)))
	}
	return filename
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
