package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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
type KeyValues struct {
	Key   string
	Value []string
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
	log.Printf("Worker id:%d Request a task", wk.id)
	args := RPCArgs{}
	reply := RPCReply{}
	args.Id = wk.id
	call("Master.AssignTask", args, &reply)
	wk.taskType = reply.Taskf
	wk.shutdown = (reply.Taskf == "Shutdown")
	log.Printf("Worker id:%v, taskf:%s, files:%s", wk.id, wk.taskType, reply.Files)
	return reply.Files
}

func (wk *Worker) DoTask(files []string) []string {
	log.Printf("Worker %d execute %s task.\n", wk.id, wk.taskType)
	outFiles := []string{}
	if wk.taskType == "Mapf" {
		wk.map_files = append(wk.map_files, files...)
		nFile := len(wk.map_files)
		for _, file := range files {
			outFile := DoMap(wk.id, file, wk.nReduce, nFile)
			outFiles = append(outFiles, outFile...)
		}
	} else if wk.taskType == "Reducef" {
		wk.reduce_files = append(wk.reduce_files, files...)
		outFile := DoReduce(files, wk.id)
		outFiles = append(outFiles, outFile)
	} else if wk.taskType == "Wait" {
		time.Sleep(75 * time.Millisecond)
	}
	return outFiles
}

// Return work id and intermediate file
func (wk *Worker) Done(inFiles []string) {
	args := RPCArgs{}
	reply := RPCReply{}

	wk.status = InCompleted
	args.Id = wk.id
	if wk.taskType == "Mapf" {
		args.Phase = MapPhase
	} else if wk.taskType == "Reducef" {
		args.Phase = ReducePhase
	}
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
			outFiles := wk.DoTask(files)
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
	// for _, f := range wk.map_files {
	// 	os.Remove(f)
	// }
	// for _, f := range wk.reduce_files {
	// 	os.Remove(f)
	// }
	wk.map_files = wk.map_files[:0]
	wk.reduce_files = wk.reduce_files[:0]
	wk.final_chan <- true
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
	log.Printf("Run Worker %d is close", worker.id)
}

// Map to nReduce intermediate files
func DoMap(workId int32, inFile string, nReduce int, nFile int) []string {
	file, err := os.Open(inFile)
	checkError(err, fmt.Sprintf("Failed to open input file %s", inFile))
	contents, err := ioutil.ReadAll(file)
	checkError(err, "Failed to file contens.")
	// contents := make([]byte, info.Size())
	// file.Read(contents)
	file.Close()

	mapResult := Map(inFile, string(contents))
	encoders := make([]*json.Encoder, nReduce)

	files := make([]string, nReduce)
	for i := range encoders {
		filename := fmt.Sprintf("mr-map-out-%d-%d-%d", workId, nFile, i)
		ff, err := os.Create(filename)
		checkError(err, "Failed to create a intermeditate file.")
		files[i] = filename
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

// DoReduce, input a set of KV, output a count file
func DoReduce(inFiles []string, workId int32) string {
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

	filename := fmt.Sprintf("mr-reduce-out-%d", workId)
	ff, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	checkError(err, "Failed to open a append file.")
	defer ff.Close()
	for k, list := range keyValues {
		ff.WriteString(fmt.Sprintf("%s %d\n", k, len(list)))
	}
	// encoder := json.NewEncoder(ff)
	// err = encoder.Encode(&keyValues)
	// checkError(err, "Failed to encoder keyValue file.")

	return filename
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
