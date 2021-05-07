package mr

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// 1. Make new master
// 2. start a rpc stub
// 3. Wait worker register
// 		Wait heartbreak from worker and report worker status
// 4. Assign map tasks to workers
// 5. Wait map tasks is finish, and resend failure tasks to workers
// 6. Assign reduce tasks to worker
// 7. Wait reduce tasks is finish, and resend failure tasks to workers
// 8. Sort files
// 9. Send shutdown signals and cleanup intermediate files
// Phase is master Phase, it
// Worker state

// Master's Type definitions here.
type Master struct {
	mu    sync.RWMutex
	mu1   sync.RWMutex
	phase TaskPhase

	workers      map[int32]*WorkerState // worker name and status
	sockname     string
	name         string
	reduce_files []string // Reduce phase files
	map_files    []string // Map phase files
	sort_files   []string // Over phase files

	nReduce int // Max worker number is runing in reduce phase
	nMap    int // Max worker number is runing in map phase

	shutdown chan bool

	work_id   int32
	workstate chan int // Check if all task is complete
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	log.Println("Master start server")
	rpc.Register(m)
	rpc.HandleHTTP()

	os.Remove(m.sockname)
	listener, err := net.Listen("unix", m.sockname)
	checkError(err, "Failed to build a master listener.")
	go http.Serve(listener, nil)
}

// Register worker, and assign id to worker.
func (m *Master) Register(args *RPCArgs, reply *RPCReply) error {
	log.Println("Register worker")
	state := new(WorkerState)
	atomic.AddInt32(&m.work_id, 1)
	m.workers[m.work_id] = state
	reply.Id = m.work_id
	reply.NReduce = m.nReduce

	return nil
}

func (m *Master) HeartBreak(args RPCArgs, reply *RPCReply) error {
	log.Printf("Recevice worker %d heartbreak\n", args.Id)
	if m.workers[args.Id].HeartBreaks > 0 {
		m.workers[args.Id].HeartBreaks--
	}
	reply.ReturnCode = Ok
	return nil
}

func (m *Master) AssignTask(args RPCArgs, reply *RPCReply) error {
	log.Printf("AssignTask work id=%d, phase=%d\n", args.Id, m.phase)
	m.mu1.Lock()
	if m.phase == MapPhase {
		last := len(m.map_files) - 1
		reply.Taskf = "mapf"
		reply.Files = append(reply.Files, m.map_files[last])
		m.map_files = m.map_files[:last]
	} else if m.phase == ReducePhase {
		last := len(m.reduce_files) - 1
		reply.Taskf = "reducef"
		reply.Files = append(reply.Files, m.reduce_files[last])
		m.reduce_files = m.reduce_files[:last]
	} else if m.phase == OverPhase {
		reply.Taskf = "shutdown"
	}
	reply.ReturnCode = Ok
	m.mu1.Unlock()
	return nil
}

// Wait for heartbreak of worker
func (m *Master) UpdateWorker() {
	timeoutchan := make(chan bool)
loop:
	for {
		go func() {
			<-time.After(1 * time.Second)
			timeoutchan <- true
		}()

		select {
		case <-timeoutchan:
			for _, worker := range m.workers {
				worker.HeartBreaks += 1
				if worker.HeartBreaks >= 5 {
					log.Printf("Worker id:%v heartbreak is stop!\n", worker.Id)
				}
			}

		case <-m.shutdown:
			break loop
		}
	}
}

func (m *Master) WorkDone(args RPCArgs, reply *RPCReply) error {
	log.Printf("Worker Id:%d is done!", args.Id)
	m.mu.Lock()

	if m.phase == MapPhase {
		m.reduce_files = append(m.reduce_files, args.OutPath...)
	} else if m.phase == ReducePhase {
		m.sort_files = append(m.sort_files, args.OutPath...)
	}
	m.workers[args.Id].TaskState = InCompleted
	all_done := true
	for _, worker := range m.workers {
		if worker.TaskState != InCompleted {
			all_done = false
			break
		}
	}
	// Whether all task are distributed
	distributed_over := (m.phase == MapPhase && len(m.map_files) == 0) || (m.phase == ReducePhase && len(m.reduce_files) == 0)

	if all_done && distributed_over {
		log.Printf("All %d task is done", m.phase)
		for _, worker := range m.workers {
			if worker.TaskState == InCompleted {
				worker.TaskState = Idle
			}
		}
		// All task is done, and transit next task state
		m.phase++
	}
	m.mu.Unlock()
	return nil
}

// Wait for a .HeartBreaks every 5s.
// Wait for task assign request
func (m *Master) Schedule() {
	log.Println("Master start schedule")

	go m.UpdateWorker() // Wait for worker .HeartBreaks loop

	for {
		// Wait for worker register
		if m.phase == InitPhase {
			time.Sleep(1 * time.Second)
			m.phase = MapPhase
		}
		time.Sleep(time.Millisecond * 100)
		if m.phase == OverPhase {
			break
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.phase == OverPhase
}

// Send shutdown signal to workers
func (m *Master) Shutdown() {
	m.shutdown <- true
}

// Sort all intermediate files
func (m *Master) Sort() {
	kv := make(map[string]int)
	ff := func(r rune) bool { return !unicode.IsSpace(r) }
	for _, file := range m.sort_files {
		file, err := os.Open(file)
		checkError(err, "Failed to open file.")
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			words := strings.FieldsFunc(line, ff)
			count, _ := strconv.Atoi(words[1])
			kv[words[0]] = kv[words[0]] + count
		}
	}

	file, err := os.Open("mr-sorted")
	checkError(err, "Failed to open files.")
	defer file.Close()
	for k, v := range kv {
		file.WriteString(fmt.Sprintf("%s %d\n", k, v))
	}
}

func NewMaster(files []string, nReduce int) *Master {
	master := new(Master)

	master.name = "master"
	master.sockname = masterSock()
	master.workers = make(map[int32]*WorkerState)

	master.nReduce = nReduce
	master.map_files = files
	master.phase = InitPhase
	master.shutdown = make(chan bool)
	return master
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Printf("MakeMaster, nReduce = %d", nReduce)
	// Create a rpc service and run it.
	m := NewMaster(files, nReduce)
	m.server()   // Lauch a RPC stub
	m.Schedule() // Start map phase schedule
	// Sort all files
	m.Sort()
	m.Shutdown() // Master and workers shutdown
	return m
}
