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
	return nil
}

func (m *Master) AssignTask(args RPCArgs, reply *RPCReply) error {
	log.Printf("AssignTask work id=%d, phase=%d\n", args.Id, m.phase)
	m.mu1.Lock()
	defer m.mu1.Unlock()

	if m.phase == MapPhase {
		if len(m.map_files) != 0 {
			last := len(m.map_files) - 1
			reply.Taskf = "Mapf"
			reply.Files = append(reply.Files, m.map_files[last])
			m.map_files = m.map_files[:last]
			m.workers[args.Id].TaskState = InProcess
		} else {
			reply.Taskf = "Wait"
			reply.Files = []string{}
		}
	} else if m.phase == ReducePhase {
		if len(m.reduce_files) != 0 {
			last := len(m.reduce_files) - 1
			reply.Taskf = "Reducef"
			reply.Files = append(reply.Files, m.reduce_files[last])
			m.reduce_files = m.reduce_files[:last]
			m.workers[args.Id].TaskState = InProcess
		} else {
			reply.Taskf = "Wait"
			reply.Files = []string{}
		}
	} else if m.phase == OverPhase {
		reply.Taskf = "Shutdown"
		reply.Files = []string{}
	}
	return nil
}

// Wait for heartbreak of worker
func (m *Master) MonitorWorker() {
	timeoutchan := make(chan bool)
loop:
	for {
		go func() {
			<-time.After(1 * time.Second)
			timeoutchan <- true
		}()

		select {
		case <-timeoutchan:
			for id, status := range m.workers {
				status.HeartBreaks += 1
				if status.HeartBreaks >= 5 {
					log.Printf("Worker id:%v heartbreak is stop!\n", id)
				}
			}

		case <-m.shutdown:
			break loop
		}
	}
}

func (m *Master) WorkDone(args RPCArgs, reply *RPCReply) error {
	log.Printf("Worker Id:%d is done, State: %d", args.Id, args.Phase)
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers[args.Id].TaskState = InCompleted

	all_done := func(files []string) bool {
		for _, w := range m.workers {
			if w.TaskState != InCompleted {
				return false
			}
		}
		return true && len(files) == 0
	}
	phase_done := false
	if m.phase == MapPhase {
		m.reduce_files = append(m.reduce_files, args.OutPath...)
		phase_done = all_done(m.map_files)
	} else if m.phase == ReducePhase {
		m.sort_files = append(m.sort_files, args.OutPath...)
		phase_done = all_done(m.reduce_files)
	}

	if phase_done {
		log.Printf("All %d task is done", m.phase)
		for _, w := range m.workers {
			if w.TaskState == InCompleted {
				w.TaskState = Idle
			}
		}
		// All task is done, and transit next task state
		m.phase++
	}
	return nil
}

// Wait for a .HeartBreaks every 5s.
// Wait for task assign request
func (m *Master) Schedule() {
	log.Println("Master start schedule")

	go m.MonitorWorker() // Wait for worker .HeartBreaks loop

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
func (m *Master) Done() bool {
	return m.phase == OverPhase
}

// Send shutdown signal to workers
func (m *Master) Shutdown() {
	m.shutdown <- true
}

// Sort all intermediate files
func (m *Master) Sort() {
	keyValue := make(map[string]int)
	log.Printf("final files %d, %+v", len(m.sort_files), m.sort_files)
	inFiles := make(map[string]bool)
	for _, f := range m.sort_files {
		inFiles[f] = true
	}
	for file := range inFiles {
		ff, err := os.Open(file)
		checkError(err, "Failed to open file.")
		defer ff.Close()
		scanner := bufio.NewScanner(ff)
		for scanner.Scan() {
			line := scanner.Text()
			words := strings.Fields(line)
			if len(words) != 2 {
				log.Println(words)
				break
			}
			count, _ := strconv.Atoi(words[1])
			keyValue[words[0]] = keyValue[words[0]] + count
		}
		// ioutil
		// decoder := json.NewDecoder(ff)
		// kv := KeyValues{}
		// for decoder.More() {
		// 	err := decoder.Decode(&kv)
		// 	checkError(err, "Failed to decoder kv.")
		// 	keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value...)
		// }
	}
	ff, err := os.Create("mr-out")
	checkError(err, "Failed to create final file.")
	defer ff.Close()
	for k, c := range keyValue {
		ff.WriteString(fmt.Sprintf("%s %d\n", k, c))
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
	m.Sort()     // Sort final files
	m.Shutdown() // Master and workers shutdown
	return m
}
