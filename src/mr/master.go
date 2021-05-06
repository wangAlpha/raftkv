package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	sync.Mutex
	phase TaskPhase

	workers  map[int]*WorkerState // worker name and status
	sockname string
	name     string
	reduces  []string
	maps     []string

	nReduce int // Max worker number is runing in reduce phase
	nMap    int // Max worker number is runing in map phase

	rpc_conn *net.Conn // RPC server connection
	files    []string
	shutdown chan bool

	work_id   int
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

	// loop:
	// 	for {
	// 		conn, err := listener.Accept()
	// 		checkError(err, "Failed to accept a connection.")
	// 		select {
	// 		case <-m.shutdown:
	// 			log.Println("Master received shutdown command!")
	// 			break loop
	// 		default:
	// 			log.Println("rpc.ServeConn")
	// 			go rpc.ServeConn(conn)
	// 		}
	// 	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.phase == OverPhase
}

// Register worker, and assign id to worker.
func (m *Master) Register(args *RPCArgs, reply *RPCReply) error {
	log.Println("Register worker")

	state := new(WorkerState)
	m.work_id++
	m.workers[m.work_id] = state
	reply.Id = m.work_id

	reply.ReturnCode = Ok
	return nil
}

func (m *Master) HeartBreak(args RPCArgs, reply *RPCReply) error {
	log.Printf("Recevice worker %v heartbreak\n", args.Id)
	if m.workers[args.Id].HeartBreaks > 0 {
		m.workers[args.Id].HeartBreaks--
	}
	reply.ReturnCode = Ok
	return nil
}

func (m *Master) AssignTask(args RPCArgs, reply *RPCReply) error {
	log.Printf("AssignTask work id=%d, phase=%d\n", args.Id, m.phase)

	if m.phase == MapPhase {
		nWorker := len(m.workers)
		M := len(m.files) / nWorker // 每个worker的文件数
		log.Printf("nWorker: %d, M: %d", nWorker, M)
		j := 0
		for i := 0; i < len(m.files); i += M {
			j += M
			if j > len(m.files) {
				j = len(m.files)
			}
			if i+2*M > len(m.files) {
				reply.Files = m.files[i:]
				break
			} else {
				reply.Files = m.files[i:j]
			}
		}
		reply.Taskf = "mapf"
	} else if m.phase == ReducePhase {
		// files := m.files
		// reply.Files = files
		reply.Taskf = "reducef"
	} else if m.phase == OverPhase {
		reply.Taskf = "shutdown"
	}
	reply.ReturnCode = Ok
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
			m.Lock()
			defer m.Unlock()
			for _, worker := range m.workers {
				worker.HeartBreaks += 1
				if worker.HeartBreaks >= 5 {
					log.Printf("Worker %v heartbreak is stop!\n", worker.Id)
				}
			}

		case <-m.shutdown:
			break loop
		}
	}
}

func (m *Master) WorkDone(args RPCArgs, reply *RPCReply) error {
	log.Println("Worker is done!")
	// m.Lock()
	// defer m.Unlock()

	m.workers[args.Id].TaskState = InCompleted
	all_done := true
	for _, worker := range m.workers {
		if worker.TaskState != InCompleted {
			all_done = false // 任务没有完成
			break
		}
	}
	if all_done {
		log.Printf("All %d task is done", m.phase)
		// All task is done, and next task state
		state := <-m.workstate
		m.workstate <- state + 1
		
	} else {
		for _, worker := range m.workers {
			log.Printf("%+v", worker.TaskState)
		}
	}
	return nil
}

// Wait for a .HeartBreaks every 5s.
// Wait for task assign request
func (m *Master) Schedule() {
	log.Println("Master start schedule")

	go m.UpdateWorker() // Wait for worker .HeartBreaks loop
	time.Sleep(time.Millisecond * 100)
loop:
	for {
		select {
		case state, ok := <-m.workstate:
			// Shutdown until all worker is shutdown
			if ok && state == OverPhase {
				break loop
			} else {
				for _, worker := range m.workers {
					if worker.TaskState != InCompleted {
						worker.TaskState = Idle
					}
				}
			}
		default:
			break loop
		}
	}
}

// Clean up intermediate files.
func (m *Master) CleanupFiles() {
}

// Send shutdown signal to workers
func (m *Master) Shutdown() {
	m.CleanupFiles()
}

// Sort all intermediate files
func Sort() {
}

func NewMaster(files []string, nReduce int) *Master {
	master := new(Master)

	master.name = "master"
	master.sockname = masterSock()
	master.workers = make(map[int]*WorkerState)
	master.files = files
	master.nReduce = nReduce
	master.rpc_conn = nil
	master.shutdown = make(chan bool)
	master.phase = MapPhase
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
	m.server()                  // Lauch a RPC stub
	time.Sleep(1 * time.Second) // Wait worker register
	m.Schedule()                // Start map phase schedule
	// Sort all files
	Sort()
	m.Shutdown() // Master and workers shutdown
	return m
}
