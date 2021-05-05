package mr

import (
	"log"
	"net"
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
	checkError(err)

loop:
	for {
		conn, err := listener.Accept()
		checkError(err)
		select {
		case <-m.shutdown:
			log.Println("Master received shutdown command!")
			break loop
		default:
			log.Println("rpc.ServeConn")
			go rpc.ServeConn(conn)
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

// Register worker, and assign id to worker.
func (m *Master) RegisterWorker(args RPCArgs, reply *RPCReply) error {
	log.Println("Register worker")

	state := new(WorkerState)
	m.work_id += 1
	state.id = m.work_id
	m.workers[m.work_id] = state

	reply.return_code = Ok
	return nil
}

func (m *Master) Heartbreak(args RPCArgs, reply *RPCReply) error {
	log.Printf("Recevice worker %v heartbreak\n", args.id)
	m.workers[args.id].heartbreaks--
	reply.return_code = Ok
	return nil
}

func (m *Master) AssignTask(args RPCArgs, reply *RPCReply) error {
	// id := string(args.id)
	// number := ihash(id) % m.nMap
	if m.phase == MapPhase {
		files := m.files
		reply.files = files
		reply.taskf = "mapf"
	} else if m.phase == ReducePhase {
		files := m.files
		reply.files = files
		reply.taskf = "reducef"
	} else if m.phase == OverPhase {
		reply.taskf = "shutdown"
	}
	reply.return_code = Ok
	return nil
}

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
				worker.heartbreaks += 1
				if worker.heartbreaks >= 5 {
					log.Printf("Worker %v heartbreak is stop!\n", worker.id)
				}
			}
		case <-m.shutdown:
			break loop
		}
	}
}

func (m *Master) WorkDone(args RPCArgs, reply *RPCReply) error {
	log.Println("Worker is done!")
	m.workers[m.work_id].task_state = InCompleted
	done := true
	for _, worker := range m.workers {
		if worker.task_state != InCompleted {
			done = false // 任务没有完成
			break
		}
	}
	if done {
		// All task is done, and next task
		state := <-m.workstate
		m.workstate <- state + 1
	}
	return nil
}

// Wait for a heartbreaks every 5s.
// Wait for task assign request
func (m *Master) Schedule() {
	log.Println("Master start schedule")

	go m.UpdateWorker() // Wait for worker heartbreaks loop
	m.phase = InitPhase
	time.Sleep(time.Millisecond * 100)
loop:
	for {
		select {
		case state, ok := <-m.workstate:
			if ok && state == OverPhase {
				break loop
			} else {
				for _, worker := range m.workers {
					if worker.task_state != InCompleted {
						worker.task_state = InitPhase
					}
				}
			}
		}
	}
}

// Clean up intermediate files.
func (m *Master) CleanupFiles() error {
	return nil
}

// Send shutdown signal to workers
func (m *Master) Shutdown() error {
	m.CleanupFiles()

	return nil
}

// Sort all intermediate files
func Sort() error {
	return nil
}

func NewMaster(files []string) *Master {
	master := new(Master)

	master.name = "master"
	master.sockname = masterSock()
	master.files = files
	master.rpc_conn = nil
	master.shutdown = make(chan bool)

	return master
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Println("MakeMaster")
	// Create a rpc service and run it.
	m := NewMaster(files)
	m.server()                  // Lauch a RPC stub
	time.Sleep(1 * time.Second) // Wait worker register
	m.Schedule()                // Start map phase schedule
	// Sort all files
	Sort()
	m.Shutdown() // Master and workers shutdown
	return m
}
