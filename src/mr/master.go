package mr

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
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

type Workstate int
type TaskPhase int

// Worker state type.
const (
	Idle = iota
	InProcess
	InCompleted
)

// Master state type.
const (
	InitPhase = iota
	MapPhase
	ReducePhase
	OverPhase
)

// Work state definitions here.
type WorkerState struct {
	id          int
	state       WorkState
	heartbreaks int
}

// Master's Type definitions here.
type Master struct {
	sync.Mutex
	phase TaskPhase

	workers  map[int]WorkerState // worker name and status
	sockname string
	name     string
	reduces  []string
	maps     []string

	nReduce int // Max worker number is runing in reduce phase
	nMap    int // Max worker number is runing in map phase

	rpc_conn *net.Conn // RPC server connection
	files    []string
	shutdown chan bool

	work_id int
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

// func (m *Master) ReportTasks(args RPCArgs, reply *RPCReply) error {
// 	log.Println("Report task status", m.phase)
// 	log.Println(m.phase)
// 	// if m.phase == InReduce {
// 	// 	fmt.Println("InReduce phase")
// 	// } else if {}
// 	// return "working"
// 	return nil
// }

func (m *Master) RegisterWorker(args RPCArgs, reply *RPCReply) error {
	log.Println("Register worker")
	state := WorkerState{}
	m.work_id += 1
	m.workers[m.work_id] = state
	// m.workers[args.name] = Idle
	// TODO
	// Register worker's name and status.
	return nil
}

func (m *Master) freshWorker() {
	// for {
	// select {
	// case shutdown:
	// default:
	// }
	// }
}

func (m *Master) Heartbreak(args Args, reply *Reply) error {
	log.Println("Recevice worker %s heartbreak")
	// for _, status range m.task_status {
	// 	//
	// }
	// Find my name
	return nil
}

// Wait for a heartbreaks every 5s.
// Wait for task assign request
func (m *Master) Schedule() {
	log.Println("Master start schedule")

	go m.freshWorker()
	// Start a wait heartbreak loop

	// call("Master.ReportTasks", args, &reply)
}

// Clean up intermediate files.
func (m *Master) CleanupFiles() error {
	return nil
}

// Send shutdown signal to workers
func (m *Master) Shutdown() error {
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
	m.server()   // Lauch a RPC stub
	m.Schedule() // Start map phase schedule
	// Sort all files
	Sort()
	m.Shutdown() // Master and workers shutdown
	return m
}
