package mr

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Phase is master Phase, it
type Phase int

// Master state type.
const (
	Idle = iota
	InMap
	InReduce
	PhaseOver
)

// Master's Type definitions here.
type Master struct {
	sync.Mutex
	phase Phase

	workers  []string
	sockname string
	name     string
	reduces  []string
	maps     []string

	nReduce int // Max worker is runing in reduce phase
	nMap    int // max workers is runing in map phase

	rpc_conn *net.Conn // RPC server connection
	files    []string
	shutdown chan bool
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// Your code here -- RPC handlers for the worker to call.

// TODO Issue task
func (m *Master) IssueTask() {
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
	return m.phase == PhaseOver
}

// Sign out of all workers.
// func (m *Master) unregisterWorker() bool {
// 	for w := range m.workers {
// 		var relpy string
// 		call(master, "unregister", &reply)
// 	}
// 	return true
// }
func (m *Master) ReportTasks(args RPCArgs, reply *RPCReply) error {
	log.Println("Report task status", m.phase)
	log.Println(m.phase)
	// if m.phase == InReduce {
	// 	fmt.Println("InReduce phase")
	// } else if {}
	// return "working"
	return nil
}

func (m *Master) RegisterWorker(args RPCArgs, reply *RPCReply) error {
	log.Println("Register worker")
	// TODO
	// Register worker's name and status.
	return nil
}

func schedule() {
	args := RPCArgs{
		Phrase:  0,
		OutPath: make([]string, 1),
	}
	var reply RPCReply
	call("Master.ReportTasks", args, &reply)
	log.Printf("Master ReportTasks = %+v\n", reply)
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
	m.server()

	schedule() // Start map phase schedule

	return m
}
