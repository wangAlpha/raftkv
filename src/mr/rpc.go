package mr

import (
	"log"
	"net/rpc"
	"os"
	"strconv"
)

type TaskPhase int

// type WorkState int

const Ok = 200

// Worker state type.
const (
	Idle = iota
	InProcess
	InCompleted
)

// Master state type.
const (
	MapPhase = iota
	ReducePhase
	OverPhase
)

// Work state definitions here.
type WorkerState struct {
	Id          int
	HeartBreaks int
	TaskState   TaskPhase
}

// RPC Args message definition
type RPCArgs struct {
	Id      int
	State   TaskPhase // Idle, InCompleted
	OutPath []string
}

// RPC reply message definition
type RPCReply struct {
	Id          int
	Taskf       string
	ReturnCode  int
	WorkerState WorkerState
	Files       []string
}

func checkError(err error, msg string) {
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

//
// Send an RPC request to the master, wait for the response.
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
