package mr

import (
	"log"
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
	InitPhase = iota
	MapPhase
	ReducePhase
	OverPhase
)

// Work state definitions here.
type WorkerState struct {
	id          int
	heartbreaks int
	task_state  TaskPhase
}

// RPC Args message definition
type RPCArgs struct {
	id      int
	state   TaskPhase
	OutPath []string
}

// RPC reply message definition
type RPCReply struct {
	id           int
	worker_state WorkerState
	taskf        interface{}
	return_code  int
	files        []string
}

func checkError(err error) {
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
