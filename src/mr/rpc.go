package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type RPCArgs struct {
	Phrase  int
	OutPath []string
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type RPCReply struct {
}

func WorkerId() string {
	return "1"
}

// func workerSocker() string {

// 	// s := "/var/tmp/824-mr-"

// 	return s
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
