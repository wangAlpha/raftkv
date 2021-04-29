package mr

import (
	"log"
	"os"
	"strconv"
)

// RPC Args message definition
type RPCArgs struct {
	Phrase  int
	name    string
	OutPath []string
}

// RPC reply message definition
type RPCReply struct {
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
