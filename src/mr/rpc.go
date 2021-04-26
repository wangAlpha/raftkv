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

// func (p *RpcService) startServer() {

// 	rpc.RegisterName("RpcService", new(RpcService))

// 	listener, err := net.Listen("tcp", ":1234")
// 	if err != nil {
// 		log.Fatal("ListenTCP error:", err)
// 	}

// 	conn, err := listener.Accept()
// 	if err != nil {
// 		log.Fatal("Accept error:", err)
// 	}
// 	msg := make(chan string)
// 	go schedule(conn, msg)
// }

// func schedule(t string) {
// 	if t == "master" {
// 		//
// 		for {
// 			//
// 		}
// 	} else {
// 	}
// }

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
