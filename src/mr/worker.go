package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

type Worker struct {
	name    string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (wk *Worker) register() {
	log.Println("Worker register")
	sockname := masterSock()
	var args RPCArgs
	var reply RPCReply
	call("Master.Register", &args, &reply)
}

//
// main/mrworker.go calls this function.
//
func RunWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("RunWorker...")
	worker := Worker{}
	worker.reducef = reducef
	worker.mapf = mapf
	worker.name = "worker1"

	worker.register()

	// Make many workers
	// Register work to master
	//
	// Sort file and send sorted file to the master.
	// Your worker implementation here.

	// TODO
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	// args := ExampleArgs{}

// 	// // fill in the argument(s).
// 	// args.X = 99

// 	// // declare a reply structure.
// 	// reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	// fmt.Printf("reply.Y %v\n", reply.Y)
// }

// Call dial service
// func DialService(network, address string) (*ServiceWorker, error) {
// 	c, err := rpc.Dial(network, address)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &ServiceWorker{Client: c}, nil
// }

//  func (p *HelloServiceClient) Hello(request string, reply *string) error {
// 	return p.Client.Call(HelloServiceName+".Hello", request, reply)
// }

//
// send an RPC request to the master, wait for the response.
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

	fmt.Println(err)
	return false
}
