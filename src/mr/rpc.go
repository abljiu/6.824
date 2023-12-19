package mr

import "os"
import "strconv"

// RPC definitions.
const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	DONE   = "DONE"
)

//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {
	WorkerID int
}

type Reply struct {
	Task string
	TaskID int
	TaskType string
	nReduce int 
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
