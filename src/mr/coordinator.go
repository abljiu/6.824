package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nReduce   int
	lock      sync.Mutex
	state     string
	waitTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Tasks(args *Args, reply *Reply) error {
	if args.FinishTaskID != -1 {
		if args.FinishTaskType == MAP {
			task := Task{
				ID:       args.FinishTaskID,
				TaskType: REDUCE,
				WorkerID: args.WorkerID,
			}
			c.lock.Lock()
			log.Printf("Put task %d into REDUCE", args.FinishTaskID)
			c.waitTasks <- task
			c.lock.Unlock()
		}
	}
	c.lock.Lock()
	select {
	case task := <-c.waitTasks:
		if task.TaskType == MAP {
			reply.TaskFile = task.WaitMapFile
			reply.MapID = task.ID
			reply.TaskType = MAP
			reply.NReduce = c.nReduce
			c.state = MAP
		} else if task.TaskType == REDUCE {
			reply.ReduceID = task.ID
			reply.TaskType = REDUCE
			reply.NReduce = c.nReduce
			reply.MapID = task.WorkerID
			c.state = REDUCE
		}
	default:
		{
			c.state = DONE
			reply.TaskType = DONE
			log.Printf("All tasks have been completed!")
		}
	}
	c.lock.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.state == DONE {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:   nReduce,
		state:     MAP,
		waitTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	// Your code here.
	for i, file := range files {
		task := Task{
			ID:          i,
			TaskType:    MAP,
			WaitMapFile: file,
		}
		c.waitTasks <- task
	}
	log.Printf("Coordinator completes work queue initialization")
	c.server()

	return &c
}
