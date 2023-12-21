package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	lock        sync.Mutex
	state       string
	MapTasks    chan Task
	ReduceTasks chan Task
	wang        chan int
	fileLen     int
	files       []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func dolog(i ...interface{}) {
	log.Println(i)
}

func (c *Coordinator) Tasks(args *Args, reply *Reply) error {
	defer func(r *Reply) {
		dolog("replyyyyyyy     ", *args, *r)
	}(reply)

	log.Printf("Coordinator get Call %+v %+v", *args, *reply)
	if args.FinishTaskID != -1 {
		dolog("try get lock", *args, *reply)
		c.lock.Lock()
		dolog("get lock", *args, *reply)
		if args.FinishTaskType == MAP {
			task := Task{
				ID:       args.FinishTaskID,
				TaskType: REDUCE,
				WorkerID: args.WorkerID,
			}
			log.Printf("Put task %d into REDUCE Len Chan%d capChan%d", args.FinishTaskID, len(c.ReduceTasks), cap(c.ReduceTasks))
			c.ReduceTasks <- task
			if len(c.ReduceTasks) == c.fileLen {
				log.Printf("c.state = REDUCE")
				c.state = REDUCE
			}
		}
		if len(c.wang) == 0 {
			c.state = DONE
		}
		c.lock.Unlock()
		dolog("unlock", *args)
	}
up:
	c.lock.Lock()
	if c.state == MAP {
		select {
		case task := <-c.MapTasks:
			reply.TaskFile = task.WaitMapFile
			reply.MapID = task.ID
			reply.TaskType = MAP
			reply.NReduce = c.nReduce
			copy(reply.F, c.files)
		default:
			c.lock.Unlock()
			time.Sleep(100 * time.Millisecond)
			goto up
		}

	} else if c.state == REDUCE {

		select {
		case task := <-c.wang:
			reply.ReduceID = task
			reply.TaskType = REDUCE
			reply.NReduce = c.nReduce
			reply.MapID = task
			reply.F = append(reply.F, c.files...)
			fmt.Printf("reply: %v\n", reply)
		default:
			c.lock.Unlock()
			time.Sleep(100 * time.Millisecond)
			goto up
		}

	} else if c.state == DONE {
		reply.TaskType = DONE
		log.Printf("All tasks have been completed!")
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
		nReduce:     nReduce,
		state:       MAP,
		MapTasks:    make(chan Task, len(files)),
		ReduceTasks: make(chan Task, nReduce),
		fileLen:     len(files),
		wang:        make(chan int, nReduce),
		files:       files,
	}
	for i := 0; i < nReduce; i++ {
		c.wang <- i
	}
	// Your code here.
	for i, file := range files {
		task := Task{
			ID:          i,
			TaskType:    MAP,
			WaitMapFile: file,
		}
		c.MapTasks <- task
	}
	log.Printf("Coordinator completes work queue initialization")
	c.server()
	log.Print(c, "\n\n\n\n")
	return &c
}
