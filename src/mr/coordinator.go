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
	nMap        int
	files       []string
	tasks       map[string]Task
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

	log.Printf("Coordinator get Call %+v %+v", *args, *reply)
	if args.FinishTaskID != -1 {
		c.lock.Lock()
		if args.FinishTaskType == MAP {
			c.nMap--
			if c.nMap == 0 {
				log.Printf("c.state = REDUCE")
				c.state = REDUCE
				for i := 0; i < c.nReduce; i++ {
					task := Task{
						ID:              i,
						WaitReduceFiles: c.files,
					}
					c.ReduceTasks <- task
				}
			}
		} else if args.FinishTaskType == REDUCE {
			c.nReduce--
			if c.nMap == 0 {
				log.Printf("c.state = DONE")
				c.state = DONE
			}
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
			task.DeadLine = time.Now().Add(10 * time.Second)
			c.tasks[task.TaskType+fmt.Sprint(task.ID)] = task
		default:
			c.lock.Unlock()
			time.Sleep(100 * time.Millisecond)
			goto up
		}

	} else if c.state == REDUCE {
		select {
		case task := <-c.ReduceTasks:
			reply.TaskType = REDUCE
			reply.NReduce = c.nReduce
			reply.WaitReduceFiles = task.WaitReduceFiles
			reply.ReduceID = task.ID
			task.DeadLine = time.Now().Add(10 * time.Second)
			c.tasks[task.TaskType+fmt.Sprint(task.ID)] = task
			// fmt.Printf("reply: %v\n", reply)

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
	c.lock.Lock()
	if c.state == DONE {
		c.lock.Unlock()
		time.Sleep(2 * time.Second)
		return true
	} else {
		c.lock.Unlock()
		return false
	}
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
		files:       files,
		tasks:       make(map[string]Task),
		nMap:        len(files),
	}
	// Your code here.
	for i, file := range files {
		task := Task{
			ID:          i,
			TaskType:    MAP,
			WaitMapFile: file,
		}
		c.tasks[task.TaskType+fmt.Sprint(task.ID)] = task
		c.MapTasks <- task
	}
	log.Printf("Coordinator completes work queue initialization")
	c.server()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != -1 && time.Now().After(task.DeadLine) {
					// 回收并重新分配
					task.WorkerID = -1
					if task.TaskType == MAP {
						c.MapTasks <- task
					} else if task.TaskType == REDUCE {
						c.ReduceTasks <- task
					}
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
