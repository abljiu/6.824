package mr

import "os"
import "strconv"

// RPC definitions.
const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	DONE   = "DONE"
)

type Args struct {
	WorkerID       int
	FinishTaskID   int
	FinishTaskType string
}

type Reply struct {
	TaskFile string //需要map的文件
	MapID    int    //等待Map的ID任务号 or 完成map后的ID号
	TaskType string //任务类型
	NReduce  int    //reduce任务的个数
	ReduceID int    //等待Reduce的ID任务号
	F    []string
}

type Task struct {
	ID          int    //任务ID
	WorkerID    int    //处理该任务的工人ID
	TaskType    string //任务类型
	WaitMapFile string //待map文件
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
