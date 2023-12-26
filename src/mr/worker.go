package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	id := os.Getpid()
	args := Args{
		WorkerID:       id,
		FinishTaskID:   -1,
		FinishTaskType: "",
	}
	reply := Reply{}
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	for {
		call("Coordinator.Tasks", &args, &reply)
		log.Print("call back", args, reply)
		switch reply.TaskType {
		case MAP:
			{
				log.Printf("Worker %d start MAP task %d", id, reply.MapID)
				//解析给定文件
				filename := reply.TaskFile
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				//获得该文件键值对数组
				kva := mapf(filename, string(content))
				hashedKva := make(map[int][]KeyValue)
				//对键值对进行哈希 分成nReduce个数组
				for _, kv := range kva {
					hashed := ihash(kv.Key) % reply.NReduce
					hashedKva[hashed] = append(hashedKva[hashed], kv)
				}

				//生成中间文件 mr-X-Y
				for i := 0; i < reply.NReduce; i++ {
					filename := "mr-" + path.Base(reply.TaskFile) + "-" + fmt.Sprint(i)
					outFile, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
					enc := json.NewEncoder(outFile)
					for _, kv := range hashedKva[i] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot encode in tmp file: %v", filename)
						}
					}
					outFile.Close()
				}
				args.FinishTaskID = reply.MapID
				args.FinishTaskType = MAP
				args.FinishMapFile = reply.TaskFile
				break
			}
		case REDUCE:
			{
				log.Printf("Worker %d start REDUCE task %d", id, reply.ReduceID)
				fmt.Printf("reply: %v\n", reply)
				//读取给定的中间文件
				intermediate := []KeyValue{}

				for _, TaskFile := range reply.WaitReduceFiles {
					filename := "mr-" + path.Base(TaskFile) + "-" + fmt.Sprint(reply.ReduceID)
					outFile, err := os.Open(filename)
					if err != nil {
						panic(err)
					}
					dec := json.NewDecoder(outFile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							// 如果是 EOF 错误，表示文件读取完毕
							if err == io.EOF {
								break
							}
							log.Fatalf("cannot decode in tmp file: %v", filename)
						}
						intermediate = append(intermediate, kv)
					}
					outFile.Close()
				}
				//将中间文件的键值对数组排序
				// fmt.Printf("intermediate: %v\n", intermediate)
				sort.Sort(ByKey(intermediate))

				oname := "mr-out-" + fmt.Sprint(reply.ReduceID)
				ofile, _ := os.Create(oname)

				i := 0
				for i < len(intermediate) {
					j := i + 1
					//找到key相同的键值对
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				ofile.Close()
				args.FinishTaskID = reply.ReduceID
				args.FinishTaskType = REDUCE
				break
			}
		case DONE:
			{
				log.Printf("All tasks have been completed!")
				log.Printf("Worker %d exit\n", id)
				return
			}
		}
		log.Printf("Worker %d completed the work %s ID %d", id, args.FinishTaskType, args.FinishTaskID)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
