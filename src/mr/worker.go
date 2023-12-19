package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	for {
		args := Args{
			WorkerID: id,
		}
		reply := Reply{}
		call("Coordinator.Tasks", &args, &reply)

		switch reply.TaskType {
		case MAP:
			{
				filename := reply.Task
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				hashedKva := make(map[int][]KeyValue)

				for _, kv := range kva {
					hashed := ihash(kv.Key) % reply.nReduce
					hashedKva[hashed] = append(hashedKva[hashed], kv)
				}
				for i := 0; i < reply.nReduce; i++ {
					filename := "mr-" + string(reply.TaskID) + "-" + string(i)
					outFile, _ := os.Create(filename)
					for _, kv := range hashedKva[i] {
						fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
					}
					outFile.Close()
				}
			}
		case REDUCE:
			{

			}
		}

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
