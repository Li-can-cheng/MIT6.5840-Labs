package mr

// worker.go

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			fmt.Println("failed to call AssignTask, retrying...")
			time.Sleep(time.Second)
			continue
		}
		task := reply.Task
		if task.TaskType == "None" {
			fmt.Println("No tasks available, exiting...")
			break
		}
		if task.TaskType == "Wait" {
			time.Sleep(time.Second)
			continue
		}

		log.Printf("Received task: %+v", task)
		if task.TaskType == "Map" {
			handleMapAndShuffleTask(mapf, &task)
		} else if task.TaskType == "Reduce" {
			handleReduceTask(reducef, &task)
		} else {
			time.Sleep(time.Second)
			fmt.Println("Unknown task type, retrying...")
		}
		reportTaskComplete(&task)
		time.Sleep(time.Second)

	}
}

func reportTaskComplete(task *Task) {
	args := TaskCompleteArgs{TaskID: task.ID, IntermediateFiles: task.IntermediateFiles}
	reply := TaskCompleteReply{}
	ok := call("Coordinator.TaskComplete", &args, &reply)
	if !ok {
		log.Printf("Failed to report task complete for task %d", task.ID)
	}
}

func handleReduceTask(reducef func(string, []string) string, task *Task) {
	kvs := make(map[string][]string)

	log.Printf("Processing reduce task with intermediate files: %v", task.IntermediateFiles)

	for _, filename := range task.IntermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Cannot open intermediate file %v: %v", filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			if err := dec.Decode(&kvs); err != nil {
				break
			}
		}
		file.Close()
	}

	oname := fmt.Sprintf("mr-out-%d", task.ReduceID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Cannot create output file %v: %v", oname, err)
	}
	defer ofile.Close()

	for k, v := range kvs {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}

	log.Printf("Reduce task %d completed, output file: %v", task.ID, oname)
}

func handleMapAndShuffleTask(mapf func(string, string) []KeyValue, task *Task) {
	if task.NReduce <= 0 {
		log.Fatalf("Invalid nReduce: %v", task.NReduce)
	}
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	defer file.Close()

	keyValueArray := mapf(task.Filename, string(content))

	// Sort keyValueArray by key
	sort.Sort(ByKey(keyValueArray))

	// Merge values for the same key
	merged := make(map[string][]string)
	for _, kv := range keyValueArray {
		merged[kv.Key] = append(merged[kv.Key], kv.Value)
	}

	// Prepare intermediate files
	intermediate := make(map[int][](map[string][]string))
	for key, values := range merged {
		reducerNumber := ihash(key) % task.NReduce
		intermediate[reducerNumber] = append(intermediate[reducerNumber], map[string][]string{key: values})
		// for _, value := range values {
		// 	intermediate[reducerNumber] = append(intermediate[reducerNumber], KeyValue{Key: key, Value: value})
		// }
	}

	var intermediateFiles []string
	for i, kvs := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", task.ID, i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode kv pair to %v", oname)
			}
		}
		ofile.Close()
		intermediateFiles = append(intermediateFiles, oname)
		log.Printf("Intermediate file created: %v", oname)
	}

	task.IntermediateFiles = intermediateFiles
	reportTaskComplete(task)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("Dialing error:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Println("RPC call error:", err)
		return false
	}

	return true
}
