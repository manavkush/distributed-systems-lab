package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
	// Your worker implementation here.
	for {
		task := AskForTask()

		if task.Status == "done" {
			break;
		}
		// do map task
		if task.TaskType == "map" {
			DoMap(task, mapf)
			println("Task Type: ", task.TaskType, "Task Status: ", task.Status)
		} else if task.TaskType == "reduce" {
			DoReduce(task, reducef)
			println("Task Type: ", task.TaskType, "Task Status: ", task.Status)
		} else if task.Status == "wait" {
			time.Sleep(5 * time.Second)
		}	
	}
	// intermediate := []mr.KeyValue{}
	// for _, filename := range os.Args[2:] {
	// 	file, err := os.Open(filename)
	// 	if err != nil {
	// 		log.Fatalf("cannot open %v", filename)
	// 	}
	// 	content, err := io.ReadAll(file)
	// 	if err != nil {
	// 		log.Fatalf("cannot read %v", filename)
	// 	}
	// 	file.Close()
	// 	kva := mapf(filename, string(content))
	// 	intermediate = append(intermediate, kva...)
	// }


	// CallExample()
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

func DoMap(task *Task, mapf func(string, string) []KeyValue) {
	log.Printf("DoMap started: %v", task.Id)
	// Read the file
	// Open nReduce files
	openFiles := []*os.File{}
	encoders := []*json.Encoder{}

	for i := 0; i < task.NReduce; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", task.Id, i)
		newFile, err := os.OpenFile(fmt.Sprintf("mr-%v-%v", task.Id, i), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot create file %v", fileName)
		}

		openFiles = append(openFiles, newFile)
		encoders = append(encoders, json.NewEncoder(newFile))
	}
	
	intermediate := []KeyValue{}
	for _, filename := range task.Files {
		file, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		fileContent := string(file)
		kva := mapf(filename, string(fileContent))
		intermediate = append(intermediate, kva...)
	}

	for _, kv := range intermediate {
		key := kv.Key 
		bucket := ihash(key) % task.NReduce
		enc := encoders[bucket]
		err := enc.Encode(&kv)
		// println("DEBUG: Encoding the keyValue pairs", enc, bucket)
		if err != nil {
			log.Fatalf("cannot encode %v %v", kv, err)
		}
	}

	// {
	// 	if err != nil {
	// 		log.Fatalf("cannot read %v", filename)
	// 	}
	//
	// 	// split contents into an array of words.
	// 	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	// 	words := strings.FieldsFunc(fileContent, ff)
	//
	// 	// println("DEBUG: Read the file contents")
	// 	// for _, filename := range os.Args[2:] {
	// 	// 	file, err := os.Open(filename)
	// 	// 	if err != nil {
	// 	// 		log.Fatalf("cannot open %v", filename)
	// 	// 	}
	// 	// 	content, err := io.ReadAll(file)
	// 	// 	if err != nil {
	// 	// 		log.Fatalf("cannot read %v", filename)
	// 	// 	}
	// 	// 	file.Close()
	// 	// 	kva := mapf(filename, string(content))
	// 	// 	intermediate = append(intermediate, kva...)
	// 	// }
	//
	// 	// for _, w := range words {
	// 	// 	// Now put the words in correct bucket
	// 	// 	bucket := ihash(w) % task.NReduce
	// 	// 	kv := KeyValue{Key: w, Value: "1"}
	// 	//
	// 	// 	enc := encoders[bucket]
	// 	// 	err := enc.Encode(&kv)
	// 	// 	// println("DEBUG: Encoding the keyValue pairs", enc, bucket)
	// 	// 	if err != nil {
	// 	// 		log.Fatalf("cannot encode %v %v", kv, err)
	// 	// 	}
	// 	// }
	// }

	for _, file := range openFiles {
		file.Close()
	}

	// Tell coordinator that map task is done
	TaskCompleted(task)
}

func DoReduce(task *Task, reducef func(string, []string) string) {
	log.Println("DoReduce started: ", task)
	kva := []KeyValue{}
	id := task.Id

	filePathRegex := fmt.Sprintf("mr-*-%v", task.Id)
	matchingFiles, err := filepath.Glob(filepath.Join(".", filePathRegex))
	if err != nil {
		log.Fatal("Error in getting matching files", err)
	}

	for _, fileName := range(matchingFiles) {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Error reading file.", err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	// Write the output to final file
	outFileName := fmt.Sprintf("mr-out-%v", id)
	outFile, err := os.Create(outFileName)

	if err != nil {
		log.Fatal("Cannot open outfile: ", outFileName)
	}

	i := 0; 
	for i < len(kva) {
		j := i+1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j 
	}
	println("---------Task completed: -------------", id, task.TaskType)
	TaskCompleted(task)
}


func TaskCompleted(task *Task) {
	// Tell coordinator that task is done
	time.Sleep(1 * time.Second)
	// taskArgs := CompletedArgs{Id: task.Id, TaskType: task.TaskType}
	taskReply := CompletedReply{}
	call("Coordinator.TaskCompleted", task, &taskReply)

	// call("Coordinator.Example", taskArgs, &CompletedReply{})
}

func AskForTask() *Task {
	args := AskForTaskArgs{}
	reply := Task{}

	call("Coordinator.GiveTask", &args, &reply)

	return &reply

	// if reply.Status == "done" {
	// 	return false
	// }
	//
	// // do map task
	// if reply.TaskType == "map" {
	// 	DoMap(&reply)
	// 	println("Task Type: ", reply.TaskType, "Task Status: ", reply.Status)
	// } else if reply.TaskType == "reduce" {
	// 	DoReduce(&reply)
	// 	println("Task Type: ", reply.TaskType, "Task Status: ", reply.Status)
	// } else if reply.Status == "wait" {
	// 	time.Sleep(5 * time.Second)
	// } else if reply.TaskType == "done" {
	// 	return false
	// }
	//
	// return true
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

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
