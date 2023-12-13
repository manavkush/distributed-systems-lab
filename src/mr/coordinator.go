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

type TaskPair struct {
	Id int
	Files []string
}

type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex
	nReduce int
	numMapCompleted int
	// Queue to store map tasks. If any map task fails, it will be added back to the queue
	MapQueue []TaskPair
	Files []string

	// Queue to store reduce tasks. If any reduce task fails, it will be added back to the queue
	ReduceQueue []TaskPair
	NumReduceCompleted int

	OngoingTasks map[string]*Task

	done bool // true if all mappers and reducers are done
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GiveTask(args *AskForTaskArgs, reply *Task) error {
	// Assigning a map task
	c.mutex.Lock()
	defer c.mutex.Unlock()

	mapTaskToBeAssigned := len(c.MapQueue)
	reduceTaskToBeAssigned := len(c.ReduceQueue)
	println("GiveTask Called: ", mapTaskToBeAssigned, reduceTaskToBeAssigned)

	// If there are map tasks to be assigned, assign one
	if mapTaskToBeAssigned > 0 {
		task := c.MapQueue[0]
		if mapTaskToBeAssigned == 1 {
			// remove all the elements
			c.MapQueue = c.MapQueue[:0]
		} else {
			c.MapQueue = c.MapQueue[1:]
		}

		// Add the task details
		{
			reply.Id = task.Id
			reply.Files = task.Files
			reply.NReduce = c.nReduce
			reply.TaskType = "map"
			reply.Start = time.Now()
			reply.Status = "mapping"
		}

		key := fmt.Sprintf("%d-%s", task.Id, "map")
		c.OngoingTasks[key] = reply

		// channel := make(chan bool)
		// go func() {
		// 	time.Sleep(10 * time.Second)
		// 	println("After 10 seconds: status:", reply.Status)
		// 	if reply.Status == "mapping" {
		// 		c.MapQueue = append(c.MapQueue, task) 
		// 	} else if reply.Status == "completed" {
		// 		c.numMapCompleted++
		// 	}
		// }()
		//
		return nil
	} 

	// If there are no map tasks to be assigned, check if all map tasks are done
	if c.numMapCompleted < len(c.Files) {
		reply.Status = "wait"
		return nil;
	} 

	// If all map tasks are done, assign reduce tasks
	if reduceTaskToBeAssigned > 0 {
		// All map tasks are done and some reduce tasks are pending
		task := c.ReduceQueue[0]
		if len(c.ReduceQueue) == 1 {
			// remove all the elements
			c.ReduceQueue = c.ReduceQueue[:0]
		} else {
			c.ReduceQueue = c.ReduceQueue[1:]
		}

		// Add the task details
		{
			reply.Id = task.Id
			reply.NReduce = c.nReduce
			reply.TaskType = "reduce"
			reply.Start = time.Now()
			reply.Status = "reducing"
		}

		key := fmt.Sprintf("%d-%s", task.Id, "reduce")
		c.OngoingTasks[key] = reply

		return nil
	}

	// If the reduce tasks are being executed, wait for them to complete
	if c.NumReduceCompleted < c.nReduce {
		reply.Status = "wait"
		return nil
	} else {
		// All reduce tasks are done
		c.done = true
		reply.Status = "done"
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.done {
		return true
	}
	return false
}

func (c *Coordinator) TaskCompleted(task *Task, reply *CompletedReply) error {
	fmt.Println("---------Task completed: -------------") 
	fmt.Printf("Coordinator Task completed called. Id: %d Type: %s\n", task.Id, task.TaskType)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := fmt.Sprintf("%d-%s", task.Id, task.TaskType)

	if _, ok := c.OngoingTasks[key]; ok {
		delete(c.OngoingTasks, key)
	}

	if task.TaskType == "map" {
		c.numMapCompleted++
	} else if task.TaskType == "reduce" {
		c.NumReduceCompleted++
		if c.NumReduceCompleted == c.nReduce {
			c.done = true
		}
	}
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		numMapCompleted: 0,
		NumReduceCompleted: 0,
		done: false,

		OngoingTasks: make(map[string]*Task),
		MapQueue: make([]TaskPair, 0),
		ReduceQueue: make([]TaskPair, 0),
		Files: make([]string, 0),
	}
	
	// start a go routine to check if any task is taking too long and add it back to the queue
	// restartTaskChannel := make(chan Task)

	go func(c *Coordinator) {
		for {
			c.mutex.Lock()
			fmt.Printf("Ongoing tasks: %v\n", c.OngoingTasks)
			for key := range c.OngoingTasks {
				task := c.OngoingTasks[key]
				println("Checking for task: ", task)
				if time.Since(task.Start) > 10 * time.Second {
					delete(c.OngoingTasks, key)
					if task.TaskType == "map" {
						fmt.Printf("Map task failed for task id: %d\n", task.Id)
						c.MapQueue = append(c.MapQueue, TaskPair{Id: task.Id, Files: task.Files})
					} else if task.TaskType == "reduce" {
						c.ReduceQueue = append(c.ReduceQueue, TaskPair{Id: task.Id, Files: nil})
					}
				}
			}
			c.mutex.Unlock()
			time.Sleep(10*time.Second)
		}
	}(&c)

	for idx, file := range files {
		c.Files = append(c.Files, file)
		c.MapQueue = append(c.MapQueue, TaskPair{Id: idx, Files: []string{file}})
	}

	for idx := 0; idx < nReduce; idx++ {
		c.ReduceQueue = append(c.ReduceQueue, TaskPair{Id: idx, Files: nil})
	}

	log.Println("Coordinator created")
	c.server()
	return &c
}
