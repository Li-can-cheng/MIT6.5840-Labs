package mr

// coordinator.go
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

type Task struct {
	ID                int
	ReduceID          int
	TaskType          string
	Filename          string
	NReduce           int
	TimeStamp         time.Time
	IntermediateFiles []string // 新增字段
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	nMap        int
	mapTasks    []Task
	reduceTasks []Task
	taskStatus  map[int]TaskStatus
	mu          sync.Mutex
}
type TaskStatus int

const (
	IDLE TaskStatus = iota
	INPROGRESS
	COMPLETED
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// start a thread that listens for RPCs from worker.go
//
// 在 AssignTask 方法中增加任务重试逻辑
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.mapTasks {
		if c.taskStatus[task.ID] == IDLE || (c.taskStatus[task.ID] == INPROGRESS && time.Since(task.TimeStamp) > 10*time.Second) {
			task.TimeStamp = time.Now()
			c.taskStatus[task.ID] = INPROGRESS
			reply.Task = task
			log.Printf("Assigned map task %d with nReduce %d to worker", task.ID, task.NReduce)
			return nil
		}
	}

	if !c.allMapTasksCompleted() {
		reply.Task = Task{TaskType: "Wait"}
		return nil
	}

	for _, task := range c.reduceTasks {
		if c.taskStatus[task.ID] == IDLE || (c.taskStatus[task.ID] == INPROGRESS && time.Since(task.TimeStamp) > 10*time.Second) {

			task.TimeStamp = time.Now()
			task.ReduceID = task.ID - c.nMap
			for i := 0; i < c.nMap; i++ {
				task.IntermediateFiles = append(task.IntermediateFiles, fmt.Sprintf("mr-%d-%d", i, task.ReduceID))
			}
			c.taskStatus[task.ID] = INPROGRESS

			log.Printf("Assigned reduce task %d (ReduceID:%d)with intermediate files: %v to worker", task.ID, task.ReduceID, task.IntermediateFiles)
			reply.Task = task
			return nil
		}
	}

	reply.Task = Task{TaskType: "None"}
	return nil
}

// TaskComplete handles the completion of a task.

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if status, exists := c.taskStatus[args.TaskID]; exists && status != COMPLETED {
		c.taskStatus[args.TaskID] = COMPLETED
		// 保存中间文件信息
		if args.TaskID < c.nMap {
			c.mapTasks[args.TaskID].IntermediateFiles = args.IntermediateFiles
		}

		log.Printf("Task %d completed with intermediate files: %v", args.TaskID, args.IntermediateFiles)
	}
	return nil
}

func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("Failed to register coordinator RPC server:", err)
		return
	}
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	err = os.Remove(sockname)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal("Failed to remove sock file:", err)
		return
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("listen on ", sockname)
	go http.Serve(l, nil)
}

// 增加检查所有Map任务是否完成的方法
func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.mapTasks {
		if c.taskStatus[task.ID] != COMPLETED {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, status := range c.taskStatus {
		if status != COMPLETED {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	if nReduce <= 0 {
		log.Fatalf("nReduce must be greater than 0")
	}
	// log.Println("nReduce: ", nReduce)
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		nMap:       len(files),
		taskStatus: make(map[int]TaskStatus),
	}
	log.Println("Created a coordinator:", &c)

	// 在 MakeCoordinator 中初始化 TimeStamp
	for i, file := range files {
		task := Task{
			ID:        i,
			TaskType:  "Map",
			Filename:  file,
			NReduce:   nReduce,
			TimeStamp: time.Now(), // 初始化时间戳
		}
		c.mapTasks = append(c.mapTasks, task)
		c.taskStatus[i] = IDLE
	}
	for i := 0; i < nReduce; i++ {
		task := Task{
			ID:        c.nMap + i,
			ReduceID:  i,
			TaskType:  "Reduce",
			NReduce:   nReduce,
			TimeStamp: time.Now(), // 初始化时间戳
		}
		c.reduceTasks = append(c.reduceTasks, task)
		c.taskStatus[c.nMap+i] = IDLE
	}

	c.server()
	log.Println("Coordinator server started")

	return &c
}
