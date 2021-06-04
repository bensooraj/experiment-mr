package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskStatus struct {
	FilePath       string
	WorkerAssigned string
	LastUpdateTime time.Time
	State
}

type ReduceTaskStatus struct {
	ReduceFilePaths []string
	TaskStatus
}

type Coordinator struct {
	MapTasksRepo    map[int]*TaskStatus
	ReduceTasksRepo map[int]*ReduceTaskStatus
	WorkerIDCounter int32
	NReduce         int
	Context         context.Context
	ContextCancel   context.CancelFunc
	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignWorkerID(args *AssignWorkerIDArgs, reply *AssignWorkerIDReply) error {
	reply.WorkerID = c.WorkerIDCounter
	reply.NReduce = c.NReduce

	atomic.AddInt32(&c.WorkerIDCounter, 1)
	return nil
}

func (c *Coordinator) AllotTask(args *AllotTaskArgs, reply *AllotTaskReply) error {
	reply.IsTaskQueued = false

	c.Lock()
	defer c.Unlock()

	var allMapTasksComplete bool = true

	// Allot map tasks
	for mapID, status := range c.MapTasksRepo {
		if status.State == QUEUED {

			reply.TaskType = "map"
			reply.MapID = mapID
			reply.FileName = status.FilePath
			reply.FilePath = status.FilePath
			reply.IsTaskQueued = true

			c.MapTasksRepo[mapID].State = IN_PROGRESS
			c.MapTasksRepo[mapID].WorkerAssigned = args.WorkerName
			c.MapTasksRepo[mapID].LastUpdateTime = time.Now()

			log.Println("map task #", mapID, "on", status.FilePath, "assigned to", args.WorkerName)
			return nil
		}

		if status.State != COMPLETE {
			allMapTasksComplete = false
		}
	}

	if !allMapTasksComplete {
		return nil
	}

	// Allot reduce tasks
	for reduceID, status := range c.ReduceTasksRepo {
		if status.State == QUEUED {

			reply.TaskType = "reduce"
			reply.ReduceID = reduceID
			reply.ReduceFilePaths = status.ReduceFilePaths
			reply.IsTaskQueued = true

			status.State = IN_PROGRESS
			status.WorkerAssigned = args.WorkerName
			status.LastUpdateTime = time.Now()

			log.Println("reduce task #", reduceID, "assigned to", args.WorkerName)
			return nil
		}
	}
	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatuskReply) error {
	c.Lock()
	defer c.Unlock()
	if args.TaskType == "map" {

		c.MapTasksRepo[args.MapID].State = args.State

		if args.State == COMPLETE {
			for i := 0; i < c.NReduce; i++ {
				c.ReduceTasksRepo[i].ReduceFilePaths = append(c.ReduceTasksRepo[i].ReduceFilePaths, args.ReduceTasks[i])
				c.ReduceTasksRepo[i].State = QUEUED
			}
		}

		log.Println(args.MapFilePath, "map task completed. Reduce tasks", c.ReduceTasksRepo, "queued.")
	}

	if args.TaskType == "reduce" {

		c.ReduceTasksRepo[args.ReduceID].State = args.State

		log.Println("reduce task #", args.ReduceID, "completed.")
	}
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
	c.Lock()
	defer c.Unlock()

	// Verify that all map tasks are complete
	for _, tS := range c.MapTasksRepo {
		if tS.State != COMPLETE {
			return false
		}
	}

	// Verify that all reduce tasks are complete
	for _, tS := range c.ReduceTasksRepo {
		if tS.State != COMPLETE {
			return false
		}
	}

	c.ContextCancel()
	time.Sleep(5 * time.Second)

	log.Println("All map and reduce tasks are marked complete. Exiting...")

	return true
}

// Task health check
func (c *Coordinator) taskHealthCheck() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-c.Context.Done():
				log.Println("Exiting TaskHealthCheck")
				return
			case <-ticker.C:

				// map tasks
				c.Lock()
				for mapID, tS := range c.MapTasksRepo {
					elapsedTime := time.Since(tS.LastUpdateTime).Seconds()
					if tS.State == IN_PROGRESS && elapsedTime > 10 {
						log.Printf("Resetting map task #%d. It's been %f seconds since %s took it up.\n", mapID, elapsedTime, tS.WorkerAssigned)
						tS.State = QUEUED
						tS.WorkerAssigned = ""
					}
				}

				// reduce tasks
				for reduceID, tS := range c.ReduceTasksRepo {
					elapsedTime := time.Since(tS.LastUpdateTime).Seconds()
					if tS.State == IN_PROGRESS && elapsedTime > 10 {
						log.Printf("Resetting reduce task #%d. It's been %f seconds since %s took it up.\n", reduceID, elapsedTime, tS.WorkerAssigned)
						tS.State = QUEUED
						tS.WorkerAssigned = ""
					}
				}
				c.Unlock()
			}
		}
	}()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasksRepo:    make(map[int]*TaskStatus, len(files)),
		ReduceTasksRepo: make(map[int]*ReduceTaskStatus, nReduce),
		WorkerIDCounter: 0,
		NReduce:         nReduce,
	}

	c.Context, c.ContextCancel = context.WithCancel(context.Background())

	// Initialist the map tasks
	for i, fileName := range files {
		ts := &TaskStatus{
			FilePath: fileName,
			State:    QUEUED,
		}
		c.MapTasksRepo[i] = ts
	}

	// Initialist the map tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasksRepo[i] = &ReduceTaskStatus{}
	}
	c.taskHealthCheck()

	c.server()
	return &c
}
