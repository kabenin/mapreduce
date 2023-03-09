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

type WorkerState struct {
	Hartbeat time.Time
	Task     Task
}

type Coordinator struct {
	// Your definitions here.
	Workers  map[WorkerID]*WorkerState
	Files    []string
	Reduces  map[int]WorkerID
	mutex    sync.Mutex
	sleep    time.Duration
	nReduce  int
	complete bool
}

func (c *Coordinator) ReadyForTask(arg ReadyForTaskRequest, reply *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.NReduce = c.nReduce
	reply.ReduceTaskID = -1
	reply.FileName = ""
	if _, ok := c.Workers[arg.WorkerID]; ok {
		fmt.Printf("Worker %d reported readiness for a new task!\n", arg.WorkerID)
		if len(arg.CompleteFileName) != 0 {
			fmt.Printf("Worker %d reported completion of MAP task for file %s\n", arg.WorkerID, arg.CompleteFileName)

		}
		if arg.CompleteReduceTaskID != -1 {
			fmt.Printf("Worker %d reported completion of REDUCE task %d\n", arg.WorkerID, arg.CompleteReduceTaskID)
		}
		delete(c.Workers, arg.WorkerID)
	} else {
		fmt.Printf("New worker registered! %d\n", arg.WorkerID)

	}
	if len(c.Reduces) == 0 && len(c.Files) == 0 {
		fmt.Println("All jobs are complete")
		c.complete = true
		// I'm returning OK status, to keep workers running
		return nil
	}
	c.Workers[arg.WorkerID] = &WorkerState{Task: Task{}}
	c.Workers[arg.WorkerID].Hartbeat = time.Now()

	// We still have MAP tasks to do
	if len(c.Files) != 0 {
		c.Workers[arg.WorkerID].Task.FileName = c.Files[0]
		c.Files = c.Files[1:]
		reply.FileName = c.Workers[arg.WorkerID].Task.FileName
		fmt.Printf("Sending MAP for %s to worker %d\n", reply.FileName, arg.WorkerID)
		return nil
	}
	// Lets see, if all maps are done - start sending Reduce tasks
	// otherwise tell worker to wait
	maps := false
	for _, wrkr := range c.Workers {
		if len(wrkr.Task.FileName) != 0 {
			maps = true
			break
		}
	}
	if maps {
		fmt.Print("There are some maps running still...")
		return nil
	}

	if len(c.Reduces) == 0 {
		fmt.Println("All REDUCE tasks are done!")
		c.complete = true

	}
	// There are no more maps running. Assign a reduce
	for taskID := range c.Reduces {
		reply.ReduceTaskID = taskID
		delete(c.Reduces, reply.ReduceTaskID)
		c.Workers[arg.WorkerID].Task.ReduceTaskID = taskID
		fmt.Printf("Sending REDUCE task %d to worker %d\n", reply.ReduceTaskID, arg.WorkerID)
		return nil
	}
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
	go c.checkWorkers()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.Workers) > 0 {
		fmt.Printf("There are still %d workers running tasks\n", len(c.Workers))
		return false
	}
	return c.complete
}

func (c *Coordinator) checkWorkers() {
	for {
		now := time.Now()
		fmt.Printf("%v - Checking for stale workers\n", now)
		c.mutex.Lock()
		fmt.Printf("Current size of undone tasks: %d, workers: %d\n", len(c.Files), len(c.Workers))
		for id, wrkr := range c.Workers {
			elapsed := now.Sub(wrkr.Hartbeat)
			fmt.Printf("Checking worker %d. Last heartbeat was %d seconds ago\n", id, elapsed/time.Millisecond)
			if elapsed > 10*time.Second {
				fmt.Printf("Worker %v seemes to go offline. Resetting the task\n", id)
				if len(wrkr.Task.FileName) != 0 {
					c.Files = append(c.Files, wrkr.Task.FileName)
				}
				if wrkr.Task.ReduceTaskID != -1 {
					c.Reduces[wrkr.Task.ReduceTaskID] = -1
				}
				delete(c.Workers, id)

			}
		}
		sl := c.sleep
		c.mutex.Unlock()
		time.Sleep(sl)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Workers = map[WorkerID]*WorkerState{}
	c.Files = files
	c.nReduce = nReduce
	c.Reduces = map[int]WorkerID{}
	for i := 0; i < nReduce; i++ {
		c.Reduces[i] = -1
	}
	c.sleep = 1 * time.Second

	c.server()
	return &c
}
