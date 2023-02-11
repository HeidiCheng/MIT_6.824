package mr

import (
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
	nReduce         int
	nMap            int
	finishedMaps    int
	finishedReduces int
	mapChecks       []bool
	reduceChecks    []bool
	mapWorks        []int
	reduceWorks     []int
	files           []string

	// locks
	mapLock             sync.Mutex
	reduceLock          sync.Mutex
	mapCheckLock        sync.Mutex
	reduceCheckLock     sync.Mutex
	finishedMapsLock    sync.Mutex
	finishedReducesLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Assign(req *Request, assignedWork *AssignedWork) error {
	// decide to assign map or reduce task
	c.finishedMapsLock.Lock()
	defer c.finishedMapsLock.Unlock()
	if c.finishedMaps == c.nMap {
		// all tasks have been done
		c.finishedReducesLock.Lock()
		defer c.finishedReducesLock.Unlock()
		if c.finishedReduces == c.nReduce {
			assignedWork.Type = Exit
		} else {
			c.reduceLock.Lock()
			remainReduce := len(c.reduceWorks)
			c.reduceLock.Unlock()
			if remainReduce == 0 {
				assignedWork.Type = Wait
			} else {
				assignedWork.Type = Reduce
				c.reduceLock.Lock()
				assignedWork.TaskN = c.reduceWorks[0]
				c.reduceWorks = c.reduceWorks[1:]
				c.reduceLock.Unlock()
				//fmt.Printf("reduceworks: %v", c.reduceWorks)
				assignedWork.Filename = ""
				assignedWork.NReduce = c.nReduce
				go c.checkWorkFinished(10, assignedWork.TaskN, Reduce)
			}
		}
	} else {
		c.mapLock.Lock()
		remainMap := len(c.mapWorks)
		c.mapLock.Unlock()
		// There is no map task to be assigned but not all map tasks has finished.
		if remainMap == 0 {
			assignedWork.Type = Wait
		} else {
			assignedWork.Type = Map
			c.mapLock.Lock()
			assignedWork.TaskN = c.mapWorks[0]
			c.mapWorks = c.mapWorks[1:]
			c.mapLock.Unlock()
			assignedWork.Filename = c.files[assignedWork.TaskN]
			assignedWork.NReduce = c.nReduce
			go c.checkWorkFinished(10, assignedWork.TaskN, Map)
		}
	}
	return nil
}

func (c *Coordinator) Acknowledge(ack *Ack, reply *Reply) error {
	switch ack.Type {
	case Map:
		c.mapCheckLock.Lock()
		c.mapChecks[ack.TaskN] = true
		c.mapCheckLock.Unlock()
	case Reduce:
		c.reduceCheckLock.Lock()
		c.reduceChecks[ack.TaskN] = true
		c.reduceCheckLock.Unlock()
	default:
		break
	}
	return nil
}

func (c *Coordinator) checkWorkFinished(waitTime time.Duration, TaskN int, Type WorkType) error {
	// wait for 10s
	time.Sleep(waitTime * time.Second)
	switch Type {
	case Map:
		c.mapCheckLock.Lock()
		// if the task has not finished in a time period
		if c.mapChecks[TaskN] == false {
			c.mapLock.Lock()
			c.mapWorks = append(c.mapWorks, TaskN)
			c.mapLock.Unlock()
		} else {
			c.finishedMapsLock.Lock()
			c.finishedMaps += 1
			c.finishedMapsLock.Unlock()
		}
		c.mapCheckLock.Unlock()
	case Reduce:
		c.reduceCheckLock.Lock()
		if c.reduceChecks[TaskN] == false {
			c.reduceLock.Lock()
			c.reduceWorks = append(c.reduceWorks, TaskN)
			c.reduceLock.Unlock()
		} else {
			c.finishedReducesLock.Lock()
			c.finishedReduces += 1
			c.finishedReducesLock.Unlock()
		}
		c.reduceCheckLock.Unlock()
	default:
		break
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	c.finishedMapsLock.Lock()
	c.finishedReducesLock.Lock()
	// Your code here.
	if c.finishedMaps == c.nMap && c.finishedReduces == c.nReduce {
		ret = true
	}
	c.finishedMapsLock.Unlock()
	c.finishedReducesLock.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, nMap: len(files), finishedMaps: 0, finishedReduces: 0, files: files}
	// Your code here.

	// init records for map and reduce tasks
	c.mapWorks = make([]int, c.nMap)
	c.mapChecks = make([]bool, c.nMap)
	c.reduceWorks = make([]int, nReduce)
	c.reduceChecks = make([]bool, nReduce)

	// make the task pool
	for i := 0; i < c.nMap; i++ {
		c.mapWorks[i] = i
		c.mapChecks[i] = false
	}
	for i := 0; i < nReduce; i++ {
		c.reduceWorks[i] = i
		c.reduceChecks[i] = false
	}

	c.server()
	return &c
}
