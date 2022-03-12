package mr

import (
	logr "github.com/sirupsen/logrus"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	registerMutex    sync.Mutex
	remainingFileIdx int
	inputFiles       []string
	fileToWorkerMap  map[string]int
	reduceNum        int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterMapWorker(args *RegisterMapWorkerArgs, reply *RegisterMapWorkerReply) error {
	c.registerMutex.Lock()
	reply.Filename = c.inputFiles[c.remainingFileIdx]
	reply.NReduce = c.reduceNum
	c.remainingFileIdx++
	c.fileToWorkerMap[reply.Filename] = args.Pid
	c.registerMutex.Unlock()
	logr.WithFields(logr.Fields{
		"worker": args.Pid,
		"file":   reply.Filename,
		"NReduce": c.reduceNum,
	}).Info("Assign file to worker")
	return nil
}

func (c*Coordinator) ReportFinishMapTask(args *ReportFinishMapTaskArgs, reply *ReportFinishMapTaskReply) error {
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mutex := sync.Mutex{}
	c := Coordinator{
		mutex, 0, files, make(map[string]int), nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
