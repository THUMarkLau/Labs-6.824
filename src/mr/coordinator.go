package mr

import (
	logr "github.com/sirupsen/logrus"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mutex                    sync.Mutex
	inputFiles               []string
	FinishInputFileNum       int
	FreeInputFiles           map[string]bool
	FinishInputFiles         map[string]bool
	IntermediaFilesForReduce map[int][]string
	TaskInfo                 map[int]TaskInfo
	reduceNum                int
	workingWorkers           map[int]bool
	currentTaskId            int
}

type TaskInfo struct {
	WorkerId  int
	TaskId    int
	IsMapTask bool
	Filename  string
	StartTime int64
	Finish    bool
	Abort     bool
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

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mutex.Lock()
	c.workingWorkers[args.Pid] = false
	c.mutex.Unlock()
	reply.NReduce = c.reduceNum
	return nil
}

func (c *Coordinator) CheckAllMapTaskFinish(_ *CheckAllMapTaskFinishArgs, reply *CheckAllMapTaskFinishReply) error {
	c.mutex.Lock()
	reply.AllTaskFinish = c.FinishInputFileNum >= len(c.inputFiles)
	logr.WithFields(logr.Fields{
		"FinishTaskNum": c.FinishInputFileNum,
		"totalFileNum" : len(c.inputFiles),
		"allTaskFinished" : reply.AllTaskFinish,
	}).Info("Check all map task finish")
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.mutex.Lock()

	{
		for _, filename := range c.inputFiles {
			if c.FreeInputFiles[filename] {
				// found a file to be assigned
				reply.GetTask = true
				reply.TaskId = c.currentTaskId
				reply.Filename = filename
				c.FreeInputFiles[filename] = false
				c.workingWorkers[args.Pid] = false
				taskInfo := TaskInfo{
					args.Pid,
					reply.TaskId,
					true,
					filename,
					time.Now().Unix(),
					false,
					false,
				}
				c.TaskInfo[c.currentTaskId] = taskInfo
				c.currentTaskId++

				// start a go routine to check if the task should be aborted
				go func(taskId int, c *Coordinator) {
					time.Sleep(10 * time.Second)
					c.mutex.Lock()
					info := c.TaskInfo[taskId]
					if !info.Finish {
						info.Abort = true
						c.FreeInputFiles[taskInfo.Filename] = true
						logr.WithFields(logr.Fields{
							"TaskId":   info.TaskId,
							"WorkerId": info.WorkerId,
							"Filename": info.Filename,
						}).Info("Abort a task because it is out of time")
					} else {
						logr.WithFields(logr.Fields{
							"TaskId":   info.TaskId,
							"WorkerId": info.WorkerId,
							"Filename": info.Filename,
						}).Info("Task is finish, not need to abort it")
					}
					c.mutex.Unlock()
				}(taskInfo.TaskId, c)
				break
			}
		}
	}

	c.mutex.Unlock()
	logr.WithFields(logr.Fields{
		"Worker": args.Pid,
		"File":   reply.Filename,
		"TaskId": reply.TaskId,
	}).Info("Assign map task to worker")
	return nil
}

func (c *Coordinator) ReportMapTaskFinish(args *ReportMapTaskFinishArgs, reply *ReportMapTaskFinishReply) error {
	c.mutex.Lock()
	taskInfo := c.TaskInfo[args.TaskId]
	if taskInfo.Abort {
		// the task execute out of time,
		// so it is aborted
		reply.TaskAccept = false
	} else {
		reply.TaskAccept = true
		taskInfo.Finish = true
		c.TaskInfo[args.TaskId] = taskInfo
		if !c.FinishInputFiles[args.InputFile] {
			c.FinishInputFileNum++
			c.FinishInputFiles[args.InputFile] = true
			for i := 0; i < c.reduceNum; i++ {
				c.IntermediaFilesForReduce[i] = append(c.IntermediaFilesForReduce[i], args.IntermediaFiles[i])
			}
		}

		logr.WithFields(logr.Fields{
			"WorkerId":args.Pid,
			"TaskId":args.TaskId,
			"Filename":args.InputFile,
			"TotalFinishTaskNum":c.FinishInputFileNum,
			"TotalFileNum":len(c.inputFiles),
		}).Info("Accept a finished task")
	}
	c.mutex.Unlock()
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
	// log settings

	mutex := sync.Mutex{}
	freeFiles := make(map[string]bool)
	finishFiles := make(map[string]bool)
	for i := 0; i < len(files); i++ {
		freeFiles[files[i]] = true
		finishFiles[files[i]] = false
	}

	intermediaFilesForReduce := make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		intermediaFilesForReduce[i] = []string{}
	}

	c := Coordinator{
		mutex,
		files,
		0,
		freeFiles,
		finishFiles,
		intermediaFilesForReduce,
		make(map[int]TaskInfo),
		nReduce,
		make(map[int]bool),
		0,
	}

	// Your code here.

	c.server()
	return &c
}
