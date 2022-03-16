package mr

import (
	logr "github.com/sirupsen/logrus"
	"io"
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
	ReduceNum                int
	workingWorkers           map[int]bool
	CurrentTaskId            int
	FinishReduceTaskNum      int
	FreeReduceTask           map[int]bool
	FinishReduceTask         map[int]bool
}

type TaskInfo struct {
	WorkerId  int
	TaskId    int
	IsMapTask bool
	Filename  string
	ReduceId  int
	StartTime int64
	Finish    bool
	Abort     bool
}

var SLEEP_TIME = 10 * time.Second
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
	reply.NReduce = c.ReduceNum
	return nil
}

func (c *Coordinator) CheckAllMapTaskFinish(_ *CheckAllMapTaskFinishArgs, reply *CheckAllMapTaskFinishReply) error {
	c.mutex.Lock()
	reply.AllTaskFinish = c.FinishInputFileNum >= len(c.inputFiles)
	logr.WithFields(logr.Fields{
		"FinishTaskNum":   c.FinishInputFileNum,
		"totalFileNum":    len(c.inputFiles),
		"allTaskFinished": reply.AllTaskFinish,
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
				reply.TaskId = c.CurrentTaskId
				reply.Filename = filename
				c.FreeInputFiles[filename] = false
				c.workingWorkers[args.Pid] = false
				taskInfo := TaskInfo{
					args.Pid,
					reply.TaskId,
					true,
					filename,
					-1,
					time.Now().Unix(),
					false,
					false,
				}
				c.TaskInfo[c.CurrentTaskId] = taskInfo
				c.CurrentTaskId++

				// start a go routine to check if the task should be aborted
				go func(taskId int, c *Coordinator) {
					time.Sleep(SLEEP_TIME)
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
			for i := 0; i < c.ReduceNum; i++ {
				c.IntermediaFilesForReduce[i] = append(c.IntermediaFilesForReduce[i], args.IntermediaFiles[i])
			}
		}

		logr.WithFields(logr.Fields{
			"WorkerId":           args.Pid,
			"TaskId":             args.TaskId,
			"Filename":           args.InputFile,
			"TotalFinishTaskNum": c.FinishInputFileNum,
			"TotalFileNum":       len(c.inputFiles),
		}).Info("Accept a finished task")
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) CheckAllReduceTaskFinish(_ *CheckAllReduceTaskFinishArgs, reply *CheckAllReduceTaskFinishRely) error {
	c.mutex.Lock()
	reply.AllTaskFinish = c.FinishReduceTaskNum >= c.ReduceNum
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.mutex.Lock()
	for i := 0; i < c.ReduceNum; i++ {
		if c.FreeReduceTask[i] {
			// found a not working reduce task
			c.FreeReduceTask[i] = false
			reply.GetTask = true
			reply.TaskId = c.CurrentTaskId
			reply.ReduceId = i
			c.CurrentTaskId++
			reply.IntermediaFiles = c.IntermediaFilesForReduce[i]
			taskInfo := TaskInfo{}
			taskInfo.TaskId = reply.TaskId
			taskInfo.IsMapTask = false
			taskInfo.WorkerId = args.Pid
			taskInfo.ReduceId = i
			taskInfo.StartTime = time.Now().Unix()
			c.TaskInfo[reply.TaskId] = taskInfo
			logr.WithFields(logr.Fields{
				"WorkerId":        args.Pid,
				"ReduceId":        i,
				"IntermediaFiles": reply.IntermediaFiles,
			}).Info("Assign a reduce task to worker")
			// start a routine to check if the task is out of time
			go func(coordinator *Coordinator, taskId int) {
				time.Sleep(SLEEP_TIME)
				c.mutex.Lock()
				if !c.TaskInfo[taskId].Finish {
					taskInfo := c.TaskInfo[taskId]
					taskInfo.Abort = true
					reduceId := taskInfo.ReduceId
					c.TaskInfo[taskId] = taskInfo
					c.FreeReduceTask[reduceId] = true
					logr.WithFields(logr.Fields{
						"WorkerId": taskInfo.WorkerId,
						"ReduceId": reduceId,
					}).Warn("Abort a reduce task because it is out of time")
				}
				c.mutex.Unlock()
			}(c, reply.TaskId)
			break
		}
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) ReportReduceTaskFinish(args *ReportReduceTaskFinishArgs, reply *ReportReduceTaskFinishReply) error {
	c.mutex.Lock()
	{
		taskInfo := c.TaskInfo[args.TaskId]
		if taskInfo.Abort {
			reply.TaskAccepted = false
			logr.WithFields(logr.Fields{
				"WorkerId": args.Pid,
				"TaskId":   args.TaskId,
			}).Warn("Reject a finished reduce task because it is out of time")
		} else {
			reply.TaskAccepted = true
			if !c.FinishReduceTask[args.ReduceId] {
				c.FinishReduceTask[args.ReduceId] = true
				c.FinishReduceTaskNum++
				taskInfo.Finish = true
				c.TaskInfo[args.TaskId] = taskInfo
				logr.WithFields(logr.Fields{
					"WorkerId":                   args.Pid,
					"ReduceId":                   args.ReduceId,
					"TotalFinishReduceTaskCount": c.FinishReduceTaskNum,
				}).Info("Accept a finished reduce task")
			}
		}
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) ReportReduceTaskFail(args *ReportReduceTaskFailArgs, reply *ReportReduceTaskFailReply) error {
	c.mutex.Lock()
	{
		logr.WithFields(logr.Fields{
			"WorkerId":args.WorkerId,
			"ReduceId":args.ReduceId,
		}).Warn("Accept a report of reduce fail task")
		c.FinishReduceTask[args.ReduceId] = false
		c.FreeReduceTask[args.ReduceId] = true
		c.FinishReduceTaskNum--
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

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.FinishReduceTaskNum >= c.ReduceNum
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// log settings
	logFileName := "coordinator.log"
	logFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logr.WithFields(logr.Fields{
			"Filename": logFileName,
		}).Error("Failed to open log file", err)
	} else {
		//out := io.MultiWriter(logFile, os.Stdout)
		out := io.MultiWriter(logFile)
		logr.SetOutput(out)
	}

	mutex := sync.Mutex{}
	freeFiles := make(map[string]bool)
	finishFiles := make(map[string]bool)
	for i := 0; i < len(files); i++ {
		freeFiles[files[i]] = true
		finishFiles[files[i]] = false
	}

	intermediaFilesForReduce := make(map[int][]string)
	freeReduceTask := make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		intermediaFilesForReduce[i] = []string{}
		freeReduceTask[i] = true
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
		0,
		freeReduceTask,
		make(map[int]bool),
	}
	logr.Info("Coordinator start to work")

	// Your code here.

	c.server()
	return &c
}
