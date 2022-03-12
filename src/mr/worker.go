package mr

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "golang.org/x/sys/unix"
import logr "github.com/sirupsen/logrus"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerStruct struct {
	Pid           int
	Filename      string
	mapf          func(string, string) []KeyValue
	reducef       func(string, []string) string
	nReduce       int
	currentTaskId int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var logger *logr.Logger

func (worker *WorkerStruct) checkAllMapTaskFinish() bool {
	logr.WithFields(logr.Fields{
		"WorkerId": worker.Pid,
	}).Info("Checking if all the map tasks are finished")

	args := CheckAllMapTaskFinishArgs{}
	reply := CheckAllMapTaskFinishReply{}

	call("Coordinator.CheckAllMapTaskFinish", &args, &reply)

	if !reply.AllTaskFinish {
		logr.WithFields(logr.Fields{
			"WorkerId": worker.Pid,
		}).Info("There are some map tasks still unfinished")
	} else {
		logr.WithFields(logr.Fields{
			"WorkerId": worker.Pid,
		}).Info("All map tasks in coordinator are finish")
	}
	return reply.AllTaskFinish
}

func (worker *WorkerStruct) getTaskFromCoordinator() bool {
	logr.WithFields(logr.Fields{
		"WorkerId": worker.Pid,
	}).Info("Try to get map task from coordinator")

	getMapTaskArgs := GetMapTaskArgs{worker.Pid}
	getMapTaskReply := GetMapTaskReply{}

	call("Coordinator.GetMapTask", &getMapTaskArgs, &getMapTaskReply)

	if getMapTaskReply.GetTask {
		logr.WithFields(logr.Fields{
			"WorkerId": worker.Pid,
			"Filename": getMapTaskReply.Filename,
			"TaskId":   getMapTaskReply.TaskId,
		}).Info("Get map task from coordinator")
		worker.currentTaskId = getMapTaskReply.TaskId
		worker.Filename = getMapTaskReply.Filename
	} else {
		logr.WithFields(logr.Fields{
			"WorkerId": worker.Pid,
		}).Info("Failed to get map task from coordinator")
	}

	return getMapTaskReply.GetTask
}

func (worker *WorkerStruct) executeMapTask() []string {
	file, err := os.Open(worker.Filename)
	if err != nil {
		logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": worker.Filename}).Error("Failed to open file")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": worker.Filename}).Error("Failed to read file")
	}
	file.Close()
	logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": worker.Filename}).Info("Successfully read from file, start to execute map task")

	keyValues := worker.mapf(worker.Filename, string(content))
	logr.WithFields(logr.Fields{"WorkerId": worker.Pid}).Info("Successfully execute the map task, start to write the output to intermediate files")

	sort.Sort(ByKey(keyValues))
	return worker.writeMapResultToFile(keyValues)
}

func (worker *WorkerStruct) writeMapResultToFile(keyValues []KeyValue) []string {
	intermediateFileNames := []string{}
	intermediateFiles := []*os.File{}
	for i := 0; i < worker.nReduce; i++ {
		filename := "mp-intermedia-" + strconv.Itoa(worker.currentTaskId) + "-" + strconv.Itoa(i)
		intermediateFileNames = append(intermediateFileNames, filename)
		fileWriter, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logr.WithFields(logr.Fields{"Filename": filename}).Error("Fail to open file")
			return nil
		} else {
			intermediateFiles = append(intermediateFiles, fileWriter)
		}
	}
	logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Files": intermediateFileNames}).Info("Intermedia files for map task")

	totalSize := len(keyValues)
	currentFileIdx := 0
	tempStr := ""
	for i := 0; i < totalSize; {
		j := i
		for ; j+1 < totalSize && keyValues[j].Key == keyValues[j+1].Key; j++ {
			tempStr = tempStr + fmt.Sprintf("%v %v\n", keyValues[j].Key, keyValues[j].Value)
		}
		tempStr = tempStr + fmt.Sprintf("%v %v\n", keyValues[j].Key, keyValues[j].Value)
		fileIdx := ihash(keyValues[j].Key) % worker.nReduce
		fileWriter := intermediateFiles[fileIdx]
		_, err := fileWriter.Write([]byte(tempStr))
		if err != nil {
			logr.Error(err)
			logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": intermediateFileNames[currentFileIdx]}).Error("Failed to write map output to this file")
		} else {
			logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": intermediateFileNames[currentFileIdx]}).Info("Write map intermediate output tot this file")
		}
		tempStr = ""
		i = j + 1
	}

	if tempStr != "" {
		err := ioutil.WriteFile(intermediateFileNames[currentFileIdx], []byte(tempStr), fs.ModePerm)
		if err != nil {
			logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": intermediateFileNames[currentFileIdx]}).Error("Failed to write map output to this file")
		} else {
			logr.WithFields(logr.Fields{"WorkerId": worker.Pid, "Filename": intermediateFileNames[currentFileIdx]}).Info("Write map intermediate output tot this file")
		}
	}
	logr.WithFields(logr.Fields{
		"WorkerId": worker.Pid,
	}).Info("All data in map task is written into files")


	for _, writer := range intermediateFiles {
		writer.Close()
	}
	return intermediateFileNames
}

func (worker *WorkerStruct) reportMapTaskFinish(intermediaFiles []string) {
	logr.WithFields(logr.Fields{
		"WorkerId": worker.Pid,
		"TaskId":   worker.currentTaskId,
	}).Info("All content in current map task is finish, report to the coordinator")

	args := ReportMapTaskFinishArgs{
		worker.Pid,
		worker.currentTaskId,
		worker.Filename,
		intermediaFiles,
	}
	reply := ReportMapTaskFinishReply{}

	call("Coordinator.ReportMapTaskFinish", &args, &reply)

	if reply.TaskAccept {
		logr.WithFields(logr.Fields{
			"WorkerId": worker.Pid,
			"TaskId":   worker.currentTaskId,
		}).Info("A map task is accepted")
	} else {
		logr.WithFields(logr.Fields{
			"WorkerId": worker.Pid,
			"TaskId":   worker.currentTaskId,
		}).Info("A map task is not accepted, delete the intermedia files")

		for _, intermediaFile := range intermediaFiles {
			os.Remove(intermediaFile)
		}
	}
	worker.currentTaskId = -1
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	pid := unix.Getpid()

	registerMapArgs := RegisterWorkerArgs{pid}
	registerMapReply := RegisterWorkerReply{}

	logr.WithFields(logr.Fields{
		"WorkerId": pid,
	}).Info("Try to register map worker with pid")
	ok := call("Coordinator.RegisterWorker", &registerMapArgs, &registerMapReply)
	if !ok {
		logr.Error("Fail to make rpc call")
		return
	}

	logr.WithFields(logr.Fields{
		"WorkerId": pid,
		"NReduce":  registerMapReply.NReduce,
	}).Info("Successfully register worker")
	if registerMapReply.NReduce <= 0 {
		logr.WithFields(logr.Fields{"NReduce": registerMapReply.NReduce}).Error("Invalid params")
		return
	}
	worker := WorkerStruct{pid, "", mapf, reducef, registerMapReply.NReduce, -1}

	// working on map task
	for !worker.checkAllMapTaskFinish() {
		if worker.getTaskFromCoordinator() {
			intermediaFiles := worker.executeMapTask()
			worker.reportMapTaskFinish(intermediaFiles)
		}
		time.Sleep(2 * time.Second)
	}

	logr.WithFields(logr.Fields{
		"WorkerId": worker.Pid,
	}).Info("Start to get reduce task")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
