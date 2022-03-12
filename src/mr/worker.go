package mr

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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
	Pid      int
	Filename string
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	nReduce  int
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

func (worker *WorkerStruct) executeMapTask() {
	file, err := os.Open(worker.Filename)
	if err != nil {
		logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": worker.Filename}).Error("Failed to open file")
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": worker.Filename}).Error("Failed to read file")
		return
	}
	file.Close()
	logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": worker.Filename}).Info("Successfully read from file, start to execute map task")
	keyValues := worker.mapf(worker.Filename, string(content))
	logr.WithFields(logr.Fields{"Pid": worker.Pid}).Info("Successfully execute the map task, start to write the output to intermediate files")
	sort.Sort(ByKey(keyValues))
	worker.writeMapResultToFile(keyValues)

}

func (worker *WorkerStruct) writeMapResultToFile(keyValues []KeyValue) {
	intermediateFiles := []string{}
	for i := 0; i < worker.nReduce; i++ {
		intermediateFiles = append(intermediateFiles, "mp-intermedia-"+strconv.Itoa(worker.Pid)+"-"+strconv.Itoa(i))
	}
	logr.WithFields(logr.Fields{"Pid": worker.Pid, "Files": intermediateFiles}).Info("Intermedia files for map task")
	totalSize := len(keyValues)
	currentFileIdx := 0
	writeIdxForCurFile := 0
	lengthForEachFile := totalSize / worker.nReduce
	tempStr := ""
	for i := 0; i < totalSize; {
		j := i
		for ; j+1 < totalSize && keyValues[j].Key == keyValues[j+1].Key; j++ {
			tempStr = tempStr + fmt.Sprintf("%v %v\n", keyValues[j].Key, keyValues[j].Value)
		}
		tempStr = tempStr + fmt.Sprintf("%v %v\n", keyValues[j].Key, keyValues[j].Value)
		if (j - writeIdxForCurFile) >= lengthForEachFile {
			err := ioutil.WriteFile(intermediateFiles[currentFileIdx], []byte(tempStr), fs.ModePerm)
			if err != nil {
				logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": intermediateFiles[currentFileIdx]}).Error("Failed to write map output to this file")
			} else {
				logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": intermediateFiles[currentFileIdx]}).Info("Write map intermediate output tot this file")
			}
			tempStr = ""
			currentFileIdx++
			writeIdxForCurFile = j
		}
		i = j + 1
	}
	if tempStr != "" {
		err := ioutil.WriteFile(intermediateFiles[currentFileIdx], []byte(tempStr), fs.ModePerm)
		if err != nil {
			logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": intermediateFiles[currentFileIdx]}).Error("Failed to write map output to this file")
		} else {
			logr.WithFields(logr.Fields{"Pid": worker.Pid, "Filename": intermediateFiles[currentFileIdx]}).Info("Write map intermediate output tot this file")
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	pid := unix.Getpid()
	registerMapArgs := RegisterMapWorkerArgs{pid}
	registerMapReply := RegisterMapWorkerReply{}
	logr.WithFields(logr.Fields{
		"Pid": pid,
	}).Info("Try to register map worker with pid")
	ok := call("Coordinator.RegisterMapWorker", &registerMapArgs, &registerMapReply)
	if !ok {
		logr.Error("Fail to make rpc call")
		return
	}
	logr.WithFields(logr.Fields{
		"Pid":      pid,
		"Filename": registerMapReply.Filename,
		"NReduce":  registerMapReply.NReduce,
	}).Info("Successfully register reduce, get input filename, start to create worker")
	if registerMapReply.NReduce <= 0 || registerMapReply.Filename == "" {
		logr.WithFields(logr.Fields{"NReduce": registerMapReply.NReduce, "Filename": registerMapReply.Filename}).Error("Invalid params")
		return
	}
	worker := WorkerStruct{pid, registerMapReply.Filename, mapf, reducef, registerMapReply.NReduce}
	worker.executeMapTask()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
