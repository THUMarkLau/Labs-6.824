package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterWorkerArgs struct {
	Pid int
}

type RegisterWorkerReply struct {
	NReduce int
}

type CheckAllMapTaskFinishArgs struct {
}

type CheckAllMapTaskFinishReply struct {
	AllTaskFinish bool
}

type GetMapTaskArgs struct {
	Pid int
}

type GetMapTaskReply struct {
	GetTask  bool
	Filename string
	TaskId   int
}

type ReportMapTaskFinishArgs struct {
	Pid             int
	TaskId          int
	InputFile       string
	IntermediaFiles []string
}

type ReportMapTaskFinishReply struct {
	TaskAccept bool
}

type CheckAllReduceTaskFinishArgs struct {
}

type CheckAllReduceTaskFinishRely struct {
	AllTaskFinish bool
}

type GetReduceTaskArgs struct {
	Pid int
}

type GetReduceTaskReply struct {
	GetTask         bool
	IntermediaFiles []string
	TaskId          int
	ReduceId        int
}

type ReportReduceTaskFinishArgs struct {
	Pid        int
	OutputFile string
	ReduceId   int
	TaskId     int
}

type ReportReduceTaskFinishReply struct {
	TaskAccepted bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
