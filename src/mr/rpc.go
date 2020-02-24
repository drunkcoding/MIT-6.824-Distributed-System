package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"strconv"
)

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

// Add your RPC definitions here.
type RegisterArgs struct {
}
type RegisterReply struct {
	No int
}

type ScheduleTaskArgs struct {
	Id int
}
type ScheduleTaskReply struct {
	Retcode ReturnCode
	Task    WorkerTask
	File    string
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type WorkerState int

const (
	WORKER_IDLE       WorkerState = 0
	WORKER_INPROGRESS WorkerState = 1
	WORKER_COMPLETED  WorkerState = 2
)

type WorkerTask int

const (
	TASK_NONE   WorkerTask = 0
	TASK_MAP    WorkerTask = 1
	TASK_REDUCE WorkerTask = 2
)

type ReturnCode int

const (
	SUCCESS       ReturnCode = 0
	NO_MORE_TASK  ReturnCode = 1
	WAIT_FOR_TASK ReturnCode = 2
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}
