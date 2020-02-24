package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskQueue struct {
	files  map[string]bool
	reduce []bool
}

type Master struct {
	// Your definitions here.
	queue   TaskQueue
	workers []WorkerState
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	reply.No = len(m.workers)
	m.workers = append(m.workers, WORKER_IDLE)
	return nil
}

func (m *Master) ScheduleTask(args *ScheduleTaskArgs, reply *ScheduleTaskReply) error {

	for file, done := range m.queue.files {
		if !done {
			m.workers[args.Id] = WORKER_INPROGRESS
			m.queue.files[file] = true

			reply.Retcode = SUCCESS
			reply.Task = TASK_MAP
			reply.File = file
			return nil
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.queue.files = make(map[string]bool)
	m.queue.reduce = make([]bool, 0)
	m.workers = make([]WorkerState, 0)
	m.nReduce = nReduce

	for _, file := range files {
		m.queue.files[file] = false
	}

	m.server()
	return &m
}
