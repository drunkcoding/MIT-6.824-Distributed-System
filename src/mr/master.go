package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskQueue struct {
	files  map[string]bool
	reduce []bool
}

type WorkerStatus struct {
	state      WorkerState
	lastactive time.Time
	file       string
	reduce     int
}

type Master struct {
	// Your definitions here.
	queue   TaskQueue
	workers []WorkerStatus
	nReduce int
	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.Lock()
	defer m.Unlock()
	reply.No = len(m.workers)
	m.workers = append(m.workers, WorkerStatus{WORKER_IDLE, time.Now(), "", -1})
	return nil
}

func (m *Master) ScheduleTask(args *ScheduleTaskArgs, reply *ScheduleTaskReply) error {
	m.Lock()
	defer m.Unlock()

	reply.Retcode = NO_MORE_TASK

	m.workers[args.Id].lastactive = time.Now()
	if m.workers[args.Id].state == WORKER_MAP_INPROGRESS || m.workers[args.Id].state == WORKER_REDUCE_INPROGRESS {
		m.workers[args.Id].state = WORKER_COMPLETED
	}

	fmt.Printf("Worker %v ask for task\n", args.Id)

	CheckWorkerAlive(m)

	for file, done := range m.queue.files {
		if !done {
			m.workers[args.Id].state = WORKER_MAP_INPROGRESS
			m.workers[args.Id].file = file
			m.queue.files[file] = true

			reply.Retcode = SUCCESS
			reply.Task = TASK_MAP
			reply.File = file
			reply.NReduce = m.nReduce

			fmt.Printf("Map schedule id:%v|file:%v\n", args.Id, file)
			return nil
		}
	}

	if !MapAllComplete(m) {
		fmt.Printf("Map wait for all id:%v\n", args.Id)
		reply.Retcode = WAIT_FOR_TASK
		return nil
	}

	for i, done := range m.queue.reduce {
		if !done {
			m.workers[args.Id].state = WORKER_REDUCE_INPROGRESS
			m.workers[args.Id].reduce = i
			m.queue.reduce[i] = true

			reply.Retcode = SUCCESS
			reply.Task = TASK_REDUCE
			reply.File = "mr-*-" + strconv.Itoa(i)
			reply.NReduce = i

			fmt.Printf("Reduce schedule id:%v|file:%v\n", args.Id, reply.File)
			return nil
		}
	}

	fmt.Printf("Worker %v schedule nothing\n", args.Id)

	return nil
}

func MapAllComplete(m *Master) (completed bool) {
	completed = true
	for _, ok := range m.queue.files {
		completed = completed && ok
	}
	for _, worker := range m.workers {
		completed = completed && (worker.state != WORKER_MAP_INPROGRESS || worker.state == WORKER_IDLE)
	}
	return
}

func ReduceAllComplete(m *Master) (completed bool) {
	completed = true
	for _, ok := range m.queue.reduce {
		completed = completed && ok
	}
	for _, worker := range m.workers {
		completed = completed && (worker.state != WORKER_REDUCE_INPROGRESS || worker.state == WORKER_IDLE)
	}
	return
}

func CheckWorkerAlive(m *Master) {
	now := time.Now()
	for i, worker := range m.workers {
		duration := now.Sub(worker.lastactive)
		if duration > 10*time.Second && worker.state != WORKER_COMPLETED && worker.state != WORKER_IDLE {
			m.workers[i].state = WORKER_IDLE
			if worker.state == WORKER_MAP_INPROGRESS {
				m.queue.files[m.workers[i].file] = false
				m.workers[i].file = ""
			}
			if worker.state == WORKER_REDUCE_INPROGRESS {
				m.queue.reduce[m.workers[i].reduce] = false
				m.workers[i].reduce = -1
			}
			fmt.Printf("Worker %v is down, reschedule file%v|reduce:%v\n", i, m.workers[i].file, m.workers[i].reduce)
		}
	}
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
	m.Lock()
	defer m.Unlock()

	ret := ReduceAllComplete(m)

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
	m.queue.reduce = make([]bool, nReduce)
	m.workers = make([]WorkerStatus, 0)
	m.nReduce = nReduce

	for _, file := range files {
		m.queue.files[file] = false
	}

	m.server()
	return &m
}
