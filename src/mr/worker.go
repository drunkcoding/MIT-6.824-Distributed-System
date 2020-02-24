package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := RegisterMyself()

	quit := make(chan int)
	go QueryTask(id, mapf, reducef, quit)
	<-quit

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func RegisterMyself() int {
	args := RegisterArgs{}
	reply := RegisterReply{}

	call("Master.Register", &args, &reply)
	fmt.Printf("My worker id is %v\n", reply.No)
	return reply.No
}

func QueryTask(id int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, quit chan int) {

	defer close(quit)
	for {
		args := ScheduleTaskArgs{}
		args.Id = id
		reply := ScheduleTaskReply{}
		call("Master.ScheduleTask", &args, &reply)

		if reply.Retcode == NO_MORE_TASK {
			break
		}

		if reply.Retcode == SUCCESS {
			switch reply.Task {
			case TASK_MAP:
				DoMap(id, reply.File, reply.NReduce, mapf)
			case TASK_REDUCE:
				DoReduce(id, reply.File, reply.NReduce, reducef)
			}
		}

		time.Sleep(time.Second)
	}
}

func DoMap(id int, filename string, nslot int, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(filename)
	check(err)

	fmt.Printf("Worker %v DoMap on %v\n", id, filename)

	kva := mapf(filename, string(content))

	for i := 0; i < nslot; i++ {
		target := "/data/MIT-6.824-Distributed-System/src/main/mr-tmp-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)

		file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE, 0644)
		check(err)
		defer file.Close()

		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key)%nslot == i {
				err := enc.Encode(&kv)
				check(err)
			}

		}
	}

}

func DoReduce(id int, filename string, nslot int, reducef func(string, []string) string) {
	var files []string

	fmt.Printf("Worker %v DoReduce\n", id)

	root := "/data/MIT-6.824-Distributed-System/src/main"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	check(err)

	kvm := make(map[string][]string)

	for _, sub := range files {
		matched, err := filepath.Match(filepath.Join(root, filename), sub)
		check(err)
		if matched {

			fmt.Printf("List file %v with pattern %v matched %v\n", sub, filename, matched)

			kva := make([]KeyValue, 0)

			file, err := os.OpenFile(sub, os.O_RDONLY, 0644)
			check(err)
			defer file.Close()

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)

			}

			for _, kv := range kva {
				if _, ok := kvm[kv.Key]; !ok {
					kvm[kv.Key] = make([]string, 0)
				}
				kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
			}

		}
	}

	fmt.Printf("Reduce output write to file %v, size %v\n", "./mr-out-"+strconv.Itoa(nslot), len(kvm))

	file, err := os.OpenFile("./mr-out-"+strconv.Itoa(nslot), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	check(err)
	defer file.Close()

	for k, v := range kvm {
		_, err = file.WriteString(k + " " + reducef(k, v) + "\n")
		check(err)
	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
