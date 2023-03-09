package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Wrkr struct {
	WorkerID WorkerID
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wrkr := &Wrkr{
		WorkerID: WorkerID(os.Getpid()),
	}
	req := ReadyForTaskRequest{WorkerID: wrkr.WorkerID}

	for {
		fmt.Printf("Worker %d: talking to Coordinator...\n", wrkr.WorkerID)
		task := Task{}

		ok := call("Coordinator.ReadyForTask", req, &task)
		if !ok {
			log.Fatalf("Error returned by RPC")
		}
		fmt.Printf("Reply: %+v\n", task)

		if len(task.FileName) > 0 {
			wrkr.DoMap(task.FileName, task.NReduce, mapf)
			req.CompleteFileName = task.FileName
		} else {
			req.CompleteFileName = ""
		}

		if task.ReduceTaskID != -1 {
			fmt.Printf("I Got reduce task: %d\n", task.ReduceTaskID)
			wrkr.DoReduce(reducef, task.ReduceTaskID)
			req.CompleteReduceTaskID = task.ReduceTaskID
		} else {
			req.CompleteReduceTaskID = -1

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (w *Wrkr) DoMap(filename string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// open files
	ofiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	map_job_id := ihash(filename)
	for i := 0; i < nReduce; i++ {
		ofiles[i], err = os.Create(fmt.Sprintf("mr-%d-%d.int.json", map_job_id, i))
		if err != nil {
			log.Fatal("Error opening intrmediate file: ", err)
		}
		encoders[i] = json.NewEncoder(ofiles[i])
		// err := encoders[i].Encode(KeyValue{Key: "tra", Value: "tata"})
		// if err != nil {
		// 	log.Fatal("HAHA! ", err)
		// }
		defer ofiles[i].Close()
	}

	for _, kv := range kva {
		err := encoders[ihash(kv.Key)%nReduce].Encode(kv)
		if err != nil {
			log.Fatalf("Error encoding %+v: %v", kv, err)
		}
	}
}

func (w *Wrkr) DoReduce(reducef func(string, []string) string, reduceTaskId int) {
	dirs, err := os.ReadDir(".")
	if err != nil {
		log.Fatal("Error reading directory '.': ", err)
	}
	kva := map[string][]string{}
	for _, d := range dirs {
		if d.IsDir() {
			continue
		}
		if strings.HasPrefix(d.Name(), "mr-") && strings.HasSuffix(d.Name(), fmt.Sprintf("-%d.int.json", reduceTaskId)) {
			file, err := os.Open(d.Name())
			if err != nil {
				log.Fatalf("Error opening file '%v': %v", d.Name(), err)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				if _, ok := kva[kv.Key]; !ok {
					kva[kv.Key] = []string{}
				}
				kva[kv.Key] = append(kva[kv.Key], kv.Value)
			}
			file.Close()
		}
	}
	oname := fmt.Sprintf("mr-out-dist-%v", reduceTaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	for k, v := range kva {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	ofile.Close()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
