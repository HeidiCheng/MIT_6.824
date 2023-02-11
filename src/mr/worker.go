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
	"time"
)

// Change to the directory on your environment which stores intermidiate and output files
const HOME_DIR = "/Users/heidicheng/Desktop/heidi/Distributed_Systems_mit/labs/6.824/src/main/mr-tmp/"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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

	// Your worker implementation here.
	for {
		// declare request for getting work
		req := Request{}
		// declare a reply from coordinator
		assignedWork := AssignedWork{}
		RequestWork(&req, &assignedWork)
		if assignedWork.Type == Map {
			workerMap(mapf, assignedWork.Filename, assignedWork.TaskN, assignedWork.NReduce)
		} else if assignedWork.Type == Reduce {
			workerReduce(reducef, assignedWork.TaskN)
		} else if assignedWork.Type == Wait {
			// Wait until map tasks all finished
			time.Sleep(2)
		} else {
			// exit the loop
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func RequestWork(req *Request, assignedWork *AssignedWork) {
	// send the RPC request for getting work, wait for the reply
	call("Coordinator.Assign", &req, &assignedWork)
}

func AckFinished(ack *Ack, reply *Reply) {
	// send the RPC ackenowledge when task is done
	call("Coordinator.Acknowledge", &ack, &reply)
}

func request(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, &args, &reply)
	if err == nil {
		return true
	}

	log.Fatal("Call Error: ", err)
	return false
}

func workerMap(mapf func(string, string) []KeyValue, inputFile string, TaskN int, nReduce int) bool {
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %s", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %s", inputFile)
	}
	file.Close()

	// execute map for target input file
	result := mapf(inputFile, string(content))

	// put result into buckets based on hashed result
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range result {
		index := ihash(kv.Key) % nReduce
		buckets[index] = append(buckets[index], kv)
	}

	// create files for saving intermediate result
	for i, bucket := range buckets {
		Filename := fmt.Sprintf("mr-%v-%v", TaskN, i)
		// create temporary file first to prevent two workers writing into the same file
		tmpFile, tmpErr := ioutil.TempFile(HOME_DIR, "TEMP")
		if tmpErr != nil {
			log.Fatalf("cannot create temporary file for %s", Filename)
		}
		enc := json.NewEncoder(tmpFile)
		for _, bucketkv := range bucket {
			err = enc.Encode(&bucketkv)
			if err != nil {
				log.Fatalf("Key-value pair cannot encode to json")
			}
		}
		// change temporary file's name to after finish encoding
		err = os.Rename(tmpFile.Name(), HOME_DIR+Filename)
		if err != nil {
			log.Printf("cannot create file %s", Filename)
			return false
		}
	}

	AckFinished(&Ack{TaskN: TaskN, Type: Map}, &Reply{})
	return true
}

func workerReduce(reducef func(string, []string) string, TaskN int) bool {
	pattern := fmt.Sprintf("mr-*-%d", TaskN)
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("cannot read files with pattern: %v", pattern)
	}

	oname := fmt.Sprintf("mr-out-%v", TaskN)
	ofile, err := ioutil.TempFile(HOME_DIR, oname)
	if err != nil {
		log.Fatalf("cannot create temporary file for %v", oname)
	}
	// read input from intermediate file
	m := make(map[string][]string)
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Fatalf("Cannot read file: %v", f)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
	}

	for k, v := range m {
		output := reducef(k, v)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}

	ofile.Close()
	err = os.Rename(ofile.Name(), HOME_DIR+oname)
	if err != nil {
		// another worker might already take over the job
		log.Printf("cannot create file %v", oname)
		return false
	}

	AckFinished(&Ack{TaskN: TaskN, Type: Reduce}, &Reply{})
	return true
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
