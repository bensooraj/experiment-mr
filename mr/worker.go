package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Unique name to a worker
var WorkerName string
var WorkerID int32
var NReduce int

const (
	outputDir = "output"
)

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
// worker/worker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Get a worker name/id
	WorkerID, NReduce = CallAssignWorkerID()
	WorkerName = fmt.Sprintf("worker_%d", WorkerID)

	for {
		// Get a task
		task := CallAllotTask()

		if task.IsTaskQueued && task.TaskType == "map" {
			intermediateKV := []KeyValue{}
			intermediateKVMap := make(map[int][]KeyValue, NReduce)
			intermediateFilesMap := make(map[int]string, NReduce)

			file, err := os.Open(task.FilePath)
			if err != nil {
				log.Fatalf("cannot open %v", task.FilePath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.FilePath)
			}
			file.Close()
			intermediateKV = mapf(task.FilePath, string(content))
			log.Printf("[%s][map][%d] Reading file %s\n", WorkerName, task.MapID, task.FilePath)

			// Initialise all the maps
			for i := 0; i < NReduce; i++ {
				intermediateKVMap[i] = []KeyValue{}
				intermediateFilesMap[i] = fmt.Sprintf("%s/mr-intermediate-%d-%d.json", outputDir, task.MapID, i)
			}

			// Bucketise the key-value pairs according to the number of reduce tasks
			for _, kv := range intermediateKV {
				reduceBucketIndex := ihash(kv.Key) % NReduce
				intermediateKVMap[reduceBucketIndex] = append(intermediateKVMap[reduceBucketIndex], kv)
			}

			// Write to the reduce files
			for i := 0; i < NReduce; i++ {

				oFileName := fmt.Sprintf("%s", intermediateFilesMap[i])
				oFile, err := os.OpenFile(oFileName, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Println("Error opening the intermediate file", oFileName)
				}
				jsonEnc := json.NewEncoder(oFile)
				jsonEnc.Encode(intermediateKVMap[i])
				log.Printf("[%s][map][%d] Writing the intermediate file %s\n", WorkerName, task.MapID, oFileName)
				oFile.Close()
			}

			CallUpdateTaskStatus(UpdateTaskStatusArgs{
				TaskType:    "map",
				MapID:       task.MapID,
				MapFilePath: task.FilePath,
				ReduceTasks: intermediateFilesMap,
				State:       COMPLETE,
			})
			time.Sleep(1 * time.Second)
			// break
		} else if task.IsTaskQueued && task.TaskType == "reduce" {

			var kva []KeyValue

			for _, fileName := range task.ReduceFilePaths {
				var kv []KeyValue

				log.Printf("[%s][reduce][%d] Reading file %s\n", WorkerName, task.ReduceID, fileName)

				jsonByte, err := ioutil.ReadFile(fileName)
				if err != nil {
					log.Println("Error reading the intermediate json file", fileName)
					continue
				}
				err = json.Unmarshal(jsonByte, &kv)
				if err != nil {
					log.Println("Error unmarshalling the json file", fileName)
					continue
				}

				kva = append(kva, kv...)
			}
			sort.Sort(ByKey(kva))

			outputFileName := fmt.Sprintf("%s/mr-out-%d", outputDir, task.ReduceID)
			outputFile, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Println("Error opening output the file", outputFileName)
				continue
			}

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			log.Printf("[%s][reduce][%d] Writing the final output file %s\n", WorkerName, task.ReduceID, outputFileName)

			// Close all the reduce output files
			outputFile.Close()

			CallUpdateTaskStatus(UpdateTaskStatusArgs{
				TaskType: "reduce",
				ReduceID: task.ReduceID,
				State:    COMPLETE,
			})

			time.Sleep(1 * time.Second)
		} else {
			log.Println("No tasks received!")
			time.Sleep(2 * time.Second)
			// break
		}
	}
}

//
func CallAssignWorkerID() (int32, int) {

	// declare an argument structure.
	args := AssignWorkerIDArgs{}

	// declare a reply structure.
	reply := AssignWorkerIDReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.AssignWorkerID", &args, &reply)

	log.Println("Worker ID #", reply.WorkerID)

	return reply.WorkerID, reply.NReduce
}

func CallAllotTask() AllotTaskReply {

	// declare an argument structure.
	args := AllotTaskArgs{WorkerName: WorkerName}

	// declare a reply structure.
	reply := AllotTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.AllotTask", &args, &reply)

	return reply
}

func CallUpdateTaskStatus(args UpdateTaskStatusArgs) {

	// declare a reply structure.
	reply := UpdateTaskStatuskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.UpdateTaskStatus", &args, &reply)
	return
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
