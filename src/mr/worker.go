package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "path/filepath"
import "encoding/json"
import "sort"


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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func RequestTask(workerId int) *GetTaskResponse {
	args := GetTaskRequest{} 
	args.WorkerId = workerId 

	reply := GetTaskResponse{}
	reply.TaskId = -1; 
	reply.Files = nil; 
	reply.TaskType = -1; 
	
	ok := call("Coordinator.GetTask", &args, &reply); 
	if ok {
		fmt.Fprintf(os.Stderr, "Received task")
		fmt.Println(reply.Files); 

	} else {
		fmt.Fprintf(os.Stderr, "task request call failed"); 
	}

	return &reply; 


}


func RegisterWorker() int {
	args := RegisterWorkerArgs{} 
	reply := RegisterWorkerReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	workerId := -1; 
	if ok {
		workerId = reply.WorkerId; 
	}

	return workerId; 

}

func submitTask(resultFiles []string, workerId int, taskId int, taskType int) int {
	args := SubmitTaskArgs{}
	args.TaskType = taskType
	args.WorkerId = workerId 
	args.Files = resultFiles 
	args.TaskId = taskId 

	reply := SubmitTaskReply{}

	ok := call("Coordinator.ReceiveTask", &args, &reply)
	if ok {
		fmt.Fprintf(os.Stderr, "Submitted task %v", taskId); 
	} else {
		fmt.Fprintf(os.Stderr, "error submitting task"); 
	}
	return reply.Success




}


func mapJob(inputFiles []string, workerId int, nReduce int, taskId int, mapf func(string, string) []KeyValue) []string {
	totalkva := []KeyValue{}
	
	OUT_FILE_DIRECTORY := "mr_int_files"

	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)

		}
		file.Close()
		kv_list := mapf(filename, string(content))
		//fmt.Fprintf(os.Stderr, "%+v\n", kv_list)
		totalkva = append(totalkva, kv_list...); 
		

	}



	// split totalkva into nReduce files 
	outFiles := []string{} 
	encoders := []*json.Encoder{}
	//fmt.Println(totalkva)
	cwd, _ := os.Getwd() 

	
	i := 0; 
	for i < nReduce {
		filename := fmt.Sprintf("mr-%d-%d-%d", workerId, taskId, i)
		path := filepath.Join(cwd, OUT_FILE_DIRECTORY, filename)
		actualPath := filepath.FromSlash(path)

		outFile, err := os.Create(actualPath);
		if err != nil {
			fmt.Println(err); 

		}
		enc := json.NewEncoder(outFile)
		encoders = append(encoders, enc)
		outFiles = append(outFiles, filename)
		fmt.Println(filename)

		i += 1
	}

	for _, kv := range totalkva {
		err := encoders[ihash(kv.Key) % nReduce].Encode(&kv)
		if err != nil {
			fmt.Println(err)
		}




	}

	return outFiles 



	
}

func reduceJob(reduceFiles []string, workerId int, nReduce int, taskId int, reducef func(string, []string) string) string {
	OUT_FILE_DIRECTORY := "mr_int_files"

	patternString := filepath.Join(OUT_FILE_DIRECTORY, fmt.Sprintf("mr-*-*-%d", taskId)); 
	my_reduce_files := []string{}

	// entries, err := os.ReadDir(root); 
	// if err != nil {
	// 	fmt.Println(err)
	// }

	for _, reduceFile := range reduceFiles {
		reduceFilepath := filepath.Join(OUT_FILE_DIRECTORY, reduceFile)

		is_match, _ := filepath.Match(patternString, reduceFilepath)


		if is_match {
			my_reduce_files = append(my_reduce_files, reduceFilepath)

		}
	}

	my_kvs := []KeyValue{}

	for _, filepath := range my_reduce_files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		fmt.Println(filepath)
		

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  fmt.Println(err)
			  break
			}
			my_kvs = append(my_kvs, kv)
		  }
	}

	sort.Sort(ByKey(my_kvs)) 
	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(my_kvs) {
		j := i + 1
		for j < len(my_kvs) && my_kvs[j].Key == my_kvs[i].Key {
			j++
		}
		current_values := []string{}

		for k := i; k < j; k++ {
			current_values = append(current_values, my_kvs[k].Value)

		}
		this_output := reducef(my_kvs[i].Key, current_values) // all instances of Key, each "value" shoudl just be one 

		fmt.Fprintf(ofile, "%v %v\n", my_kvs[i].Key, this_output)

		i = j 
	}
	ofile.Close() 
	return oname



}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	fmt.Fprintf(os.Stderr, "Hello!")
	workerId := RegisterWorker(); 
	if workerId < 0 {
		fmt.Fprintf(os.Stderr, "No tasks from coordinator"); 
		return; 

	}

	taskResponsePointer := RequestTask(workerId); 
	
	taskResponse := *taskResponsePointer 
	fmt.Println(taskResponse)

	for (taskResponse.TaskType == 1 || taskResponse.TaskType == 2) {
		if taskResponse.TaskType == 1 {
			// do map
			fmt.Println(taskResponse.Files)
			outputFiles := mapJob(taskResponse.Files, workerId, taskResponse.NReduce, taskResponse.TaskId, mapf); 
			fmt.Println(outputFiles)

			_ = submitTask(outputFiles, workerId, taskResponse.TaskId, 1); 

		} else if taskResponse.TaskType == 2 {
			// do reduce
			fmt.Println(taskResponse.Files)
			outputFile := reduceJob(taskResponse.Files, workerId, taskResponse.NReduce, taskResponse.TaskId, reducef); 
			fmt.Println("Wrote reduce output to %v", outputFile)
		}

		taskResponsePointer = RequestTask(workerId)
		taskResponse = *taskResponsePointer 
		fmt.Println(taskResponse.TaskId)


	}



	

	// uncomment to send the Example RPC to the coordinator.
	//workingfile := CallCoordinator()
	workingfile := ""
	if workingfile != "" {
		file, err := os.Open(workingfile)
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", workingfile)

		}
		file.Close()
		kv_list := mapf(workingfile, string(content))
		fmt.Fprintf(os.Stderr, "%+v\n", kv_list)

	}
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallCoordinator() string {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.WorkerId = 20


	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.SendTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		filename := reply.Filename 
		taskId := reply.TaskId
		//do some task 
		args.Finished = true 
		args.Filename = filename 

		ok := call("Coordinator.ReceiveTask", &args, &reply)
		if ok {
			fmt.Printf("Task id %d finished %v \n", taskId, filename)
			return filename 
		} else {
			fmt.Printf("coordinator response failed\n")
		}

	} else {
		fmt.Printf("call failed!\n")
	}
	return ""
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
