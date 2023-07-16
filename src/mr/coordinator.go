package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io/ioutil"
import "plugin"
import "fmt"
import "sync"

// Shouldn't actually need these imports 
//import "encoding/json"
//import "path/filepath"




type Coordinator struct {
	// Your definitions here.
	taskId int 
	reduceTaskId int 
	reduceJobs int 
	mapJobs int 
	mapFiles []string 
	reduceFiles[]string
	activeWorkers[]int
	nReduce int 
	mu sync.Mutex 

	

}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	if (c.mapJobs > 0) {
		c.mu.Lock()
		workerId := len(c.activeWorkers)
		c.activeWorkers = append(c.activeWorkers, 0)
		reply.WorkerId = workerId
		fmt.Printf("Registering worker %d\n", reply.WorkerId); 
		c.mu.Unlock() 
	}
	return nil 
	

}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	fmt.Printf("Received task request from %d ", args.WorkerId)
	// Give a map job 

	if (c.mapJobs > 0) {
		c.mu.Lock()
		reply.Files = []string{}
		reply.Files = append(reply.Files, c.mapFiles[0])
		reply.TaskId = c.taskId
		reply.TaskType = 1
		reply.NReduce = c.nReduce 
		fmt.Println(reply)
		c.mapJobs -= 1 
		c.taskId += 1

		c.mapFiles = c.mapFiles[1:]
		fmt.Printf("...Giving task %s", reply.Files[0])
		c.mu.Unlock() 

	} else if (c.reduceJobs > 0) {
		c.mu.Lock()
		reply.Files = c.reduceFiles
		reply.TaskType = 2
		reply.TaskId = c.reduceTaskId 
		c.reduceTaskId += 1
		c.reduceJobs -= 1 
		c.mu.Unlock() 


	}
	return nil

}


func (c *Coordinator) ReceiveTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	if (args.TaskType == 1) {
		// finished map task 
		fmt.Fprintf(os.Stderr, "\n%v finished %d map task\n", args.WorkerId, args.TaskId); 
		c.mu.Lock()
		c.reduceFiles = append(c.reduceFiles, args.Files...)

		// code to test if json was successfully being transfered via files
		// cwd, _ := os.Getwd()
		// filename := args.Files[0] 
		// path := filepath.Join(cwd, OUT_FILE_DIRECTORY, filename)
		// actualPath := filepath.FromSlash(path)
		
		// file, _ := os.Open(actualPath)
		// dec := json.NewDecoder(file)
		// kva := []KeyValue{}
		// fmt.Println(actualPath)

		

		// for {
		// 	var kv KeyValue
		// 	if err := dec.Decode(&kv); err != nil {
		// 	  fmt.Println(err)
		// 	  break
		// 	}
		// 	kva = append(kva, kv)
		//   }
		// fmt.Println(kva)
		c.mu.Unlock()
		reply.Success = 1; 

	}
	return nil; 


}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }



//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	//fmt.Fprintf(os.Stderr, "Created file %v \n", sockname)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	


	return c.reduceJobs == 0 && c.mapJobs == 0 
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.mapJobs = len(files)
	c.reduceJobs = nReduce 
	c.mapFiles = files 
	c.reduceFiles = []string{}

	c.taskId = 1
	c.reduceTaskId = 1

	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator xxx.so inputfiles...\n")
		os.Exit(1)
	}
	//mapf, reducef := loadPlugin(os.Args[1])

	
	for _, filename := range files {
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)

		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)

		}
		file.Close()

		go func (fname string, file_contents []byte) {
			x := filename 
			y := content 
			if (y == nil || x == "") {
				os.Exit(1)
			}

		} (filename, content)


	}

	// Your code here.


	c.server()
	return &c
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}


