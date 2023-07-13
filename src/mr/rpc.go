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
	Finished bool 
	WorkerId int 
	Filename string
}

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerId int 
}

type ExampleReply struct {
	TaskId int
	Filename string 
}

type GetTaskRequest struct {
	WorkerId int 
	

}

// get a task from coordinator 
// sent by worker 
type GetTaskResponse struct {

	TaskId int 
	Files []string
	TaskType int // 1 for map, 2 for reduce, 3 for no task remaining
	NReduce int 

}

// have the coordinator send a task
// to the worker 
type SendTask struct {


}

// submit a completed task to the coordinator 

type SubmitTaskArgs struct {
	TaskId int 
	Files []string 
	TaskType int 
	WorkerId int


}

type SubmitTaskReply struct {
	Success int 
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}


