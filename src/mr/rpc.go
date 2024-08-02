package mr

// rpc.go

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

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

type TaskRequestArgs struct{}

type TaskRequestReply struct {
	Task Task
}

type TaskCompleteArgs struct {
	TaskID            int
	IntermediateFiles []string // 新增字段
}

type TaskCompleteReply struct{}

// Cook up a unique-ish UNIX-domainket name soc
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
