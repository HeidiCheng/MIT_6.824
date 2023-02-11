package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type WorkType int64

const (
	Map    WorkType = 0
	Reduce          = 1
	Exit            = 2
	Wait            = 3
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AssignedWork struct {
	TaskN    int
	Filename string
	NReduce  int
	Type     WorkType
}

type Ack struct {
	TaskN int
	Type  WorkType
}

type Request struct {
	Req bool
}

type Reply struct {
	Rep bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
