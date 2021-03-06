package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)

type State int

const (
	QUEUED State = iota
	IN_PROGRESS
	COMPLETE
	FAILED
)

type AssignWorkerIDArgs struct {
}

type AssignWorkerIDReply struct {
	WorkerID int32
	NReduce  int
}

type AllotTaskArgs struct {
	WorkerName string
}

type AllotTaskReply struct {
	TaskType string

	MapID    int
	FileName string
	FilePath string

	ReduceID        int
	ReduceFilePaths []string

	IsTaskQueued bool
}

type UpdateTaskStatusArgs struct {
	TaskType string
	FileName string

	MapID       int
	MapFilePath string

	ReduceID    int
	ReduceTasks map[int]string
	State
}

type UpdateTaskStatuskReply struct {
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
