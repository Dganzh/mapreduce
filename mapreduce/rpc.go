package mapreduce

import (
	"os"
	"strconv"
)


func masterSock() string {
	s := "/var/tmp/dganzh-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type TaskType int
type TaskState int

const (
	TypeMap TaskType = iota
	TypeReduce
	TypeDone
)

const (
	StateFinish TaskState = iota
	StateError
)

type TaskModel struct {
	Type TaskType
	Args string
	MapIdx int
}



type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerID  int
	ReduceNum int
	MapNum    int
}


type AssignTaskArgs struct {
	WorkerID int
}

type AssignTaskReply struct {
	Task TaskModel
}


type ReportArgs struct {
	WorkerID int
	Task     TaskModel
	State    TaskState
}

type ReportReply struct {

}

