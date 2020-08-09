package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Master struct {
	tasks []string
	mapNum int
	reduceNum int
	mapIdx int
	finishMapTasks []string
	reduceFinishNum int
	workerID int
	reduceID int
}

type Server struct {
	master *Master
}

// all finish
func (m *Master) Done() bool {
	return m.reduceNum == m.reduceFinishNum
}

// assign all Task
func (m *Master) AssignDone() bool {
	return m.reduceNum == m.reduceID + 1
}

func (m *Master) genWorkerID() (wid int){
	wid = m.workerID
	m.workerID++
	return
}

func (m *Master) getMapTask() string {
	if len(m.tasks) <= 0 || m.mapIdx >= len(m.tasks){
		return ""
	}
	task := m.tasks[m.mapIdx]
	return task
}

func (m *Master) genReduceID() (rid int) {
	rid = m.reduceID
	m.reduceID++
	return
}

func (m *Master) finishTask(task *TaskModel) {
	if task.Type == TypeMap {
		m.finishMapTasks = append(m.finishMapTasks, task.Args)
	} else if task.Type == TypeReduce {
		m.reduceFinishNum++
	}
}


func (m *Master) SaveState() {
	// 应该持久化,保持所有相关数据,以便进程重启能继续进行
}

func (s *Server) RegisterHandler(args *RegisterArgs, reply *RegisterReply) error {
	reply.WorkerID = s.master.genWorkerID()
	reply.ReduceNum = s.master.reduceNum
	reply.MapNum = s.master.mapNum
	return nil
}

func (s *Server) AssignTaskHandler(args *AssignTaskArgs, reply *AssignTaskReply) error {
	task := TaskModel{}
	if s.master.reduceNum == s.master.reduceID {
		task.Type = TypeDone
	} else if s.master.mapIdx < len(s.master.tasks) {		// map phase
		task.Type = TypeMap
		task.Args = s.master.getMapTask()
		task.MapIdx = s.master.mapIdx
		s.master.mapIdx++
	} else {					// reduce phase
		task.Type = TypeReduce
		task.Args = strconv.Itoa(s.master.genReduceID())
	}
	reply.Task = task
	return nil
}

func (s *Server) ReportHandler(args *ReportArgs, reply *ReportReply) error {
	switch args.State {
	case StateFinish:
		s.master.finishTask(&args.Task)
	case StateError:
		// log
		fmt.Printf("failed Task %d, %+v", args.WorkerID, args.Task)
	}
	return nil
}


// start a thread that listens for RPCs from worker.go
func (s *Server) server() {
	rpc.Register(s)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		tasks: files,
		mapNum: len(files),
		reduceNum: nReduce,
		reduceFinishNum: 0,
		workerID: 0,
		reduceID: 0,
	}
	return &m
}


func RunServer(m *Master) {
	fmt.Println("Start Server ... ")
	s := Server{master: m}
	s.server()
	for !s.master.Done() {
		time.Sleep(time.Second)
	}
}

