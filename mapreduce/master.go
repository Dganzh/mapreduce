package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	Done bool
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
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

// 防止没有注册rpc handler引发报错
func (m *Master) Dummy(args *interface{}, reply *interface{}) error {
	return nil
}


func RunServer() {
	m := Master{}
	fmt.Println("Start Server ... ")
	m.server()
	for !m.Done {
		fmt.Println("Master State: ", m.Done)
		time.Sleep(time.Second)
	}
}

