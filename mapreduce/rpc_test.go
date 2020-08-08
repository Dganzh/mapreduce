package mapreduce

import (
	"fmt"
	"testing"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	m.Done = true
	return nil
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func TestCallExample(t *testing.T) {
	CallExample()
}


