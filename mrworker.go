package main

import (
	"fmt"
	"mapreduce/mapreduce"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	worker := mapreduce.MakeWorkder(os.Args[1])
	worker.Start()
}


