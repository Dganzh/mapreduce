package main

import (
	"fmt"
	"mapreduce/mapreduce"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	master := mapreduce.MakeMaster(os.Args[1:], 10)
	mapreduce.RunServer(master)
}

