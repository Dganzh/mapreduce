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



