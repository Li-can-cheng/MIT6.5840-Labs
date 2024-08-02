package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.5840/mr"
)

func main() {
	// a check for input args
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	tmpCoordinator := mr.MakeCoordinator(os.Args[1:], 10)
	for tmpCoordinator.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
	fmt.Println("Coordinator Done, exiting...")

}
