package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func checkError(err error, msg string) {
	if err != nil {
		log.Println(err, msg)
		os.Exit(-1)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
