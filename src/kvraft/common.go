package kvraft

import (
	"log"
	"os"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Operator int

const (
	Ok = iota
	ErrNoneLeader
	ErrSessionExpired
	ErrDuplicateOp
	ErrTimeout
	ErrOperator
)

const OverTime = 200 * time.Millisecond

var ErrName = map[int]string{
	Ok:                "OK",
	ErrNoneLeader:     "ErrNoneLeader",
	ErrSessionExpired: "ErrSessionExpired",
	ErrDuplicateOp:    "ErrDuplicateOp",
	ErrTimeout:        "ErrDuplicateOp",
	ErrOperator:       "ErrOperator",
}

var (
	INFO = log.New(os.Stderr, "INFO:", log.Ltime|log.Lshortfile).Printf
	WARN = log.New(os.Stderr, "WARN:", log.Ltime|log.Lshortfile).Printf
)

// Put or Append
type CommandArgs struct {
	ClientId  int64
	LeaderId  int
	CommnadId int
	Request   Command
}

type CommandReply struct {
	LeaderId   int
	Value      string
	StatusCode int
}
