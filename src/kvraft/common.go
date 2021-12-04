package kvraft

import (
	"log"
	"os"
	"time"
)

type Operator int

const (
	OpGet = iota
	OpPut
	OpAppend
)

const (
	Ok = iota
	ErrNoneLeader
	ErrSessionExpired
	ErrDuplicateOp
	ErrTimeout
	ErrNoneKey
	ErrOperator
)

const OverTime = 240 * time.Millisecond

var ErrName = map[int]string{
	Ok:                "OK",
	ErrNoneLeader:     "ErrNoneLeader",
	ErrSessionExpired: "ErrSessionExpired",
	ErrDuplicateOp:    "ErrDuplicateOp",
	ErrTimeout:        "ErrDuplicateOp",
	ErrNoneKey:        "ErrNoneKey",
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
	CommandId int
	RequestOp Command
}

type CommandReply struct {
	ClientId   int64
	CommnadId  int
	LeaderId   int
	StatusCode int
	Value      string
}
