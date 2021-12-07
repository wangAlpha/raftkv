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

const OverTime = 720 * time.Millisecond

var ErrName = map[int]string{
	Ok:                "OK",
	ErrNoneLeader:     "ErrNoneLeader",
	ErrSessionExpired: "ErrSessionExpired",
	ErrDuplicateOp:    "ErrDuplicateOp",
	ErrTimeout:        "ErrDuplicateOp",
	ErrNoneKey:        "ErrNoneKey",
	ErrOperator:       "ErrOperator",
}

var OpName = map[Operator]string{
	OpGet:    "OpGet",
	OpPut:    "OpPut",
	OpAppend: "OpAppend",
}

var (
	// LogFile, _ = os.OpenFile("output.log", os.O_CREATE|os.O_WRONLY, 0666)
	LogFile = os.Stderr
	INFO    = log.New(LogFile, "INFO ", log.Ltime|log.Lshortfile).Printf
	WARN    = log.New(LogFile, "WARN ", log.Ltime|log.Lshortfile).Printf
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
