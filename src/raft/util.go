package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient   logTopic = "CLNT"
	dCommit   logTopic = "CMIT"
	dDrop     logTopic = "DROP"
	dError    logTopic = "ERRO"
	dInfo     logTopic = "INFO"
	dLeader   logTopic = "LEAD"
	dLog      logTopic = "LOG1"
	dLog2     logTopic = "LOG2"
	dPersist  logTopic = "PERS"
	dSnap     logTopic = "SNAP"
	dTerm     logTopic = "TERM"
	dTest     logTopic = "TEST"
	dTimer    logTopic = "TIMR"
	dTrace    logTopic = "TRCE"
	dVote     logTopic = "VOTE"
	dWarn     logTopic = "WARN"
	dPhase    logTopic = "PHAS"
	dElection logTopic = "ELEC"
	dClean    logTopic = "CLEA"
	dApply    logTopic = "APLY"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, who int, format string, a ...interface{}) {
	if topic == dDrop || topic == dClean {
		return
	}
	if debugVerbosity >= 1 {
		t := time.Since(debugStart).Microseconds()
		t /= 100
		prefix := fmt.Sprintf("%06d %v S%v ", t, string(topic), who)
		format = prefix + format
		log.Printf(format, a...)
	}
}
