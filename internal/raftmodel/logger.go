package raftmodel

import (
	"log"
	"os"

	raft "go.etcd.io/raft/v3"
)

// productionRaftLogger retains actionable warnings and failures without
// formatting the upstream Raft core's per-Ready informational messages.
// Async storage acknowledgements can legitimately arrive after an unstable
// entry was advanced; upstream reports every such acknowledgement at Info.
// Sustained writes produce thousands of those messages across a multi-group
// node, and formatting and draining them competes with heartbeats and commits.
type productionRaftLogger struct {
	sink *raft.DefaultLogger
}

var raftLogger raft.Logger = &productionRaftLogger{
	sink: &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)},
}

func (*productionRaftLogger) Debug(...interface{})          {}
func (*productionRaftLogger) Debugf(string, ...interface{}) {}
func (*productionRaftLogger) Info(...interface{})           {}
func (*productionRaftLogger) Infof(string, ...interface{})  {}

func (logger *productionRaftLogger) Error(values ...interface{}) {
	logger.sink.Error(values...)
}

func (logger *productionRaftLogger) Errorf(format string, values ...interface{}) {
	logger.sink.Errorf(format, values...)
}

func (logger *productionRaftLogger) Warning(values ...interface{}) {
	logger.sink.Warning(values...)
}

func (logger *productionRaftLogger) Warningf(format string, values ...interface{}) {
	logger.sink.Warningf(format, values...)
}

func (logger *productionRaftLogger) Fatal(values ...interface{}) {
	logger.sink.Fatal(values...)
}

func (logger *productionRaftLogger) Fatalf(format string, values ...interface{}) {
	logger.sink.Fatalf(format, values...)
}

func (logger *productionRaftLogger) Panic(values ...interface{}) {
	logger.sink.Panic(values...)
}

func (logger *productionRaftLogger) Panicf(format string, values ...interface{}) {
	logger.sink.Panicf(format, values...)
}
