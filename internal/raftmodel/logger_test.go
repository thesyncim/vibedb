package raftmodel

import (
	"bytes"
	"log"
	"strings"
	"testing"

	raft "go.etcd.io/raft/v3"
)

func TestProductionRaftLoggerDropsInfoAndRetainsWarnings(t *testing.T) {
	var output bytes.Buffer
	logger := &productionRaftLogger{
		sink: &raft.DefaultLogger{Logger: log.New(&output, "", 0)},
	}

	logger.Debug("debug")
	logger.Infof("unstable acknowledgement %d", 7)
	if output.Len() != 0 {
		t.Fatalf("low-severity output = %q", output.String())
	}

	logger.Warningf("quorum unavailable for %d", 3)
	if got := output.String(); !strings.Contains(got, "WARN: quorum unavailable for 3") {
		t.Fatalf("warning output = %q", got)
	}
}
