package clusterbackup

import (
	"errors"
	"testing"
)

func TestLiveBackupRequestResponseCanonicalAndCorruptionClosed(t *testing.T) {
	request := LiveRequest{Operation: filled32(1), Group: backupGroup(2), SourceMember: 3}
	raw := AppendLiveRequest(request)
	opened, err := OpenLiveRequest(raw[:])
	if err != nil || opened != request {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	cut := backupCut(2)
	response := AppendLiveResponse(request.Operation, cut)
	openedCut, err := OpenLiveResponse(response[:], request.Operation)
	if err != nil || openedCut != cut {
		t.Fatalf("cut=%+v err=%v", openedCut, err)
	}
	for index := 0; index < len(raw); index += 17 {
		corrupt := raw
		corrupt[index] ^= 1
		if _, err = OpenLiveRequest(corrupt[:]); !errors.Is(err, ErrLiveBackup) {
			t.Fatalf("request corruption index=%d err=%v", index, err)
		}
	}
	for index := 0; index < len(response); index += 29 {
		corrupt := response
		corrupt[index] ^= 1
		if _, err = OpenLiveResponse(corrupt[:], request.Operation); !errors.Is(err, ErrLiveBackup) {
			t.Fatalf("response corruption index=%d err=%v", index, err)
		}
	}
	if _, err = OpenLiveRequest(append(raw[:], 0)); !errors.Is(err, ErrLiveBackup) {
		t.Fatalf("request trailing err=%v", err)
	}
	if _, err = OpenLiveResponse(response[:len(response)-1], request.Operation); !errors.Is(err, ErrLiveBackup) {
		t.Fatalf("response truncated err=%v", err)
	}
}
