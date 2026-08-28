package clusterbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type liveTestConnection struct{ net.Conn }

func (liveTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return rafttransport.PeerIdentity{}
}
func (liveTestConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

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

func TestLiveClientStreamsExactBoundedArtifactWithDeadlines(t *testing.T) {
	payload := bytes.Repeat([]byte("artifact"), 8192)
	request := LiveRequest{Operation: filled32(1), Group: backupGroup(2), SourceMember: 3}
	cut := backupCut(2)
	cut.SourceMember = request.SourceMember
	cut.ArtifactBytes = uint64(len(payload))
	cut.ArtifactHash = sha256.Sum256(payload)
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		var raw [LiveRequestBytes]byte
		if _, err := io.ReadFull(server, raw[:]); err != nil {
			done <- err
			return
		}
		opened, err := OpenLiveRequest(raw[:])
		if err != nil || opened != request {
			done <- errors.Join(ErrLiveBackup, err)
			return
		}
		response := AppendLiveResponse(request.Operation, cut)
		if err = writeFull(server, response[:]); err == nil {
			err = writeFull(server, payload)
		}
		done <- err
	}()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	liveClient := LiveClient{Open: func(context.Context) (rafttransport.PeerConnection, error) {
		return liveTestConnection{client}, nil
	}, ReadDeadline: deadline, WriteDeadline: deadline}
	var output bytes.Buffer
	got, err := liveClient.Export(t.Context(), request, &output)
	if err != nil || got != cut || !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("cut=%+v bytes=%d err=%v", got, output.Len(), err)
	}
	if serverErr := <-done; serverErr != nil {
		t.Fatal(serverErr)
	}
}

func TestLiveClientPartitionFailsWithinConfiguredDeadline(t *testing.T) {
	request := LiveRequest{Operation: filled32(1), Group: backupGroup(2), SourceMember: 3}
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		var raw [LiveRequestBytes]byte
		_, _ = io.ReadFull(server, raw[:])
		var probe [1]byte
		_, _ = server.Read(probe[:])
	}()
	deadline := func() time.Time { return time.Now().Add(20 * time.Millisecond) }
	liveClient := LiveClient{Open: func(context.Context) (rafttransport.PeerConnection, error) {
		return liveTestConnection{client}, nil
	}, ReadDeadline: deadline, WriteDeadline: deadline}
	started := time.Now()
	if _, err := liveClient.Export(t.Context(), request, io.Discard); err == nil || time.Since(started) > time.Second {
		t.Fatalf("partition err=%v elapsed=%v", err, time.Since(started))
	}
	<-done
}
