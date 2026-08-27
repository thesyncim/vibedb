package clusterbackup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	LiveRequestBytes  = 160
	LiveResponseBytes = 320
)

var (
	ErrLiveBackup = errors.New("clusterbackup: live backup exchange")
	liveRequest   = [8]byte{'V', 'B', 'B', 'A', 'C', 'K', 'U', 'P'}
	liveResponse  = [8]byte{'V', 'B', 'B', 'A', 'C', 'K', 'O', 'K'}
)

func LiveRequestDiscriminator() [8]byte { return liveRequest }

type LiveRequest struct {
	Operation    [sha256.Size]byte
	Group        raftmember.GroupKey
	SourceMember uint64
}

func (request LiveRequest) Valid() bool {
	return request.Operation != ([sha256.Size]byte{}) && validGroup(request.Group) && request.SourceMember != 0
}

func AppendLiveRequest(request LiveRequest) (raw [LiveRequestBytes]byte) {
	copy(raw[:8], liveRequest[:])
	copy(raw[8:40], request.Operation[:])
	appendGroup(raw[40:40], request.Group)
	binary.BigEndian.PutUint64(raw[112:120], request.SourceMember)
	digest := sha256.Sum256(raw[:128])
	copy(raw[128:], digest[:])
	return raw
}

func OpenLiveRequest(raw []byte) (LiveRequest, error) {
	if len(raw) != LiveRequestBytes || [8]byte(raw[:8]) != liveRequest ||
		sha256.Sum256(raw[:128]) != [sha256.Size]byte(raw[128:]) {
		return LiveRequest{}, ErrLiveBackup
	}
	request := LiveRequest{Group: openLiveGroup(raw[40:112]), SourceMember: binary.BigEndian.Uint64(raw[112:120])}
	copy(request.Operation[:], raw[8:40])
	if !request.Valid() || binary.BigEndian.Uint64(raw[120:128]) != 0 {
		return LiveRequest{}, ErrLiveBackup
	}
	return request, nil
}

func openLiveGroup(raw []byte) (group raftmember.GroupKey) {
	copy(group.ClusterID[:], raw[:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return group
}

func AppendLiveResponse(operation [sha256.Size]byte, cut GroupCut) (raw [LiveResponseBytes]byte) {
	copy(raw[:8], liveResponse[:])
	copy(raw[8:40], operation[:])
	appendGroupCut(raw[40:288], cut)
	digest := sha256.Sum256(raw[:288])
	copy(raw[288:], digest[:])
	return raw
}

func OpenLiveResponse(raw []byte, operation [sha256.Size]byte) (GroupCut, error) {
	if len(raw) != LiveResponseBytes || [8]byte(raw[:8]) != liveResponse ||
		[sha256.Size]byte(raw[8:40]) != operation ||
		sha256.Sum256(raw[:288]) != [sha256.Size]byte(raw[288:]) {
		return GroupCut{}, ErrLiveBackup
	}
	cut := openGroupCut(raw[40:288])
	if !cut.Valid() {
		return GroupCut{}, ErrLiveBackup
	}
	return cut, nil
}

type LiveClient struct {
	Open                        func(context.Context) (rafttransport.PeerConnection, error)
	ReadDeadline, WriteDeadline rafttransport.DeadlineFunc
}

func (client LiveClient) Export(ctx context.Context, request LiveRequest, destination io.Writer) (GroupCut, error) {
	if ctx == nil || !request.Valid() || destination == nil || client.Open == nil ||
		client.ReadDeadline == nil || client.WriteDeadline == nil {
		return GroupCut{}, ErrLiveBackup
	}
	connection, err := client.Open(ctx)
	if err != nil || connection == nil {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	defer connection.Close()
	if err = connection.SetWriteDeadline(client.WriteDeadline()); err != nil {
		return GroupCut{}, err
	}
	raw := AppendLiveRequest(request)
	if err = writeFull(connection, raw[:]); err != nil {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	if err = connection.SetReadDeadline(client.ReadDeadline()); err != nil {
		return GroupCut{}, err
	}
	var response [LiveResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	cut, err := OpenLiveResponse(response[:], request.Operation)
	if err != nil || cut.Group != request.Group || cut.SourceMember != request.SourceMember {
		return GroupCut{}, ErrLiveBackup
	}
	digest := sha256.New()
	var buffer [32 << 10]byte
	remaining := cut.ArtifactBytes
	for remaining != 0 {
		if cause := context.Cause(ctx); cause != nil {
			return GroupCut{}, cause
		}
		if err = connection.SetReadDeadline(client.ReadDeadline()); err != nil {
			return GroupCut{}, err
		}
		chunk := buffer[:min(uint64(len(buffer)), remaining)]
		n, readErr := io.ReadFull(connection, chunk)
		if n > 0 {
			if writeErr := writeFull(destination, chunk[:n]); writeErr != nil {
				return GroupCut{}, writeErr
			}
			_, _ = digest.Write(chunk[:n])
			remaining -= uint64(n)
		}
		if readErr != nil {
			return GroupCut{}, errors.Join(ErrLiveBackup, readErr)
		}
	}
	var got [sha256.Size]byte
	copy(got[:], digest.Sum(nil))
	if got != cut.ArtifactHash {
		return GroupCut{}, ErrLiveBackup
	}
	var trailing [1]byte
	if err = connection.SetReadDeadline(client.ReadDeadline()); err != nil {
		return GroupCut{}, err
	}
	n, trailingErr := connection.Read(trailing[:])
	if n != 0 || !errors.Is(trailingErr, io.EOF) {
		return GroupCut{}, errors.Join(ErrLiveBackup, trailingErr)
	}
	return cut, nil
}

func writeFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := writer.Write(raw)
		if n > 0 {
			raw = raw[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
