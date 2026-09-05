package gatewayruntime

import (
	"bytes"
	"context"
	"io"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// prefixedPeerConnection puts the discriminator bytes back in front of the
// protocol reader after the bounded control listener has selected a handler.
// It preserves the original authenticated connection and therefore its peer
// identity, key digest, traffic class, deadlines, and close semantics.
type prefixedPeerConnection struct {
	rafttransport.PeerConnection
	prefix []byte
}

func (connection *prefixedPeerConnection) Read(dst []byte) (int, error) {
	if connection == nil || connection.PeerConnection == nil {
		return 0, io.ErrClosedPipe
	}
	if len(connection.prefix) != 0 {
		count := copy(dst, connection.prefix)
		connection.prefix = connection.prefix[count:]
		return count, nil
	}
	return connection.PeerConnection.Read(dst)
}

func (runtime *Runtime) serveGatewayControlConnection(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if runtime == nil || ctx == nil || connection == nil {
		return errGatewayControlDirectory
	}
	if runtime.controlReadDeadline == nil {
		return errGatewayControlDirectory
	}
	if deadline := runtime.controlReadDeadline(); deadline.IsZero() {
		return errGatewayControlDirectory
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	var discriminator [8]byte
	if _, err := io.ReadFull(connection, discriminator[:]); err != nil {
		return err
	}
	wrapped := &prefixedPeerConnection{PeerConnection: connection, prefix: discriminator[:]}
	bootstrapDiscriminator := nodecontrol.BootstrapReadRequestDiscriminator()
	if bytes.Equal(discriminator[:], bootstrapDiscriminator[:]) {
		if runtime.bootstrapReadService == nil {
			return nodecontrol.ErrBootstrapReadUnavailable
		}
		return runtime.bootstrapReadService.Serve(ctx, wrapped)
	}
	if runtime.controlService == nil {
		return errGatewayControlDirectory
	}
	return runtime.controlService.Serve(ctx, wrapped)
}
