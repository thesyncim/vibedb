package rafttransport

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// outboundFailure adds only the routing metadata needed to correlate a
// synchronous send/queue refusal with the owning Raft group. The message
// payload, authority grant, and service-key material are deliberately absent.
// Backpressure and transport shutdown remain their exact sentinel values:
// owner loops use those identities to preserve their bounded retry behavior.
func outboundFailure(outbound raftmember.OutboundMessage, err error) error {
	if err == nil || errors.Is(err, ErrBackpressure) || errors.Is(err, ErrTransportClosed) {
		return err
	}
	kind := "ordinary"
	messageType := int32(0)
	var index, term uint64
	if outbound.Message != nil {
		messageType = int32(outbound.Message.GetType())
		index, term = outbound.Message.GetIndex(), outbound.Message.GetTerm()
	} else if outbound.Authority != nil {
		kind = "authority"
	}
	return fmt.Errorf("rafttransport: outbound send failed group=%x from=%d to=%d kind=%s message_type=%d index=%d term=%d: %w",
		outbound.Group.GroupID, outbound.From, outbound.To, kind, messageType, index, term, err)
}
