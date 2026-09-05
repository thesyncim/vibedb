package raftservice

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// deferredReadIngress retains admitted reads whose group has an uncaptured
// Ready. A parked read owns its original context and ingress charge, but not
// the owner lane. Other reads, proposals, controls and peer traffic may proceed:
// these requests overlap the parked read, which revalidates its full serving
// fence on every admission attempt. No quorum or applied-floor proof is reused.
//
// Capacity is covered by MaxIngressItems; a retained request does not return
// its ingress charge until it is admitted, refused, or settled at shutdown.
// The scratch map contains only groups represented in this bounded queue.
type deferredReadIngress struct {
	requests []deferredReadRequest
	blocked  map[raftmember.GroupKey]struct{}
}

func newDeferredReadIngress(capacity int) deferredReadIngress {
	return deferredReadIngress{
		requests: make([]deferredReadRequest, 0, capacity),
		blocked:  make(map[raftmember.GroupKey]struct{}),
	}
}

// Park only read admission state, not the much larger ownerRequest envelope
// carrying schema installation, runtime replacement and other control values.
type deferredReadRequest struct {
	kind  requestKind
	group raftmember.GroupKey
	read  readRequest
	reply chan ownerReply
	bytes int64
	async bool
}

func (request deferredReadRequest) ingress() ownerRequest {
	return ownerRequest{kind: request.kind, group: request.group, read: request.read,
		reply: request.reply, bytes: request.bytes, async: request.async}
}

func (queue *deferredReadIngress) retain(request ownerRequest) {
	queue.requests = append(queue.requests, deferredReadRequest{
		kind: request.kind, group: request.group, read: request.read,
		reply: request.reply, bytes: request.bytes, async: request.async,
	})
}

// retry is called only after new ingress, Host progress, a local completion,
// or a logical pulse. At most one refused read per group reaches Host during
// this pass. A busy group cannot prevent another group's barrier admission.
func (queue *deferredReadIngress) retry(handle func(ownerRequest) error) (bool, error) {
	clear(queue.blocked)
	original := queue.requests
	queue.requests = original[:0]
	progressed := false
	for index, request := range original {
		if _, blocked := queue.blocked[request.group]; blocked {
			queue.requests = append(queue.requests, request)
			continue
		}
		err := handle(request.ingress())
		if errors.Is(err, errOwnerReadDeferred) {
			queue.blocked[request.group] = struct{}{}
			queue.requests = append(queue.requests, request)
			continue
		}
		progressed = true
		if err != nil {
			// handle settled and released this request. Keep every later request
			// owned so Run's deferred cleanup can settle them exactly once.
			queue.requests = append(queue.requests, original[index+1:]...)
			clear(original[len(queue.requests):])
			return progressed, err
		}
	}
	clear(original[len(queue.requests):])
	return progressed, nil
}
