package main

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type rf3SplitStreamOpener struct {
	tls      *rafttransport.PeerTLS
	deadline rafttransport.DeadlineFunc
	dial     func(context.Context, string) (net.Conn, error)
	control  map[rafttransport.NodeID]string
	snapshot map[rafttransport.NodeID]string
	slots    chan struct{}
}

func newRF3SplitStreamOpener(
	tls *rafttransport.PeerTLS,
	deadline rafttransport.DeadlineFunc,
	dial func(context.Context, string) (net.Conn, error),
	catalog *gateway.Snapshot,
	plan *splitcontroller.Plan,
	maxConnections int,
) (*rf3SplitStreamOpener, error) {
	if tls == nil || deadline == nil || dial == nil || catalog == nil || plan == nil ||
		maxConnections <= 0 || maxConnections > 256 {
		return nil, errRF3Serving
	}
	result := &rf3SplitStreamOpener{
		tls: tls, deadline: deadline, dial: dial,
		control: make(map[rafttransport.NodeID]string), snapshot: make(map[rafttransport.NodeID]string),
		slots: make(chan struct{}, maxConnections),
	}
	for child := uint8(0); child < autosplit.MaxSplitChildren; child++ {
		target, ok := plan.Target(child)
		if !ok {
			continue
		}
		for _, replica := range target.Replicas {
			control, err := catalog.Address(replica.ControlEndpoint)
			if err != nil || !result.install(replica.Node, control, replica.SnapshotAddress) {
				return nil, errors.Join(errRF3Serving, err)
			}
		}
	}
	if len(result.control) == 0 || len(result.control) != len(result.snapshot) {
		return nil, errRF3Serving
	}
	return result, nil
}

func (opener *rf3SplitStreamOpener) install(
	node rafttransport.NodeID, control, snapshot string,
) bool {
	if node == (rafttransport.NodeID{}) || control == "" || snapshot == "" {
		return false
	}
	if current, ok := opener.control[node]; ok && current != control {
		return false
	}
	if current, ok := opener.snapshot[node]; ok && current != snapshot {
		return false
	}
	for currentNode, address := range opener.snapshot {
		if currentNode != node && address == snapshot {
			return false
		}
	}
	opener.control[node], opener.snapshot[node] = control, snapshot
	return true
}

func (opener *rf3SplitStreamOpener) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return opener.open(ctx, node, rafttransport.TrafficShardControl, opener.control)
}

func (opener *rf3SplitStreamOpener) OpenSnapshot(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return opener.open(ctx, node, rafttransport.TrafficSnapshot, opener.snapshot)
}

func (opener *rf3SplitStreamOpener) open(
	ctx context.Context, node rafttransport.NodeID, class rafttransport.TrafficClass,
	directory map[rafttransport.NodeID]string,
) (rafttransport.PeerConnection, error) {
	if opener == nil || ctx == nil {
		return nil, errRF3Serving
	}
	address, ok := directory[node]
	if !ok {
		return nil, errRF3Serving
	}
	select {
	case opener.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	raw, err := opener.dial(ctx, address)
	if err != nil || raw == nil {
		<-opener.slots
		if raw != nil {
			_ = raw.Close()
		}
		return nil, errors.Join(errRF3Serving, err)
	}
	connection, err := opener.tls.Client(ctx, raw, node, class, opener.deadline)
	if err != nil {
		<-opener.slots
		return nil, err
	}
	return &rf3BoundedSplitConnection{PeerConnection: connection, release: func() { <-opener.slots }}, nil
}

type rf3BoundedSplitConnection struct {
	rafttransport.PeerConnection
	once    sync.Once
	release func()
}

func (connection *rf3BoundedSplitConnection) Close() error {
	if connection == nil {
		return nil
	}
	err := connection.PeerConnection.Close()
	connection.once.Do(connection.release)
	return err
}

var _ splitcontroller.PlanObservationStreamOpener = (*rf3SplitStreamOpener)(nil)
var _ rafttransport.SnapshotStreamOpener = (*rf3SplitStreamOpener)(nil)
