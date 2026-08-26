// Package shardcontrol multiplexes the fixed binary services carried by one
// mutually authenticated shard-control listener.
package shardcontrol

import (
	"context"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var ErrMux = errors.New("shardcontrol: invalid service discriminator")

const DiscriminatorBytes = 8

// Handler owns one already-authenticated connection. Implementations consume
// their complete fixed grammar and close the connection when they return.
type Handler interface {
	Serve(context.Context, rafttransport.PeerConnection) error
}

type Route struct {
	Discriminator [DiscriminatorBytes]byte
	Handler       Handler
}

// Mux is immutable after construction and performs no per-request allocation.
// The caller's TLS listener remains the sole concurrency bound.
type Mux struct {
	routes  []Route
	traffic rafttransport.TrafficClass
}

func New(routes ...Route) (*Mux, error) {
	return NewForTraffic(rafttransport.TrafficShardControl, routes...)
}

// NewForTraffic constructs the same allocation-free discriminator mux for an
// exact authenticated traffic class. Snapshot transfer and split artifacts
// use this to share one snapshot listener without weakening ALPN isolation.
func NewForTraffic(traffic rafttransport.TrafficClass, routes ...Route) (*Mux, error) {
	if len(routes) == 0 || len(routes) > 16 {
		return nil, ErrMux
	}
	if traffic != rafttransport.TrafficShardControl && traffic != rafttransport.TrafficSnapshot {
		return nil, ErrMux
	}
	owned := make([]Route, len(routes))
	copy(owned, routes)
	for index, route := range owned {
		if route.Discriminator == ([DiscriminatorBytes]byte{}) || route.Handler == nil {
			return nil, ErrMux
		}
		for prior := range index {
			if owned[prior].Discriminator == route.Discriminator {
				return nil, ErrMux
			}
		}
	}
	return &Mux{routes: owned, traffic: traffic}, nil
}

func (mux *Mux) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if mux == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != mux.traffic {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrMux
	}
	var discriminator [DiscriminatorBytes]byte
	if _, err := io.ReadFull(connection, discriminator[:]); err != nil {
		_ = connection.Close()
		return errors.Join(ErrMux, err)
	}
	for _, route := range mux.routes {
		if route.Discriminator == discriminator {
			wrapped := &replayConnection{PeerConnection: connection, prefix: discriminator}
			return route.Handler.Serve(ctx, wrapped)
		}
	}
	_ = connection.Close()
	return ErrMux
}

// replayConnection returns the discriminator already consumed by Mux before
// reading the remaining bytes directly from the authenticated connection.
type replayConnection struct {
	rafttransport.PeerConnection
	prefix [DiscriminatorBytes]byte
	offset uint8
}

func (connection *replayConnection) Read(dst []byte) (int, error) {
	if connection.offset < DiscriminatorBytes && len(dst) != 0 {
		count := copy(dst, connection.prefix[connection.offset:])
		connection.offset += uint8(count)
		return count, nil
	}
	return connection.PeerConnection.Read(dst)
}
