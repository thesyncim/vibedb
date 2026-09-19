package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	errRF3FrontendDrainPreparedAckReaderUnavailable = errors.New("vibedb-shard: prepared drain source reader unavailable")
	errRF3FrontendDrainPreparedAckReaderAuth        = errors.New("vibedb-shard: prepared drain source reader authentication failed")
	errRF3FrontendDrainPreparedAckReaderWire        = errors.New("vibedb-shard: invalid prepared drain source reader wire")
	errRF3FrontendDrainPreparedAckReaderState       = errors.New("vibedb-shard: prepared drain source reader state mismatch")
)

// The source service checks the receiver's exact physical identity, key and
// lifecycle against its complete committed catalog cut before returning bytes.
// A local Raft transport registry only contains peers of hosted groups: using
// it here would prevent a storage node in another group from obtaining the
// service directory it needs to start. This first check authenticates the
// transport; it grants no authority independently of that catalog check.
func rf3CanonicalSourcePeerAuthorizer(profile *rafttransport.PeerTLS) func(rafttransport.PeerConnection) bool {
	return func(connection rafttransport.PeerConnection) bool {
		return profile != nil && connection != nil &&
			connection.TrafficClass() == rafttransport.TrafficShardControl &&
			connection.PeerIdentity().TrustDomain == profile.LocalIdentity().TrustDomain &&
			connection.PeerIdentity().Node != (rafttransport.NodeID{}) &&
			connection.PeerKeyDigest() != ([32]byte{})
	}
}

// rf3FrontendDrainPreparedAckCutReader is the physical storage reader for a
// prepared frontend drain.  It opens a fresh gateway-control stream for every
// request through the exact manifest seed set.  The source gateway supplies
// the committed cut from its replicated authority; no source-cut bytes are
// accepted from the ACK caller.
type rf3FrontendDrainPreparedAckCutReader struct {
	mu                       sync.RWMutex
	profile                  *rafttransport.PeerTLS
	transport                *servicetls.Client
	seeds                    map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed
	canonicalSourceTransport *servicetls.Client
	canonicalSourceSeeds     map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed
	sourceRosterPath         string
	sourceFloor              frontenddrain.PreparedAckCutReadFloor
	trust                    rafttransport.TrustDomain
	localNode                rafttransport.NodeID
	localIncarnation         uint64
	localServiceKey          [sha256.Size]byte
	readDeadline             rafttransport.DeadlineFunc
	writeDeadline            rafttransport.DeadlineFunc
}

func newRF3FrontendDrainPreparedAckCutReader(
	profile *rafttransport.PeerTLS,
	seeds []nodecontrol.BootstrapGatewaySeed,
	localIncarnation uint64,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) (*rf3FrontendDrainPreparedAckCutReader, *servicetls.Client, error) {
	return newRF3FrontendDrainPreparedAckCutReaderWithSources(
		profile, seeds, nil, localIncarnation, readDeadline, writeDeadline,
	)
}

// newRF3FrontendDrainPreparedAckCutReaderWithSources keeps gateway-control
// drain requests and the pre-open physical source route on separate pinned
// transports. A storage certificate must never be used as a gateway source
// identity, and a gateway seed cannot be silently repurposed as a shard peer.
func newRF3FrontendDrainPreparedAckCutReaderWithSources(
	profile *rafttransport.PeerTLS,
	gatewaySeeds, canonicalSourceSeeds []nodecontrol.BootstrapGatewaySeed,
	localIncarnation uint64,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
	rosterPath ...string,
) (*rf3FrontendDrainPreparedAckCutReader, *servicetls.Client, error) {
	if profile == nil || readDeadline == nil || writeDeadline == nil {
		return nil, nil, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	local := profile.LocalIdentity()
	if local.Node == (rafttransport.NodeID{}) || local.TrustDomain.ClusterID == ([16]byte{}) ||
		local.TrustDomain.ClusterIncarnation == ([16]byte{}) || profile.LocalServiceKeyDigest() == ([sha256.Size]byte{}) {
		return nil, nil, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	reader := &rf3FrontendDrainPreparedAckCutReader{
		profile: profile,
		trust:   local.TrustDomain, localNode: local.Node, localIncarnation: localIncarnation,
		localServiceKey: profile.LocalServiceKeyDigest(), readDeadline: readDeadline,
		writeDeadline:        writeDeadline,
		seeds:                make(map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed, len(gatewaySeeds)),
		canonicalSourceSeeds: make(map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed, len(canonicalSourceSeeds)),
	}
	if len(rosterPath) != 0 {
		reader.sourceRosterPath = rosterPath[0]
	}
	// A missing seed set leaves the route installed but fail-closed.  This is
	// useful while a cold process is waiting for its manifest-backed gateway
	// roster; it never creates an unauthenticated fallback path.
	if len(gatewaySeeds) > nodecontrol.MaxBootstrapGatewaySeeds ||
		len(canonicalSourceSeeds) > nodecontrol.MaxBootstrapGatewaySeeds {
		return nil, nil, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	gatewayEndpoints, err := rf3FrontendDrainPreparedAckSeedEndpoints(
		gatewaySeeds, reader.seeds,
	)
	if err != nil {
		return nil, nil, err
	}
	canonicalSourceEndpoints, err := rf3FrontendDrainPreparedAckSeedEndpoints(
		canonicalSourceSeeds, reader.canonicalSourceSeeds,
	)
	if err != nil {
		return nil, nil, err
	}
	var transport *servicetls.Client
	if len(gatewayEndpoints) != 0 {
		transport, err = servicetls.NewClient(servicetls.ClientOptions{
			TLS: profile.WithLocalGatewayControlConnections(), Class: rafttransport.TrafficGatewayControl,
			Endpoints: gatewayEndpoints, Dial: func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
			}, HandshakeDeadline: writeDeadline, MaxConnections: len(gatewayEndpoints), MaxHandshakes: len(gatewayEndpoints),
		})
		if err != nil {
			return nil, nil, errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, err)
		}
		reader.transport = transport
	}
	if len(canonicalSourceEndpoints) != 0 {
		canonicalSourceTransport, sourceErr := newRF3CanonicalSourceTransport(profile, canonicalSourceEndpoints, writeDeadline)
		if sourceErr != nil {
			if transport != nil {
				_ = transport.Close()
			}
			return nil, nil, errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, sourceErr)
		}
		reader.canonicalSourceTransport = canonicalSourceTransport
	}
	if reader.sourceRosterPath != "" {
		persisted, loadErr := loadRF3CanonicalSourceRoster(reader.sourceRosterPath)
		if loadErr != nil && !errors.Is(loadErr, os.ErrNotExist) {
			if transport != nil {
				_ = transport.Close()
			}
			if reader.canonicalSourceTransport != nil {
				_ = reader.canonicalSourceTransport.Close()
			}
			return nil, nil, errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, loadErr)
		}
		if persisted != nil {
			for _, seed := range persisted.Seeds {
				node := seed.NodeID
				if prior, found := reader.canonicalSourceSeeds[node]; found && prior != seed {
					if transport != nil {
						_ = transport.Close()
					}
					if reader.canonicalSourceTransport != nil {
						_ = reader.canonicalSourceTransport.Close()
					}
					return nil, nil, errRF3FrontendDrainPreparedAckReaderUnavailable
				}
				reader.canonicalSourceSeeds[node] = seed
			}
			reader.sourceFloor = persisted.Floor
			endpoints := rf3CanonicalSourceSeedEndpoints(reader.canonicalSourceSeeds)
			if len(endpoints) != 0 {
				if reader.canonicalSourceTransport != nil {
					_ = reader.canonicalSourceTransport.Close()
				}
				reader.canonicalSourceTransport, err = newRF3CanonicalSourceTransport(profile, endpoints, writeDeadline)
				if err != nil {
					if transport != nil {
						_ = transport.Close()
					}
					return nil, nil, errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, err)
				}
			}
		}
	}
	return reader, transport, nil
}

func newRF3CanonicalSourceTransport(
	profile *rafttransport.PeerTLS, endpoints []servicetls.Endpoint, deadline rafttransport.DeadlineFunc,
) (*servicetls.Client, error) {
	if profile == nil || len(endpoints) == 0 || deadline == nil {
		return nil, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	return servicetls.NewClient(servicetls.ClientOptions{
		TLS: profile.WithLocalServiceConnections(), Class: rafttransport.TrafficShardControl,
		Endpoints: endpoints, Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: deadline, MaxConnections: len(endpoints), MaxHandshakes: len(endpoints),
	})
}

func rf3CanonicalSourceSeedEndpoints(
	seeds map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed,
) []servicetls.Endpoint {
	keys := make([]rafttransport.NodeID, 0, len(seeds))
	for node := range seeds {
		keys = append(keys, node)
	}
	slices.SortFunc(keys, func(left, right rafttransport.NodeID) int { return bytes.Compare(left[:], right[:]) })
	endpoints := make([]servicetls.Endpoint, 0, len(keys))
	for _, node := range keys {
		seed := seeds[node]
		if seed.Valid() {
			endpoints = append(endpoints, servicetls.Endpoint{Address: seed.ControlAddress, Node: seed.NodeID})
		}
	}
	return endpoints
}

func cloneRF3CanonicalSourceSeeds(
	seeds map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed,
) map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed {
	copySeeds := make(map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed, len(seeds))
	for node, seed := range seeds {
		copySeeds[node] = seed
	}
	return copySeeds
}

const (
	rf3CanonicalSourceRosterFormat   = 1
	rf3CanonicalSourceRosterMaxBytes = 128 << 10
)

var errRF3CanonicalSourceRoster = errors.New("vibedb-shard: invalid canonical source roster")

// rf3CanonicalSourceRoster is a durable learned source set. Manifest seeds
// establish the first trust edge; subsequent exact source cuts replace this
// bounded set and floor as one atomic file so removal of an original catalog
// voter cannot strand a recovering physical receiver.
type rf3CanonicalSourceRoster struct {
	Format uint16                                `json:"format"`
	Floor  frontenddrain.PreparedAckCutReadFloor `json:"floor"`
	Seeds  []nodecontrol.BootstrapGatewaySeed    `json:"seeds"`
}

func loadRF3CanonicalSourceRoster(path string) (*rf3CanonicalSourceRoster, error) {
	if path == "" {
		return nil, errRF3CanonicalSourceRoster
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= sha256.Size || info.Size() > rf3CanonicalSourceRosterMaxBytes {
		return nil, errors.Join(errRF3CanonicalSourceRoster, err)
	}
	raw, err := io.ReadAll(io.LimitReader(file, rf3CanonicalSourceRosterMaxBytes+1))
	if err != nil || len(raw) <= sha256.Size || len(raw) > rf3CanonicalSourceRosterMaxBytes {
		return nil, errors.Join(errRF3CanonicalSourceRoster, err)
	}
	body, trailer := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	digest := sha256.Sum256(body)
	if !bytes.Equal(trailer, digest[:]) {
		return nil, errRF3CanonicalSourceRoster
	}
	var roster rf3CanonicalSourceRoster
	if err = vibejson.Unmarshal(body, &roster); err != nil {
		return nil, errors.Join(errRF3CanonicalSourceRoster, err)
	}
	canonical, err := vibejson.Marshal(&roster)
	if err != nil || !bytes.Equal(canonical, body) || roster.Format != rf3CanonicalSourceRosterFormat || !roster.Floor.Valid() ||
		len(roster.Seeds) == 0 || len(roster.Seeds) > frontenddrain.MaxPreparedAckSourceRoster {
		return nil, errors.Join(errRF3CanonicalSourceRoster, err)
	}
	seenNodes := make(map[rafttransport.NodeID]struct{}, len(roster.Seeds))
	seenAddresses := make(map[string]struct{}, len(roster.Seeds))
	for index, seed := range roster.Seeds {
		if !seed.Valid() || index > 0 && bytes.Compare(roster.Seeds[index-1].NodeID[:], seed.NodeID[:]) >= 0 {
			return nil, errRF3CanonicalSourceRoster
		}
		if _, found := seenNodes[seed.NodeID]; found {
			return nil, errRF3CanonicalSourceRoster
		}
		if _, found := seenAddresses[seed.ControlAddress]; found {
			return nil, errRF3CanonicalSourceRoster
		}
		seenNodes[seed.NodeID], seenAddresses[seed.ControlAddress] = struct{}{}, struct{}{}
	}
	return &roster, nil
}

func persistRF3CanonicalSourceRoster(path string, roster rf3CanonicalSourceRoster) error {
	if path == "" || roster.Format != rf3CanonicalSourceRosterFormat || !roster.Floor.Valid() ||
		len(roster.Seeds) == 0 || len(roster.Seeds) > frontenddrain.MaxPreparedAckSourceRoster {
		return errRF3CanonicalSourceRoster
	}
	canonical, err := vibejson.Marshal(&roster)
	if err != nil || len(canonical) == 0 || len(canonical)+sha256.Size > rf3CanonicalSourceRosterMaxBytes {
		return errors.Join(errRF3CanonicalSourceRoster, err)
	}
	digest := sha256.Sum256(canonical)
	raw := append(append([]byte(nil), canonical...), digest[:]...)
	if info, statErr := os.Lstat(path); statErr == nil && !info.Mode().IsRegular() {
		return errRF3CanonicalSourceRoster
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return errors.Join(errRF3CanonicalSourceRoster, statErr)
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".frontend-drain-source-roster-*")
	if err != nil {
		return errors.Join(errRF3CanonicalSourceRoster, err)
	}
	temporaryPath := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err == nil {
		var written int
		written, err = temporary.Write(raw)
		if err == nil && written != len(raw) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return errors.Join(errRF3CanonicalSourceRoster, err)
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return errors.Join(errRF3CanonicalSourceRoster, err)
	}
	keep = true
	directoryFile, err := os.Open(directory)
	if err != nil {
		return errors.Join(errRF3CanonicalSourceRoster, err)
	}
	if err = directoryFile.Sync(); err == nil {
		err = directoryFile.Close()
	} else {
		_ = directoryFile.Close()
	}
	if err != nil {
		return errors.Join(errRF3CanonicalSourceRoster, err)
	}
	return nil
}

func rf3CanonicalSourceSeedsFromCut(cut frontenddrain.PreparedAckCut) ([]nodecontrol.BootstrapGatewaySeed, error) {
	if !cut.Valid() || len(cut.SourceRoster) == 0 || len(cut.SourceRoster) > frontenddrain.MaxPreparedAckSourceRoster {
		return nil, errRF3CanonicalSourceRoster
	}
	seeds := make([]nodecontrol.BootstrapGatewaySeed, 0, len(cut.SourceRoster))
	for _, source := range cut.SourceRoster {
		seed := nodecontrol.BootstrapGatewaySeed{NodeID: source.NodeID, Incarnation: source.Incarnation,
			ControlAddress: source.ControlAddress, SPKIPinDigest: replication.Digest(source.SPKIPinDigest)}
		if !seed.Valid() {
			return nil, errRF3CanonicalSourceRoster
		}
		seeds = append(seeds, seed)
	}
	slices.SortFunc(seeds, func(left, right nodecontrol.BootstrapGatewaySeed) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})
	for index := 1; index < len(seeds); index++ {
		if seeds[index-1].NodeID == seeds[index].NodeID || seeds[index-1].ControlAddress == seeds[index].ControlAddress {
			return nil, errRF3CanonicalSourceRoster
		}
	}
	return seeds, nil
}

func rf3FrontendDrainPreparedAckSeedEndpoints(
	seeds []nodecontrol.BootstrapGatewaySeed,
	destination map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed,
) ([]servicetls.Endpoint, error) {
	if len(seeds) == 0 {
		return nil, nil
	}
	endpoints := make([]servicetls.Endpoint, 0, len(seeds))
	seenAddresses := make(map[string]struct{}, len(seeds))
	for _, seed := range seeds {
		if !seed.Valid() {
			return nil, errRF3FrontendDrainPreparedAckReaderUnavailable
		}
		if _, found := destination[seed.NodeID]; found {
			return nil, errRF3FrontendDrainPreparedAckReaderUnavailable
		}
		if _, found := seenAddresses[seed.ControlAddress]; found {
			return nil, errRF3FrontendDrainPreparedAckReaderUnavailable
		}
		destination[seed.NodeID] = seed
		seenAddresses[seed.ControlAddress] = struct{}{}
		endpoints = append(endpoints, servicetls.Endpoint{Address: seed.ControlAddress, Node: seed.NodeID})
	}
	return endpoints, nil
}

// Close releases only the additional physical source transport. The gateway
// transport remains the second constructor result for existing callers and is
// closed by those callers alongside the reader.
func (reader *rf3FrontendDrainPreparedAckCutReader) Close() error {
	if reader == nil {
		return nil
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.canonicalSourceTransport == nil {
		return nil
	}
	return reader.canonicalSourceTransport.Close()
}

func (reader *rf3FrontendDrainPreparedAckCutReader) observeCanonicalSourceCut(
	cut frontenddrain.PreparedAckCut,
) error {
	if reader == nil || !cut.Valid() {
		return errRF3CanonicalSourceRoster
	}
	seeds, err := rf3CanonicalSourceSeedsFromCut(cut)
	if err != nil {
		return err
	}
	endpoints := make([]servicetls.Endpoint, 0, len(seeds))
	next := make(map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed, len(seeds))
	for _, seed := range seeds {
		next[seed.NodeID] = seed
		endpoints = append(endpoints, servicetls.Endpoint{Address: seed.ControlAddress, Node: seed.NodeID})
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	changed := !sameRF3CanonicalSourceSeedMap(reader.canonicalSourceSeeds, next)
	if changed {
		if reader.profile == nil {
			return errRF3CanonicalSourceRoster
		}
		if reader.canonicalSourceTransport == nil {
			reader.canonicalSourceTransport, err = newRF3CanonicalSourceTransport(reader.profile, endpoints, reader.writeDeadline)
		} else {
			err = reader.canonicalSourceTransport.Rotate(reader.profile.WithLocalServiceConnections(), endpoints)
		}
		if err != nil {
			return errors.Join(errRF3CanonicalSourceRoster, err)
		}
	}
	roster := rf3CanonicalSourceRoster{Format: rf3CanonicalSourceRosterFormat, Floor: cut.ReadFloor(), Seeds: seeds}
	if reader.sourceRosterPath != "" && (changed || reader.sourceFloor != roster.Floor) {
		if err = persistRF3CanonicalSourceRoster(reader.sourceRosterPath, roster); err != nil {
			return err
		}
	}
	reader.canonicalSourceSeeds = next
	reader.sourceFloor = roster.Floor
	return nil
}

func sameRF3CanonicalSourceSeedMap(
	left, right map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed,
) bool {
	if len(left) != len(right) {
		return false
	}
	for node, seed := range left {
		if other, found := right[node]; !found || other != seed {
			return false
		}
	}
	return true
}

// ReadFrontendDrainPreparedAckCut implements shardservice.FrontendDrainPreparedAckCutReader.
func (reader *rf3FrontendDrainPreparedAckCutReader) ReadFrontendDrainPreparedAckCut(
	ctx context.Context, request frontenddrain.PreparedAckRequest,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || ctx == nil || !request.Valid() || request.ReceiverNode != reader.localNode ||
		request.ReceiverServiceKeyDigest != reader.localServiceKey {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	if reader.localIncarnation != 0 && request.ReceiverIncarnation != reader.localIncarnation {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	reader.mu.RLock()
	seed, found := reader.seeds[request.SourcePrincipal]
	transport := reader.transport
	trafficClass := rafttransport.TrafficGatewayControl
	physicalSource := false
	if !found {
		seed, found = reader.canonicalSourceSeeds[request.SourcePrincipal]
		transport = reader.canonicalSourceTransport
		trafficClass = rafttransport.TrafficShardControl
		physicalSource = found
	}
	unmanaged := len(reader.seeds) == 0 && len(reader.canonicalSourceSeeds) == 0 &&
		reader.transport == nil && reader.canonicalSourceTransport == nil
	reader.mu.RUnlock()
	if !found || replication.Digest(request.SourcePrincipalKeyDigest) != seed.SPKIPinDigest ||
		transport == nil {
		// Grouped fixtures without source seeds are the nonmanaged serving
		// form: they have no independent ReadLatest route. The ACK caller is
		// already TLS-authenticated as SourcePrincipal, so the exact cut on
		// the request is the only source this process can install.
		if unmanaged && rf3FrontendDrainPreparedAckCutPublishedBy(
			request.SourceCut, request.SourcePrincipal, request.SourcePrincipalKeyDigest,
		) {
			return request.SourceCut, nil
		}
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	connection, err := transport.Dial(ctx, seed.ControlAddress)
	if err != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, err)
	}
	peer, ok := connection.(rafttransport.PeerConnection)
	if !ok || peer == nil {
		_ = connection.Close()
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	defer peer.Close()
	cut, err := reader.readFromConnectionClass(ctx, peer, seed, request, trafficClass, physicalSource)
	if err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	if observeErr := reader.observeCanonicalSourceCut(cut); observeErr != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderState, observeErr)
	}
	return cut, nil
}

// readLatestFrontendDrainCut performs one bootstrap/recovery source request.
// It does not require a drain or bearer grant and tries only the
// manifest-pinned gateway identities, in deterministic order. The source
// gateway independently reads its authority cut; no caller-owned cut bytes
// are accepted on this path.
func (reader *rf3FrontendDrainPreparedAckCutReader) readLatestFrontendDrainCut(
	ctx context.Context, query frontenddrain.PreparedAckCutReadRequest,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || ctx == nil || !query.Valid() || query.Operation != frontenddrain.CutOperationReadLatest ||
		query.ReceiverNode != reader.localNode || query.ReceiverServiceKeyDigest != reader.localServiceKey {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	reader.mu.Lock()
	if reader.transport == nil && reader.canonicalSourceTransport == nil {
		reader.mu.Unlock()
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	if query.SourceFloor == (frontenddrain.PreparedAckCutReadFloor{}) {
		query.SourceFloor = reader.sourceFloor
	}
	physicalTransport, physicalSeeds := reader.canonicalSourceTransport, cloneRF3CanonicalSourceSeeds(reader.canonicalSourceSeeds)
	gatewayTransport, gatewaySeeds := reader.transport, cloneRF3CanonicalSourceSeeds(reader.seeds)
	reader.mu.Unlock()
	var lastErr error
	// The physical source is attempted first so startup never depends on a
	// gateway having completed Runtime.Open. A surviving gateway remains an
	// independently authenticated source for manifests that have no physical
	// source seed yet; it is never used to reinterpret a storage seed.
	if physicalTransport != nil {
		cut, readErr := reader.readLatestFrontendDrainCutFrom(
			ctx, query, physicalTransport, physicalSeeds,
			rafttransport.TrafficShardControl, true,
		)
		if readErr == nil {
			if observeErr := reader.observeCanonicalSourceCut(cut); observeErr != nil {
				return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderState, observeErr)
			}
			return cut, nil
		}
		lastErr = readErr
	}
	if gatewayTransport != nil {
		cut, readErr := reader.readLatestFrontendDrainCutFrom(
			ctx, query, gatewayTransport, gatewaySeeds,
			rafttransport.TrafficGatewayControl, false,
		)
		if readErr == nil {
			if observeErr := reader.observeCanonicalSourceCut(cut); observeErr != nil {
				return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderState, observeErr)
			}
			return cut, nil
		}
		lastErr = readErr
	}
	if lastErr == nil {
		lastErr = errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	return frontenddrain.PreparedAckCut{}, lastErr
}

func (reader *rf3FrontendDrainPreparedAckCutReader) readLatestFrontendDrainCutFrom(
	ctx context.Context, query frontenddrain.PreparedAckCutReadRequest,
	transport *servicetls.Client,
	seeds map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed,
	trafficClass rafttransport.TrafficClass,
	physicalSource bool,
) (frontenddrain.PreparedAckCut, error) {
	if transport == nil || len(seeds) == 0 {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	keys := make([]rafttransport.NodeID, 0, len(seeds))
	for node := range seeds {
		keys = append(keys, node)
	}
	slices.SortFunc(keys, func(left, right rafttransport.NodeID) int {
		return bytes.Compare(left[:], right[:])
	})
	var lastErr error
	sourceKind := "gateway"
	if physicalSource {
		sourceKind = "physical"
	}
	for _, node := range keys {
		seed := seeds[node]
		connection, err := transport.Dial(ctx, seed.ControlAddress)
		if err != nil {
			lastErr = fmt.Errorf("%s source node=%x address=%s dial: %w", sourceKind, node, seed.ControlAddress,
				errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, err))
			continue
		}
		peer, ok := connection.(rafttransport.PeerConnection)
		if !ok || peer == nil {
			_ = connection.Close()
			lastErr = fmt.Errorf("%s source node=%x address=%s peer connection: %w", sourceKind, node, seed.ControlAddress, errRF3FrontendDrainPreparedAckReaderAuth)
			continue
		}
		cut, readErr := reader.readQueryFromConnectionClass(ctx, peer, seed, query, trafficClass, physicalSource)
		_ = peer.Close()
		if readErr == nil {
			return cut, nil
		}
		lastErr = fmt.Errorf("%s source node=%x address=%s traffic=%v: %w", sourceKind, node, seed.ControlAddress, trafficClass, readErr)
	}
	if lastErr == nil {
		lastErr = errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	return frontenddrain.PreparedAckCut{}, lastErr
}

// ReadLatestFrontendDrainCut is the no-subject source seam used by an
// embedded gateway during startup. It creates a fresh nonce and an empty
// source floor; the physical reader identity and service key remain bound to
// the authenticated reader profile. Later receiver refreshes use the same
// wire implementation with an explicit floor through ReadLatestServiceCut.
func (reader *rf3FrontendDrainPreparedAckCutReader) ReadLatestFrontendDrainCut(
	ctx context.Context,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || ctx == nil {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderUnavailable, err)
	}
	return reader.readLatestFrontendDrainCut(ctx, frontenddrain.PreparedAckCutReadRequest{
		Operation:                frontenddrain.CutOperationReadLatest,
		Nonce:                    nonce,
		ReceiverNode:             reader.localNode,
		ReceiverIncarnation:      reader.localIncarnation,
		ReceiverServiceKeyDigest: reader.localServiceKey,
	})
}

// ReadLatestServiceCut is the canonical terminology used by startup/recovery
// callers while retaining one implementation and one wire route.
func (reader *rf3FrontendDrainPreparedAckCutReader) ReadLatestServiceCut(
	ctx context.Context, query frontenddrain.ServiceCutReadLatestRequest,
) (frontenddrain.ServiceCut, error) {
	return reader.readLatestFrontendDrainCut(ctx, query)
}

// readFromConnection is kept separate from the servicetls dial so the exact
// wire and post-handshake identity fence can be exercised with a bounded
// in-memory PeerConnection in focused tests. Production callers reach it only
// after Client.Dial has completed the GatewayControl TLS handshake.
func (reader *rf3FrontendDrainPreparedAckCutReader) readFromConnection(
	ctx context.Context, connection rafttransport.PeerConnection,
	seed nodecontrol.BootstrapGatewaySeed,
	request frontenddrain.PreparedAckRequest,
) (frontenddrain.PreparedAckCut, error) {
	return reader.readFromConnectionClass(ctx, connection, seed, request,
		rafttransport.TrafficGatewayControl, false)
}

func (reader *rf3FrontendDrainPreparedAckCutReader) readFromConnectionClass(
	ctx context.Context, connection rafttransport.PeerConnection,
	seed nodecontrol.BootstrapGatewaySeed,
	request frontenddrain.PreparedAckRequest,
	trafficClass rafttransport.TrafficClass,
	physicalSource bool,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || ctx == nil || connection == nil || !seed.Valid() || !request.Valid() {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	}
	if request.SourcePrincipal != seed.NodeID || request.SourcePrincipalKeyDigest != [sha256.Size]byte(seed.SPKIPinDigest) {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	query := frontenddrain.PreparedAckCutReadRequest{
		Operation: frontenddrain.CutOperationInstallExact, RequirePrepared: request.RequirePrepared,
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision, SourceFloor: request.SourceCut.ReadFloor(),
		SourceCutDigest: request.SourceCutDigest(),
	}
	return reader.readQueryFromConnectionClass(ctx, connection, seed, query, trafficClass, physicalSource)
}

func (reader *rf3FrontendDrainPreparedAckCutReader) readQueryFromConnection(
	ctx context.Context, connection rafttransport.PeerConnection,
	seed nodecontrol.BootstrapGatewaySeed,
	query frontenddrain.PreparedAckCutReadRequest,
) (frontenddrain.PreparedAckCut, error) {
	return reader.readQueryFromConnectionClass(ctx, connection, seed, query,
		rafttransport.TrafficGatewayControl, false)
}

func (reader *rf3FrontendDrainPreparedAckCutReader) readQueryFromConnectionClass(
	ctx context.Context, connection rafttransport.PeerConnection,
	seed nodecontrol.BootstrapGatewaySeed,
	query frontenddrain.PreparedAckCutReadRequest,
	trafficClass rafttransport.TrafficClass,
	physicalSource bool,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || ctx == nil || connection == nil || !seed.Valid() || !query.Valid() {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	}
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != trafficClass ||
		peer.TrustDomain != reader.trust || peer.Node != seed.NodeID ||
		connection.PeerKeyDigest() != [sha256.Size]byte(seed.SPKIPinDigest) {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderAuth
	}
	if query.Operation == frontenddrain.CutOperationInstallExact &&
		(query.SourceCutDigest == ([32]byte{}) || query.SourceFloor == (frontenddrain.PreparedAckCutReadFloor{})) {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	encoded := query.Marshal()
	if len(encoded) != frontenddrain.PreparedAckCutReadRequestBytes {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	}
	if deadline := rf3FrontendDrainPreparedAckBoundedDeadline(ctx, reader.writeDeadline()); deadline.IsZero() {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	if err := writeRF3FrontendDrainPreparedAckFrame(connection, encoded); err != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
	}
	if deadline := rf3FrontendDrainPreparedAckBoundedDeadline(ctx, reader.readDeadline()); deadline.IsZero() {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	discriminator := make([]byte, len(frontenddrain.PreparedAckCutReadResponseDiscriminator))
	if _, err := io.ReadFull(connection, discriminator); err != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
	}
	if bytes.Equal(discriminator, frontenddrain.PreparedAckCutReadMovedResponseDiscriminator[:]) {
		raw := make([]byte, frontenddrain.PreparedAckCutReadMovedResponseBytes)
		copy(raw, discriminator)
		if _, err := io.ReadFull(connection, raw[len(discriminator):]); err != nil {
			return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
		}
		if _, err := frontenddrain.OpenPreparedAckCutReadMovedResponse(raw, query); err != nil {
			return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
		}
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderState, frontenddrain.ErrPreparedAckCutMoved)
	}
	if !bytes.Equal(discriminator, frontenddrain.PreparedAckCutReadResponseDiscriminator[:]) {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	}
	header := make([]byte, frontenddrain.PreparedAckCutReadResponseHeaderBytes)
	copy(header, discriminator)
	if _, err := io.ReadFull(connection, header[len(discriminator):]); err != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
	}
	cutBytes := binary.LittleEndian.Uint32(header[frontenddrain.PreparedAckCutReadResponseCutBytesOffset : frontenddrain.PreparedAckCutReadResponseCutBytesOffset+4])
	if cutBytes == 0 || uint64(cutBytes) > frontenddrain.MaxPreparedAckCutBytes {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderWire
	}
	raw := make([]byte, len(header)+int(cutBytes)+sha256.Size)
	copy(raw, header)
	if _, err := io.ReadFull(connection, raw[len(header):]); err != nil {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
	}
	response, err := frontenddrain.OpenPreparedAckCutReadResponse(raw, query)
	if err != nil || !response.Valid(query) {
		return frontenddrain.PreparedAckCut{}, errors.Join(errRF3FrontendDrainPreparedAckReaderWire, err)
	}
	if query.Operation == frontenddrain.CutOperationInstallExact && response.Cut.Digest() != query.SourceCutDigest {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderState
	}
	if physicalSource {
		if !rf3FrontendDrainPreparedAckPhysicalSourceCutMatchesQuery(response.Cut, query, seed) {
			return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderState
		}
	} else if !rf3FrontendDrainPreparedAckSourceCutMatchesQuery(response.Cut, query, seed) {
		return frontenddrain.PreparedAckCut{}, errRF3FrontendDrainPreparedAckReaderState
	}
	return response.Cut, nil
}

func rf3FrontendDrainPreparedAckBoundedDeadline(ctx context.Context, configured time.Time) time.Time {
	if ctx == nil || configured.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(configured) {
		return deadline
	}
	return configured
}

func writeRF3FrontendDrainPreparedAckFrame(writer io.Writer, frame []byte) error {
	for len(frame) != 0 {
		written, err := writer.Write(frame)
		if written > 0 {
			frame = frame[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func rf3FrontendDrainPreparedAckSourceCutMatches(
	cut frontenddrain.PreparedAckCut,
	request frontenddrain.PreparedAckRequest,
	seed nodecontrol.BootstrapGatewaySeed,
) bool {
	if !request.Valid() || request.SourcePrincipal != seed.NodeID ||
		request.SourcePrincipalKeyDigest != [sha256.Size]byte(seed.SPKIPinDigest) {
		return false
	}
	query := frontenddrain.PreparedAckCutReadRequest{
		Operation: frontenddrain.CutOperationInstallExact, RequirePrepared: request.RequirePrepared,
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision, SourceFloor: request.SourceCut.ReadFloor(),
		SourceCutDigest: request.SourceCutDigest(),
	}
	return rf3FrontendDrainPreparedAckSourceCutMatchesQuery(cut, query, seed)
}

func rf3FrontendDrainPreparedAckSourceCutMatchesQuery(
	cut frontenddrain.PreparedAckCut,
	query frontenddrain.PreparedAckCutReadRequest,
	seed nodecontrol.BootstrapGatewaySeed,
) bool {
	if !cut.Valid() || !query.Valid() || !seed.Valid() ||
		query.ReceiverNode == (rafttransport.NodeID{}) {
		return false
	}
	foundSource := false
	for _, binding := range cut.ServiceDirectory.Bindings {
		if binding.Principal != seed.NodeID {
			continue
		}
		if binding.Roles&serviceauthz.ServiceRoleGateway == 0 ||
			binding.KeyDigest != [sha256.Size]byte(seed.SPKIPinDigest) ||
			binding.GatewayIncarnation != seed.Incarnation ||
			(binding.Lifecycle != serviceauthz.ServiceActive && binding.Lifecycle != serviceauthz.ServiceDraining) {
			return false
		}
		foundSource = true
	}
	if !foundSource {
		return false
	}
	if query.DrainID == ([32]byte{}) {
		return query.GrantDigest == ([32]byte{})
	}
	if query.GrantDigest == ([32]byte{}) {
		foundSubject := false
		for _, subject := range cut.Subjects {
			if subject.DrainID != query.DrainID {
				continue
			}
			if foundSubject || !subject.Valid() || subject.GrantDigest != ([32]byte{}) ||
				subject.GatewayServiceID != seed.NodeID ||
				(query.RequirePrepared && subject.Lifecycle != 1) {
				return false
			}
			foundSubject = true
		}
		return foundSubject
	}
	foundGrant := false
	for _, grant := range cut.ServiceDirectory.ContinuationGrants {
		if grant.GrantDigest != query.GrantDigest {
			continue
		}
		if !grant.Valid() || (query.RequirePrepared && grant.State != serviceauthz.ContinuationGrantPrepared) ||
			grant.DrainID != query.DrainID || grant.GatewayServiceID != seed.NodeID ||
			grant.PeerKeyDigest != [32]byte(seed.SPKIPinDigest) {
			return false
		}
		if foundGrant {
			return false
		}
		foundGrant = true
	}
	return foundGrant
}

// rf3FrontendDrainPreparedAckPhysicalSourceCutMatchesQuery verifies the
// source identity carried by a TrafficShardControl reply. Physical sources
// publish the canonical cut from storage and therefore do not carry a gateway
// binding for the source itself. Both no-subject source operations are valid:
// ReadLatest obtains a current cut, while InstallExact rechecks the exact
// digest for a publication that has no frontend drain subject.
func rf3FrontendDrainPreparedAckPhysicalSourceCutMatchesQuery(
	cut frontenddrain.PreparedAckCut,
	query frontenddrain.PreparedAckCutReadRequest,
	seed nodecontrol.BootstrapGatewaySeed,
) bool {
	if !cut.Valid() || !query.Valid() || !seed.Valid() ||
		(query.Operation != frontenddrain.CutOperationReadLatest && query.Operation != frontenddrain.CutOperationInstallExact) ||
		query.Operation == frontenddrain.CutOperationReadLatest &&
			(query.DrainID != ([32]byte{}) || query.GrantDigest != ([32]byte{})) {
		return false
	}
	foundSource := false
	for _, binding := range cut.ServiceDirectory.Bindings {
		if binding.Principal != seed.NodeID {
			continue
		}
		if foundSource || !binding.Valid() || binding.PhysicalNode != seed.NodeID ||
			binding.PhysicalIncarnation != seed.Incarnation ||
			binding.KeyDigest != [32]byte(seed.SPKIPinDigest) ||
			binding.Roles&serviceauthz.ServiceRoleStorage == 0 ||
			(binding.Lifecycle != serviceauthz.ServiceActive && binding.Lifecycle != serviceauthz.ServiceDraining) {
			return false
		}
		foundSource = true
	}
	if !foundSource {
		return false
	}
	rosterMatch := false
	for _, source := range cut.SourceRoster {
		if source.Valid() && source.NodeID == seed.NodeID && source.Incarnation == seed.Incarnation &&
			source.ControlAddress != "" && source.SPKIPinDigest == [32]byte(seed.SPKIPinDigest) {
			rosterMatch = true
			break
		}
	}
	if !rosterMatch {
		return false
	}
	if query.DrainID == ([32]byte{}) {
		return query.GrantDigest == ([32]byte{})
	}
	if query.GrantDigest == ([32]byte{}) {
		for _, subject := range cut.Subjects {
			if subject.DrainID == query.DrainID {
				return subject.Valid() && subject.GrantDigest == ([32]byte{}) &&
					(!query.RequirePrepared || subject.Lifecycle == uint8(gateway.FrontendDrainPrepared))
			}
		}
		return false
	}
	for _, grant := range cut.ServiceDirectory.ContinuationGrants {
		if grant.GrantDigest == query.GrantDigest {
			return grant.Valid() && grant.DrainID == query.DrainID &&
				(!query.RequirePrepared || grant.State == serviceauthz.ContinuationGrantPrepared)
		}
	}
	return false
}

func rf3FrontendDrainPreparedAckCutPublishedBy(
	cut frontenddrain.PreparedAckCut, principal rafttransport.NodeID, key [32]byte,
) bool {
	if !cut.Valid() || principal == (rafttransport.NodeID{}) || key == ([32]byte{}) {
		return false
	}
	for _, binding := range cut.ServiceDirectory.Bindings {
		if binding.Principal == principal && binding.KeyDigest == key &&
			binding.Roles&serviceauthz.ServiceRoleGateway != 0 &&
			(binding.Lifecycle == serviceauthz.ServiceActive || binding.Lifecycle == serviceauthz.ServiceDraining) {
			return true
		}
	}
	return false
}

// rf3NonmanagedPreparedAckInstaller acknowledges a publisher cut without
// binding a native service directory. Seed-less receivers cannot refresh that
// directory independently, and installing it would switch CheckDelegate on
// for a process that is still using the static Policy roster.
type rf3NonmanagedPreparedAckInstaller struct{}

func (rf3NonmanagedPreparedAckInstaller) InstallFrontendDrainServiceCut(
	ctx context.Context, cut frontenddrain.PreparedAckCut,
) (uint64, error) {
	if ctx == nil || !cut.Valid() {
		return 0, errRF3FrontendDrainPreparedAckReaderUnavailable
	}
	return cut.ServiceDirectoryRevision, nil
}

func newRF3NonmanagedPreparedAckService(
	profile *rafttransport.PeerTLS, policy *serviceauthz.Policy,
	incarnation uint64, deadline rafttransport.DeadlineFunc,
) (*shardservice.FrontendDrainPreparedAckService, *rf3FrontendDrainPreparedAckCutReader, *servicetls.Client, error) {
	reader, transport, err := newRF3FrontendDrainPreparedAckCutReaderWithSources(
		profile, nil, nil, incarnation, deadline, deadline,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	service, err := shardservice.NewFrontendDrainPreparedAckService(
		shardservice.FrontendDrainPreparedAckServiceOptions{
			Reader: reader, Installer: rf3NonmanagedPreparedAckInstaller{},
			TrustDomain:  profile.LocalIdentity().TrustDomain,
			Authorize:    rf3FrontendDrainPreparedAckAuthorizer(profile, policy),
			ReadDeadline: deadline, WriteDeadline: deadline,
		},
	)
	if err != nil {
		_ = reader.Close()
		if transport != nil {
			_ = transport.Close()
		}
		return nil, nil, nil, err
	}
	return service, reader, transport, nil
}

func rf3FrontendDrainPreparedAckAuthorizer(
	profile *rafttransport.PeerTLS, policy *serviceauthz.Policy,
) func(rafttransport.PeerIdentity, frontenddrain.PreparedAckRequest) bool {
	if profile == nil || policy == nil {
		return nil
	}
	trust := profile.LocalIdentity().TrustDomain
	local := profile.LocalIdentity().Node
	return func(identity rafttransport.PeerIdentity, request frontenddrain.PreparedAckRequest) bool {
		if !request.Valid() || identity.TrustDomain != trust || identity.Node != request.SourcePrincipal ||
			request.ReceiverNode != local || request.SourcePrincipalKeyDigest == ([sha256.Size]byte{}) {
			return false
		}
		return policy.Check(identity.Node, serviceauthz.CapabilityTopology) == serviceauthz.DecisionAllow ||
			policy.Check(identity.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
	}
}
