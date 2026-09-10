package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

func newRF3PreparationSource(schemas *rf3SchemaActivator, registry *rafttransport.StaticRegistry, policy *serviceauthz.Policy, root string, deadline rafttransport.DeadlineFunc) (*nodecontrol.PreparationSourceService, error) {
	if schemas == nil || registry == nil || policy == nil || root == "" {
		return nil, nodecontrol.ErrControl
	}
	root = filepath.Join(root, "preparation-exports")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	var mu sync.Mutex
	provider := func(ctx context.Context, intent gateway.GroupEnrollmentIntent, voters [3]nodecontrol.PreparationMember, target nodecontrol.PreparationMember) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		path := filepath.Join(root, hex.EncodeToString(intent.IntentID[:])+".export")
		bindingIntent := intent
		bindingIntent.ExpectedManifestDigest = replication.Digest{}
		binding := bindingIntent.Digest()
		validate := func(raw []byte) ([]byte, error) {
			spec, err := nodecontrol.OpenPreparationSpec(raw)
			if err != nil {
				return nil, err
			}
			expected := intent.ExpectedManifestDigest
			intent.ExpectedManifestDigest = replication.Digest(sha256.Sum256(raw))
			if expected != (replication.Digest{}) && expected != intent.ExpectedManifestDigest {
				return nil, nodecontrol.ErrConflict
			}
			if voters != ([3]nodecontrol.PreparationMember{}) && spec.InitialVoters != voters {
				return nil, nodecontrol.ErrConflict
			}
			if target != (nodecontrol.PreparationMember{}) && spec.Target != target {
				return nil, nodecontrol.ErrConflict
			}
			if err = spec.ValidateAgainst(intent); err != nil {
				return nil, fmt.Errorf("preparation source payload binding: %w", err)
			}
			return raw, nil
		}
		if retained, err := readRF3BoundedFile(path, nodecontrol.MaxPayloadBytes+32); err == nil {
			if len(retained) <= 32 || !bytes.Equal(retained[:32], binding[:]) {
				return nil, nodecontrol.ErrConflict
			}
			return validate(retained[32:])
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		directory, err := os.Open(root)
		if err != nil {
			return nil, err
		}
		entries, readErr := directory.ReadDir(1025)
		_ = directory.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
		}
		if len(entries) >= 1024 {
			return nil, nodecontrol.ErrBound
		}
		schemas.mu.RLock()
		state := schemas.groups[intent.Group]
		schemas.mu.RUnlock()
		if state == nil {
			return nil, fmt.Errorf("preparation source group is not hosted: %w", nodecontrol.ErrStale)
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.apply == nil || state.identity.MemberID != intent.Source.Member || state.identity.StoreID != intent.Source.StoreID || state.identity.NodeIncarnation < intent.Source.NodeIncarnation {
			return nil, fmt.Errorf("preparation source runtime binding differs from catalog: %w", nodecontrol.ErrStale)
		}
		publication := state.apply.Published()
		profile, err := state.apply.CapacityQualificationProfile()
		if err != nil {
			return nil, err
		}
		if actual := commandFenceFromPublication(profile.Binding.Authority, state.identity, publication.ReplicaSetVersion); actual != intent.ExpectedCommand {
			return nil, fmt.Errorf("preparation source command got=%+v want=%+v: %w", actual, intent.ExpectedCommand, nodecontrol.ErrStale)
		}
		if publication.ConfState == nil || len(publication.ConfState.Voters) != 3 || len(publication.ConfState.Learners) != 0 || len(publication.ConfState.VotersOutgoing) != 0 {
			return nil, fmt.Errorf("preparation source membership is not stable RF3: %w", nodecontrol.ErrStale)
		}
		for _, voter := range voters {
			if !slices.Contains(publication.ConfState.Voters, voter.MemberID) {
				return nil, fmt.Errorf("preparation source voter %d absent: %w", voter.MemberID, nodecontrol.ErrStale)
			}
			node, err := registry.Node(intent.Group, voter.MemberID)
			if err != nil || node != voter.Node {
				return nil, fmt.Errorf("preparation source voter %d physical identity mismatch: %w", voter.MemberID, nodecontrol.ErrStale)
			}
			peer, err := registry.PhysicalPeer(node)
			if err != nil || peer.Endpoint != voter.PeerAddress {
				return nil, fmt.Errorf("preparation source voter %d peer endpoint %q differs from %q: %w", voter.MemberID, peer.Endpoint, voter.PeerAddress, nodecontrol.ErrStale)
			}
		}
		template := state.manifest.SplitControl.ChildRegistry
		description, err := sqldriver.DescribeReplicatedSchemaCatalog(state.path)
		if err != nil {
			return nil, err
		}
		template, err = refreshRF3SplitChildSchema(template, description)
		if err != nil {
			return nil, err
		}
		bootstrap, err := readRF3BoundedFile(template.StaticBootstrapPath, nodecontrol.MaxSourceBootstrapBytes)
		if err != nil {
			return nil, err
		}
		apply := state.applyID
		wal := template.WAL.Options
		indexes := make([]nodecontrol.PreparationGlobalIndex, len(template.GlobalIndexes))
		for i, index := range template.GlobalIndexes {
			indexes[i] = nodecontrol.PreparationGlobalIndex{Relation: uint16(index.Relation), Table: index.Table, IndexID: index.IndexID, Incarnation: index.Incarnation, LocatorCount: index.LocatorCount, Unique: index.Unique, KeyEncoding: uint8(index.KeyEncoding), KeyArity: index.KeyArity, TupleVersion: uint32(index.TupleVersion), MapperVersion: uint32(index.MapperVersion), BucketBits: index.BucketBits}
		}
		exported, err := nodecontrol.ExportPreparationSpec(nodecontrol.PreparationExportInput{
			Intent: intent, InitialVoters: voters, Target: target,
			Log:   nodecontrol.PreparationLogProfile{MaxFileBytes: wal.MaxFileBytes, MaxRecordBytes: wal.MaxRecordBytes, MaxRecords: wal.MaxRecords, MaxEntries: wal.MaxEntries, MaxLiveBytes: wal.MaxLiveBytes},
			Table: template.Table, CreateTable: template.CreateTable, SchemaStatements: template.SchemaStatements, GlobalIndexes: indexes, SourceBootstrap: bootstrap,
			Apply: nodecontrol.PreparationApplyProfile{MaxSessions: apply.MaxSessions, RetryWindow: apply.RetryWindow, MaxCollections: apply.TxnLimits.MaxCollections, MaxDocuments: apply.TxnLimits.MaxDocuments, MaxBytes: apply.TxnLimits.MaxBytes, ShardKey: apply.Placement.ShardKey, RequestLedgerCapacityBytes: apply.RequestLedgerCapacityBytes, RequestLedgerCleanupReserveBytes: apply.RequestLedgerCleanupReserveBytes, RequestLedgerRangeStart: apply.RequestLedgerRangeStart, RequestLedgerRangeEnd: apply.RequestLedgerRangeEnd, RequestLedgerRangeIdentity: apply.RequestLedgerRangeIdentity},
		})
		if err != nil {
			return nil, err
		}
		raw, err := validate(exported.Bytes)
		if err != nil {
			return nil, err
		}
		image := append(append([]byte(nil), binding[:]...), raw...)
		if err = writeRF3DurableMarker(path, image); err != nil {
			return nil, err
		}
		retained, err := readRF3BoundedFile(path, nodecontrol.MaxPayloadBytes+32)
		if err != nil || !bytes.Equal(image, retained) {
			return nil, errors.Join(nodecontrol.ErrConflict, err)
		}
		return retained[32:], nil
	}
	return nodecontrol.NewPreparationSourceService(provider, registry.LocalNode(), func(peer rafttransport.PeerIdentity, intent gateway.GroupEnrollmentIntent) bool {
		return intent.Source.Node == registry.LocalNode() && policy.Check(peer.Node, serviceauthz.CapabilityMembership) == serviceauthz.DecisionAllow
	}, deadline, deadline)
}
