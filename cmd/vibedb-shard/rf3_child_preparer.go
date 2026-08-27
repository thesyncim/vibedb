package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

const rf3ChildPrepareReceiptName = "prepare.receipt"

type rf3ChildPreparer struct {
	mu       sync.Mutex
	registry *rf3SplitChildPathRegistry
	local    rafttransport.NodeID
	peer     string
	native   string
	control  string
	snapshot string
}

func newRF3ChildPreparer(
	registry *rf3SplitChildPathRegistry,
	local rafttransport.NodeID,
	peer, native, control, snapshot net.Addr,
) (*rf3ChildPreparer, error) {
	if registry == nil || local == (rafttransport.NodeID{}) || peer == nil || native == nil || control == nil || snapshot == nil {
		return nil, errRF3Serving
	}
	result := &rf3ChildPreparer{
		registry: registry, local: local, peer: peer.String(), control: control.String(), snapshot: snapshot.String(),
	}
	result.native = native.String()
	return result, nil
}

func (preparer *rf3ChildPreparer) PrepareChild(
	ctx context.Context, preparation splitcontroller.ChildPreparation,
) (splitcontroller.ChildPrepareReceipt, error) {
	if preparer == nil || ctx == nil {
		return splitcontroller.ChildPrepareReceipt{}, splitcontroller.ErrChildPreparation
	}
	preparer.mu.Lock()
	defer preparer.mu.Unlock()
	if cause := context.Cause(ctx); cause != nil {
		return splitcontroller.ChildPrepareReceipt{}, cause
	}
	target := preparation.ReplicaTarget()
	paths, err := preparer.registry.acquire(
		[32]byte(preparation.OperationID()), preparation.Child(),
	)
	if err != nil || !preparer.matchesLocalTarget(target, paths) {
		return splitcontroller.ChildPrepareReceipt{}, errors.Join(
			splitcontroller.ErrChildPreparation, err,
		)
	}
	receiptPath := filepath.Join(paths.Root, rf3ChildPrepareReceiptName)
	if raw, readErr := readPrepareRF3File(receiptPath, splitcontroller.MaxChildPreparationBytes); readErr == nil {
		receipt, openErr := splitcontroller.OpenChildPrepareReceipt(raw)
		if openErr != nil || !rf3ChildReceiptMatches(preparation, receipt) {
			return splitcontroller.ChildPrepareReceipt{}, errors.Join(
				splitcontroller.ErrChildPreparation, openErr,
			)
		}
		if verifyErr := verifyRF3PreparedChildSQL(paths.Database, target); verifyErr != nil {
			return splitcontroller.ChildPrepareReceipt{}, verifyErr
		}
		return receipt, nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return splitcontroller.ChildPrepareReceipt{}, errors.Join(
			splitcontroller.ErrChildPreparation, readErr,
		)
	}
	if err = ensureRF3PreparedChildDirectories(preparer.registry.template.Root, paths.Root); err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	if _, statErr := os.Lstat(paths.WAL); statErr == nil || !errors.Is(statErr, os.ErrNotExist) {
		return splitcontroller.ChildPrepareReceipt{}, errors.Join(
			splitcontroller.ErrChildPreparation, statErr,
		)
	}
	if err = prepareRF3ChildSQL(ctx, preparer.registry.template, paths.Database, target); err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	receipt, err := splitcontroller.NewChildPrepareReceipt(preparation, target)
	if err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	raw, err := splitcontroller.AppendChildPrepareReceipt(nil, receipt)
	if err != nil {
		return splitcontroller.ChildPrepareReceipt{}, err
	}
	if err = writePrepareRF3File(receiptPath, raw, 0o600); err != nil {
		if settled, readErr := readPrepareRF3File(
			receiptPath, splitcontroller.MaxChildPreparationBytes,
		); readErr != nil || !bytes.Equal(settled, raw) {
			return splitcontroller.ChildPrepareReceipt{}, errors.Join(
				splitcontroller.ErrRuntimeStoreOutcomeUnknown, err, readErr,
			)
		}
	}
	if err = syncPrepareRF3Directory(paths.Root); err != nil {
		return splitcontroller.ChildPrepareReceipt{}, errors.Join(
			splitcontroller.ErrRuntimeStoreOutcomeUnknown, err,
		)
	}
	return receipt, nil
}

func (preparer *rf3ChildPreparer) matchesLocalTarget(
	target splitcontroller.ChildReplicaTarget, paths rf3SplitChildPaths,
) bool {
	if target.Node != preparer.local || target.RuntimeRoot != paths.Root ||
		target.SQLPath != paths.Database || target.WALPath != paths.WAL ||
		string(target.Endpoint) != preparer.peer || string(target.ControlEndpoint) != preparer.control ||
		target.SnapshotAddress != preparer.snapshot ||
		(preparer.native == "" || string(target.NativeEndpoint) != preparer.native) ||
		target.WAL.MemberID != target.Member || target.WAL.StoreID != target.StoreID ||
		target.SQL.Binding.MemberID != target.Member || target.SQL.Binding.StoreID != target.StoreID ||
		!rf3SplitChildTemplateMatchesRetained(preparer.registry.template, target.SQL, target.Apply) {
		return false
	}
	for _, member := range preparer.registry.template.Members[:preparer.registry.template.MemberCount] {
		if member.MemberID == target.Member {
			return member.NodeID == target.Node && member.PeerAddress == preparer.peer
		}
	}
	return false
}

func ensureRF3PreparedChildDirectories(root, child string) error {
	if info, err := os.Lstat(root); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(splitcontroller.ErrChildPreparation, err)
	}
	operation := filepath.Dir(child)
	for _, path := range [...]string{operation, child} {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return errors.Join(splitcontroller.ErrChildPreparation, err)
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.Join(splitcontroller.ErrChildPreparation, err)
		}
		if err = syncPrepareRF3Directory(filepath.Dir(path)); err != nil {
			return err
		}
	}
	return nil
}

func prepareRF3ChildSQL(
	ctx context.Context,
	template rf3ManifestSplitChildRegistry,
	path string,
	target splitcontroller.ChildReplicaTarget,
) error {
	if err := verifyRF3PreparedChildSQL(path, target); err == nil {
		return nil
	}
	local := sqldriver.ShardStoreIdentity{
		Distribution: distribution.DistributionName(target.SQL.Binding.Distribution),
		Shard:        distribution.ShardID(target.SQL.Binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(
			target.SQL.Binding.AllocationGeneration,
		),
		LogID: target.SQL.LogID,
	}
	database, err := sqldriver.InitializeShardStoreIdentity(path, local)
	if err != nil {
		return errors.Join(splitcontroller.ErrChildPreparation, err)
	}
	closeWith := func(cause error) error { return errors.Join(cause, database.Close()) }
	session, err := database.NewSession(ctx)
	if err != nil {
		return closeWith(err)
	}
	statement, prepareErr := session.Prepare(ctx, template.CreateTable)
	var execErr error
	if prepareErr == nil {
		_, execErr = statement.Exec(ctx, nil)
	}
	if statement != nil {
		execErr = errors.Join(execErr, statement.Close())
	}
	execErr = errors.Join(prepareErr, execErr, session.Close())
	base, bindErr := database.BindReplicatedShardStoreStorageIdentity(
		target.SQL.Binding, template.Table, target.SQL.UserStorage,
	)
	if bindErr != nil {
		return closeWith(errors.Join(splitcontroller.ErrChildPreparation, execErr, bindErr))
	}
	if !base.Equal(target.SQL) {
		return closeWith(splitcontroller.ErrChildPreparation)
	}
	if err = database.ReserveReplicatedChildApply(base, target.Apply); err != nil {
		return closeWith(errors.Join(splitcontroller.ErrChildPreparation, err))
	}
	return closeWith(nil)
}

func verifyRF3PreparedChildSQL(
	path string, target splitcontroller.ChildReplicaTarget,
) error {
	database, err := sqldriver.OpenReplicatedShardStore(path, target.SQL)
	if err != nil {
		return err
	}
	reservation, present, reservationErr := database.ReplicatedChildApplyReservation(target.SQL)
	return errors.Join(func() error {
		if reservationErr != nil || !present || reservation != target.Apply {
			return errors.Join(splitcontroller.ErrChildPreparation, reservationErr)
		}
		return nil
	}(), database.Close())
}

func rf3ChildReceiptMatches(
	preparation splitcontroller.ChildPreparation,
	receipt splitcontroller.ChildPrepareReceipt,
) bool {
	digest, err := splitcontroller.ChildPreparationDigest(preparation)
	expected, expectedErr := splitcontroller.NewChildPrepareReceipt(preparation, receipt.Target)
	return err == nil && receipt.Operation == preparation.OperationID() &&
		receipt.AllocationDigest == preparation.AllocationDigest() &&
		receipt.Child == preparation.Child() && receipt.Replica == preparation.ReplicaIndex() &&
		receipt.RequestDigest == digest && expectedErr == nil &&
		expected.ReceiptDigest == receipt.ReceiptDigest
}

var _ splitcontroller.ChildPreparer = (*rf3ChildPreparer)(nil)
