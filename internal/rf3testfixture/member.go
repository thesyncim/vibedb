// Package rf3testfixture provides narrowly scoped durable-member and credential
// preparation for cross-package RF3 process tests. It is not a serving API.
package rf3testfixture

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// MemberOptions is the complete prepared-member input retained by serve-rf3.
type MemberOptions struct {
	Root        string
	Table       string
	CreateTable string
	Identity    raftstore.Identity
	Key         raftstore.Key
	WAL         raftstore.Options
	Bootstrap   raftstore.Bootstrap
	Authority   sqldriver.ReplicatedAuthorityProfile
	Apply       sqldriver.ReplicatedApplyOptions
}

// PreparedMember owns one open WAL/SQL/apply triple. Tests may either close it
// to leave exact retained artifacts for a command process, or transfer all
// three handles to raftmember.AdoptRuntime.
type PreparedMember struct {
	WAL           *raftstore.Store
	Database      *sqldriver.Database
	Apply         *sqldriver.ReplicatedApply
	Base          sqldriver.ReplicatedShardStoreIdentity
	ApplyIdentity sqldriver.ReplicatedApplyIdentity
	WALPath       string
	SQLPath       string
}

// PrepareMember creates and opens exactly one initial prepared member. It does
// not adopt a Raft runtime or mint a node incarnation.
func PrepareMember(options MemberOptions) (*PreparedMember, error) {
	if options.Root == "" || options.Table == "" || options.CreateTable == "" ||
		options.Bootstrap.Snapshot == nil {
		return nil, errors.New("rf3 test fixture: invalid prepared member")
	}
	walPath := filepath.Join(options.Root, "member.wal")
	sqlPath := filepath.Join(options.Root, "member.vdb")
	wal, err := raftstore.Create(
		walPath, options.Identity, options.Key, options.Bootstrap, options.WAL,
	)
	if err != nil {
		return nil, err
	}
	database, err := sqldriver.InitializeShardStore(sqlPath, sqldriver.ShardStoreBinding{
		Distribution: distribution.DistributionName(options.Identity.Distribution),
		Shard:        distribution.ShardID(options.Identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(
			options.Identity.AllocationGeneration,
		),
	})
	if err != nil {
		return nil, errors.Join(err, wal.Close())
	}
	closeBoth := func(cause error) (*PreparedMember, error) {
		return nil, errors.Join(cause, database.Close(), wal.Close())
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		return closeBoth(err)
	}
	statement, err := session.Prepare(context.Background(), options.CreateTable)
	if err == nil {
		_, err = statement.Exec(context.Background(), nil)
	}
	if statement != nil {
		err = errors.Join(err, statement.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		return closeBoth(err)
	}
	base, err := raftmember.BindPreparedSQL(
		wal, database, options.Authority, options.Table,
	)
	if err != nil {
		return closeBoth(err)
	}
	apply, applyIdentity, err := raftmember.OpenPreparedApply(
		wal, database, options.Authority, base, options.Apply,
	)
	if err != nil {
		return closeBoth(err)
	}
	bootstrap, err := wal.Snapshot()
	if err == nil {
		_, err = apply.InstallSnapshot(bootstrap)
	}
	if err != nil {
		return nil, errors.Join(err, apply.Close(), database.Close(), wal.Close())
	}
	return &PreparedMember{
		WAL: wal, Database: database, Apply: apply,
		Base: base, ApplyIdentity: applyIdentity,
		WALPath: walPath, SQLPath: sqlPath,
	}, nil
}

// Close retains the prepared files while releasing every live claim.
func (member *PreparedMember) Close() error {
	if member == nil {
		return nil
	}
	var applyErr, databaseErr, walErr error
	if member.Apply != nil {
		applyErr = member.Apply.Close()
	}
	if member.Database != nil {
		databaseErr = member.Database.Close()
	}
	if member.WAL != nil {
		walErr = member.WAL.Close()
	}
	member.Apply, member.Database, member.WAL = nil, nil, nil
	return errors.Join(applyErr, databaseErr, walErr)
}

// InitialBootstrap returns the canonical index-one stable-voter base used by
// the command process gate.
func InitialBootstrap(voters []uint64) raftstore.Bootstrap {
	index, term := uint64(1), uint64(1)
	return raftstore.Bootstrap{TopologyRecoveryEpoch: 3, Snapshot: &pb.Snapshot{
		Data: []byte("rf3-command-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: append([]uint64(nil), voters...)},
		},
	}}
}
