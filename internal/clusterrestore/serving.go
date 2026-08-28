package clusterrestore

import (
	"context"
	"errors"
	"os"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// Activation is a completed local installation whose expected catalog
// witness is retained privately. It still grants no serving authority.
type Activation struct {
	Permit    ServingPermit
	operation Operation
	roots     []RootWitness
	catalog   CatalogWitness
}

// ActivateComplete installs or resumes every group and returns a non-serving
// activation handle. AuthorizeServing still requires a separately observed
// catalog witness, normally the result of a linearizable target-catalog read.
func ActivateComplete(ctx context.Context, options Options) (Activation, error) {
	permit, err := Activate(ctx, options)
	if err != nil {
		return Activation{}, err
	}
	if !privateActivationRoot(options.Root) {
		return Activation{}, ErrActivation
	}
	root, err := os.OpenRoot(options.Root)
	if err != nil {
		return Activation{}, err
	}
	defer root.Close()
	progress, err := readProgress(root, options.Operation)
	if err != nil || len(progress.Roots) != len(options.Operation.Targets) {
		return Activation{}, errors.Join(ErrActivation, err)
	}
	catalog := makeCatalogWitness(options.Operation, progress.Roots)
	if progress.Catalog != catalog.CatalogDigest {
		return Activation{}, ErrActivation
	}
	return Activation{Permit: permit, operation: options.Operation,
		roots: append([]RootWitness(nil), progress.Roots...), catalog: catalog}, nil
}

// AuthorizeServing requires the exact witness observed from the replicated
// target catalog. The locally persisted activation journal is insufficient.
func (activation Activation) AuthorizeServing(observed CatalogWitness) (*ServingAuthority, error) {
	if observed != activation.catalog {
		return nil, ErrActivation
	}
	return NewServingAuthority(activation.operation, activation.roots, observed, activation.Permit)
}

// ServingAuthority is the immutable result of validating the catalog's
// activation witness against every prepared RF3 root. It cannot be created
// from the local serving.permit file alone.
type ServingAuthority struct {
	operation [32]byte
	catalog   [32]byte
	groups    []raftmember.GroupKey
	replicas  map[raftmember.GroupKey][3]ReplicaIdentity
}

// NewServingAuthority consumes a catalog-observed witness and the complete
// prepared-root vector. Callers must obtain catalog from a linearizable read of
// the fresh target catalog group; local files are recovery evidence only.
func NewServingAuthority(operation Operation, roots []RootWitness, catalog CatalogWitness,
	permit ServingPermit,
) (*ServingAuthority, error) {
	if len(roots) != len(operation.Targets) || len(roots) == 0 {
		return nil, ErrActivation
	}
	raw, err := AppendOperation(nil, operation)
	if err != nil {
		return nil, err
	}
	opened, err := OpenOperation(raw)
	if err != nil || opened.Digest != operation.Digest {
		return nil, errors.Join(ErrActivation, err)
	}
	for ordinal, root := range roots {
		if !validRootWitness(opened, ordinal, root) {
			return nil, ErrActivation
		}
	}
	wantCatalog := makeCatalogWitness(opened, roots)
	wantPermit := makeServingPermit(opened, wantCatalog)
	if catalog != wantCatalog || permit != wantPermit {
		return nil, ErrActivation
	}
	authority := &ServingAuthority{operation: opened.Digest, catalog: catalog.CatalogDigest,
		groups:   make([]raftmember.GroupKey, 0, len(opened.Targets)),
		replicas: make(map[raftmember.GroupKey][3]ReplicaIdentity, len(opened.Targets))}
	for _, target := range opened.Targets {
		authority.groups = append(authority.groups, target.Group)
		authority.replicas[target.Group] = target.Replicas
	}
	return authority, nil
}

// AllowsReplica is the allocation-free shard serving predicate. It binds the
// exact fresh group, member, store, and node incarnation from the replicated
// activation operation.
func (authority *ServingAuthority) AllowsReplica(group raftmember.GroupKey, member uint64,
	store [16]byte, nodeIncarnation uint64,
) bool {
	if authority == nil || member == 0 || store == ([16]byte{}) || nodeIncarnation == 0 {
		return false
	}
	replicas, found := authority.replicas[group]
	if !found || member > uint64(len(replicas)) {
		return false
	}
	replica := replicas[member-1]
	return replica.Member == member && replica.Store == store &&
		replica.NodeIncarnation == nodeIncarnation
}

func (authority *ServingAuthority) Operation() [32]byte {
	if authority == nil {
		return [32]byte{}
	}
	return authority.operation
}
