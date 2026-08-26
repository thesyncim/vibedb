package schemainstall

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"sync"
)

var installationDomain = [...]byte{
	'V', 'B', 'S', 'C', 'H', 'E', 'M', 'A', '-', 'I', 'N', 'S', 'T', 'A', 'L', 'L',
	'A', 'T', 'I', 'O', 'N', '-', 'R', 'E', 'C', 'E', 'I', 'P', 'T', 0, 1, 0,
}

// Backend owns the physical generation. Prepare must leave an immutable,
// reopenable artifact but must not change serving state. Activate must make the
// exact prepared generation visible atomically. Every mutation is idempotent;
// Observe methods settle a crash or an outcome-unknown return without repeating
// an irreversible side effect. DrainOld may reclaim only the old generation.
type Backend interface {
	ObservePrepared(context.Context, Request) (artifactDigest [32]byte, found bool, err error)
	Prepare(context.Context, Request, []byte) (artifactDigest [32]byte, err error)
	ObserveActive(context.Context, Request, Authorization, [32]byte) (bool, error)
	Activate(context.Context, Request, Authorization, [32]byte) error
	ObserveDrained(context.Context, Request, Authorization, DrainProof, [32]byte) (bool, error)
	DrainOld(context.Context, Request, Authorization, DrainProof, [32]byte) error
}

type Options struct {
	Journal       Journal
	Backend       Backend
	MaxConcurrent int
}

type Installer struct {
	journal Journal
	backend Backend
	slots   chan struct{}
	stripes []sync.Mutex
}

func New(options Options) (*Installer, error) {
	if options.Journal == nil || options.Backend == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > 256 {
		return nil, ErrInvalid
	}
	return &Installer{journal: options.Journal, backend: options.Backend,
		slots:   make(chan struct{}, options.MaxConcurrent),
		stripes: make([]sync.Mutex, options.MaxConcurrent)}, nil
}

func (installer *Installer) withOperation(
	ctx context.Context, operation [32]byte, run func() error,
) error {
	if installer == nil || ctx == nil || operation == ([32]byte{}) {
		return ErrInvalid
	}
	select {
	case installer.slots <- struct{}{}:
		defer func() { <-installer.slots }()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	stripe := &installer.stripes[binary.LittleEndian.Uint64(operation[:8])%uint64(len(installer.stripes))]
	stripe.Lock()
	defer stripe.Unlock()
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return run()
}

func (installer *Installer) Prepare(ctx context.Context, request Request, bundle []byte) (Receipt, error) {
	if !validRequest(request) || uint64(len(bundle)) != request.BundleBytes ||
		sha256.Sum256(bundle) != request.BundleDigest {
		return Receipt{}, ErrInvalid
	}
	var receipt Receipt
	err := installer.withOperation(ctx, request.Operation, func() error {
		current, err := installer.journal.Read(ctx, request.Operation)
		if err == nil {
			if current.Request != request {
				return ErrConflict
			}
			receipt = receiptFor(current)
			return nil
		}
		if !errors.Is(err, ErrMissing) {
			return err
		}
		artifactDigest, found, err := installer.backend.ObservePrepared(ctx, request)
		if err != nil {
			return err
		}
		if !found {
			artifactDigest, err = installer.backend.Prepare(ctx, request, bundle)
			if err != nil {
				observed, observedFound, observeErr := installer.backend.ObservePrepared(ctx, request)
				if observeErr != nil || !observedFound {
					return errors.Join(ErrOutcomeUnknown, err, observeErr)
				}
				artifactDigest = observed
			}
		}
		if artifactDigest == ([32]byte{}) {
			return ErrInvalid
		}
		record := Record{Request: request, Revision: 1, State: StatePrepared,
			Installation: InstallationDigest(request, artifactDigest)}
		if err = installer.journal.Publish(ctx, 0, record); err != nil {
			settled, readErr := installer.journal.Read(ctx, request.Operation)
			if readErr != nil || settled != record {
				return errors.Join(ErrOutcomeUnknown, err, readErr)
			}
		}
		receipt = receiptFor(record)
		return nil
	})
	return receipt, err
}

func (installer *Installer) Authorize(ctx context.Context, authorization Authorization) (Record, error) {
	if !validAuthorization(authorization, authorization.Operation) {
		return Record{}, ErrInvalid
	}
	var result Record
	err := installer.withOperation(ctx, authorization.Operation, func() error {
		current, err := installer.journal.Read(ctx, authorization.Operation)
		if err != nil {
			return err
		}
		if current.State != StatePrepared {
			if current.Authorization != authorization {
				return ErrConflict
			}
			result = current
			return nil
		}
		next := current
		next.Revision++
		next.State = StateAuthorized
		next.Authorization = authorization
		if err = installer.journal.Publish(ctx, current.Revision, next); err != nil {
			settled, readErr := installer.journal.Read(ctx, authorization.Operation)
			if readErr != nil || settled != next {
				return errors.Join(ErrOutcomeUnknown, err, readErr)
			}
		}
		result = next
		return nil
	})
	return result, err
}

func (installer *Installer) Activate(ctx context.Context, authorization Authorization) (Record, error) {
	if !validAuthorization(authorization, authorization.Operation) {
		return Record{}, ErrInvalid
	}
	var result Record
	err := installer.withOperation(ctx, authorization.Operation, func() error {
		current, err := installer.journal.Read(ctx, authorization.Operation)
		if err != nil {
			return err
		}
		if current.Authorization != authorization || current.State == StatePrepared {
			return ErrConflict
		}
		if current.State >= StateActive {
			result = current
			return nil
		}
		active, err := installer.backend.ObserveActive(ctx, current.Request, authorization, current.Installation)
		if err != nil {
			return err
		}
		if !active {
			err = installer.backend.Activate(ctx, current.Request, authorization, current.Installation)
			if err != nil {
				active, observeErr := installer.backend.ObserveActive(ctx, current.Request, authorization, current.Installation)
				if observeErr != nil || !active {
					return errors.Join(ErrOutcomeUnknown, err, observeErr)
				}
			}
		}
		next := current
		next.Revision++
		next.State = StateActive
		if err = installer.journal.Publish(ctx, current.Revision, next); err != nil {
			settled, readErr := installer.journal.Read(ctx, authorization.Operation)
			if readErr != nil || settled != next {
				return errors.Join(ErrOutcomeUnknown, err, readErr)
			}
		}
		result = next
		return nil
	})
	return result, err
}

func (installer *Installer) Drain(ctx context.Context, authorization Authorization, proof DrainProof) (Record, error) {
	if !validAuthorization(authorization, authorization.Operation) || !validDrainProof(proof, authorization) {
		return Record{}, ErrInvalid
	}
	var result Record
	err := installer.withOperation(ctx, authorization.Operation, func() error {
		current, err := installer.journal.Read(ctx, authorization.Operation)
		if err != nil {
			return err
		}
		if current.Authorization != authorization || current.State < StateActive {
			return ErrConflict
		}
		if current.State == StateDrained {
			result = current
			return nil
		}
		drained, err := installer.backend.ObserveDrained(ctx, current.Request, authorization, proof, current.Installation)
		if err != nil {
			return err
		}
		if !drained {
			err = installer.backend.DrainOld(ctx, current.Request, authorization, proof, current.Installation)
			if err != nil {
				drained, observeErr := installer.backend.ObserveDrained(ctx, current.Request, authorization, proof, current.Installation)
				if observeErr != nil || !drained {
					return errors.Join(ErrOutcomeUnknown, err, observeErr)
				}
			}
		}
		next := current
		next.Revision++
		next.State = StateDrained
		next.DrainProof = proof
		if err = installer.journal.Publish(ctx, current.Revision, next); err != nil {
			settled, readErr := installer.journal.Read(ctx, authorization.Operation)
			if readErr != nil || settled != next {
				return errors.Join(ErrOutcomeUnknown, err, readErr)
			}
		}
		result = next
		return nil
	})
	return result, err
}

func (installer *Installer) Read(ctx context.Context, operation [32]byte) (Record, error) {
	if installer == nil {
		return Record{}, ErrInvalid
	}
	return installer.journal.Read(ctx, operation)
}

func InstallationDigest(request Request, artifactDigest [32]byte) [32]byte {
	if !validRequest(request) || artifactDigest == ([32]byte{}) {
		return [32]byte{}
	}
	hasher := sha256.New()
	_, _ = hasher.Write(installationDomain[:])
	writeRequestHash(hasher, request)
	_, _ = hasher.Write(artifactDigest[:])
	var digest [32]byte
	hasher.Sum(digest[:0])
	return digest
}

func writeRequestHash(hasher hash.Hash, request Request) {
	_, _ = hasher.Write(request.Operation[:])
	_, _ = hasher.Write(request.Group.ClusterID[:])
	_, _ = hasher.Write(request.Group.ClusterIncarnation[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], request.Group.TopologyRecoveryEpoch)
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(request.Group.ShardIncarnation[:])
	_, _ = hasher.Write(request.Group.GroupID[:])
	for _, value := range [...]uint64{uint64(request.AllocationGeneration), request.FromSchemaGeneration, request.ToSchemaGeneration, request.BundleBytes} {
		binary.BigEndian.PutUint64(scalar[:], value)
		_, _ = hasher.Write(scalar[:])
	}
	_, _ = hasher.Write(request.FromRelationManifestDigest[:])
	_, _ = hasher.Write(request.ToRelationManifestDigest[:])
	_, _ = hasher.Write(request.ApplyContractDigest[:])
	_, _ = hasher.Write(request.BundleDigest[:])
}
