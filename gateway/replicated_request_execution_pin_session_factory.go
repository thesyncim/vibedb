package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const durableExecutionPinSessionStripes = 4096

func scopedNativeSessionJournalBinding(binding replication.Digest) replication.Digest {
	var raw [33]byte
	raw[0] = 1
	copy(raw[1:], binding[:])
	return sha256.Sum256(raw[:])
}

func executionPinJournalPath(base string) (path string, scoped, present bool, err error) {
	legacy, err := NativeSessionJournalPresent(base)
	if err != nil {
		return "", false, false, err
	}
	modern, err := NativeSessionJournalPresent(base + ".scoped")
	if err != nil || legacy && modern {
		return "", false, false, errors.Join(err, ErrNativeSessionJournal)
	}
	if legacy {
		return base, false, true, nil
	}
	return base + ".scoped", true, modern, nil
}

// JournaledDurableRequestExecutionPinSessionFactory opens one compact exact-
// retry journal per request/controller principal. A fixed striped latch keeps
// memory constant while preventing two goroutines in one gateway from driving
// the same native session sequence concurrently.
type JournaledDurableRequestExecutionPinSessionFactory struct {
	executor  *ReplicatedExecutor
	directory string
	principal serviceauthz.Authority
	stripes   [durableExecutionPinSessionStripes]sync.Mutex
}

func NewJournaledDurableRequestExecutionPinSessionFactory(
	executor *ReplicatedExecutor,
	directory string,
	principal serviceauthz.Authority,
) (*JournaledDurableRequestExecutionPinSessionFactory, error) {
	if executor == nil || directory == "" || !principal.Valid() {
		return nil, ErrDurableRequest
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	return &JournaledDurableRequestExecutionPinSessionFactory{
		executor: executor, directory: filepath.Clean(directory), principal: principal,
	}, nil
}

func (factory *JournaledDurableRequestExecutionPinSessionFactory) OpenExecutionPinSession(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
	route ReplicatedRoute,
) (*NativeSession, serviceauthz.Authority, func(), error) {
	return factory.openExecutionPinSession(ctx, execution, route, false)
}

func (factory *JournaledDurableRequestExecutionPinSessionFactory) openExecutionPinSession(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
	route ReplicatedRoute,
	terminal bool,
) (*NativeSession, serviceauthz.Authority, func(), error) {
	if factory == nil || factory.executor == nil || ctx == nil || !validReplicatedRoute(route) ||
		!factory.principal.Valid() || !sameReplicatedCatalogRoute(route, execution.Home.borrowedRoute()) {
		return nil, serviceauthz.Authority{}, nil, ErrDurableRequest
	}
	binding, err := BuildDurableRequestExecutionPinBinding(execution)
	pin, pinErr := executionpin.DerivePinID(binding)
	if err != nil || pinErr != nil {
		return nil, serviceauthz.Authority{}, nil, errors.Join(err, pinErr, ErrDurableRequestConflict)
	}
	identity := durableExecutionPinSessionIdentity(pin, factory.principal)
	stripe := &factory.stripes[durableExecutionPinSessionStripe(identity)]
	stripe.Lock()
	released := false
	release := func() {
		if !released {
			released = true
			stripe.Unlock()
		}
	}

	resolver := BaseRelationResolver{Relation: 1}
	journalBinding, err := NativeSessionJournalBinding(
		route, string(route.Distribution), string(route.Shard), execution.Recipe.Tenant,
		resolver.Relation, serviceauthz.CapabilityExecutionPin,
	)
	if err != nil {
		release()
		return nil, serviceauthz.Authority{}, nil, err
	}
	journalPath, scoped, present, err := executionPinJournalPath(filepath.Join(factory.directory, hex.EncodeToString(identity[:])))
	if err != nil {
		release()
		return nil, serviceauthz.Authority{}, nil, err
	}
	if terminal && !present {
		// Check under the same identity latch as deletion. Concurrent terminal
		// replay must not recreate a session that another cleanup just removed.
		release()
		return nil, factory.principal, nil, nil
	}
	if scoped {
		journalBinding = scopedNativeSessionJournalBinding(journalBinding)
	}
	journal, err := OpenNativeSessionJournal(NativeSessionJournalOptions{
		Path:     journalPath,
		ClientID: identity, RetryHome: execution.Recipe.Identity.RetryHome,
		MaxCommandBytes: replication.MaxCommandBytes, Binding: journalBinding,
	})
	if err != nil {
		release()
		return nil, serviceauthz.Authority{}, nil, err
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: factory.executor, Route: route,
		Distribution: string(route.Distribution), Shard: string(route.Shard),
		Tenant: execution.Recipe.Tenant, ClientID: identity,
		RetryHome: execution.Recipe.Identity.RetryHome, Resolver: resolver,
		Journal: journal, ProposalCapability: serviceauthz.CapabilityExecutionPin,
		ScopedCoordination: scoped,
		MaxRelationBatches: 1, MaxMutations: 1,
		InitialCommandBytes: replication.MaxExecutionPinCommandBytes,
		MaxCommandBytes:     replication.MaxCommandBytes,
	})
	if err != nil {
		release()
		return nil, serviceauthz.Authority{}, nil, err
	}
	if session.phase == nativeSessionNew && !session.pending {
		authorized, authErr := serviceauthz.WithAuthority(ctx, factory.principal)
		if authErr != nil {
			release()
			return nil, serviceauthz.Authority{}, nil, authErr
		}
		if _, openErr := session.Open(authorized, math.MaxInt64); openErr != nil && !session.pending {
			release()
			return nil, serviceauthz.Authority{}, nil, openErr
		}
	}
	if session.pending {
		pending, pendingErr := replication.OpenCommand(session.command)
		if pendingErr != nil {
			release()
			return nil, serviceauthz.Authority{}, nil, pendingErr
		}
		if pending.Kind() == replication.CommandSessionOpen {
			authorized, authErr := serviceauthz.WithAuthority(ctx, factory.principal)
			if authErr != nil {
				release()
				return nil, serviceauthz.Authority{}, nil, authErr
			}
			if _, retryErr := session.RetryPending(authorized); retryErr != nil {
				release()
				return nil, serviceauthz.Authority{}, nil, retryErr
			}
		}
	}
	terminalPhase := terminal && (session.phase == nativeSessionRetired || session.phase == nativeSessionReleased)
	if session.phase != nativeSessionActive && !session.pending && !terminalPhase {
		release()
		return nil, serviceauthz.Authority{}, nil, ErrDurableRequestConflict
	}
	return session, factory.principal, release, nil
}

func (factory *JournaledDurableRequestExecutionPinSessionFactory) RetireTerminalExecutionPinSession(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
	route ReplicatedRoute,
) error {
	if factory == nil || ctx == nil {
		return ErrDurableRequest
	}
	session, principal, release, err := factory.openExecutionPinSession(ctx, execution, route, true)
	if release != nil {
		defer release()
	}
	if err != nil {
		return err
	}
	if session == nil {
		return nil
	}
	authorized, err := serviceauthz.WithAuthority(ctx, principal)
	if err != nil {
		return err
	}
	if session.pending {
		if _, err = session.RetryPending(authorized); err != nil {
			return err
		}
	}
	if session.phase == nativeSessionActive {
		if _, err = session.Retire(authorized); err != nil {
			return err
		}
	}
	if session.phase == nativeSessionRetired {
		if _, err = session.Release(authorized); err != nil {
			return err
		}
	}
	if session.phase != nativeSessionReleased || session.pending || session.journal == nil {
		return ErrDurableRequestUnresolved
	}
	return session.journal.destroyReleased()
}

// RetireAcknowledgedExecutionPinSession durably removes a stale local session
// after replicated ACK state proves the corresponding global pin release. It
// is intentionally path-only: ACK collection may already have reclaimed the
// recipe bytes needed to reopen the native session.
func (factory *JournaledDurableRequestExecutionPinSessionFactory) RetireAcknowledgedExecutionPinSession(
	ctx context.Context,
	pin executionpin.PinID,
	route ReplicatedRoute,
	releaseCertificate replication.Digest,
) error {
	if factory == nil || ctx == nil || ctx.Err() != nil || pin == (executionpin.PinID{}) ||
		releaseCertificate == (replication.Digest{}) || !validReplicatedRoute(route) ||
		!factory.principal.Valid() {
		return ErrDurableRequest
	}
	identity := durableExecutionPinSessionIdentity(pin, factory.principal)
	stripe := &factory.stripes[durableExecutionPinSessionStripe(identity)]
	stripe.Lock()
	defer stripe.Unlock()
	base, _, _, err := executionPinJournalPath(filepath.Join(factory.directory, hex.EncodeToString(identity[:])))
	if err != nil {
		return err
	}
	var joined error
	for slot := 0; slot < 2; slot++ {
		if err := os.Remove(base + "." + string(rune('0'+slot))); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			joined = errors.Join(joined, err)
		}
	}
	directory, err := os.Open(factory.directory)
	if err == nil {
		joined = errors.Join(joined, directory.Sync(), directory.Close())
	} else {
		joined = errors.Join(joined, err)
	}
	return joined
}

func durableExecutionPinSessionStripe(identity replication.ID128) uint16 {
	return binary.LittleEndian.Uint16(identity[:2]) & (durableExecutionPinSessionStripes - 1)
}

func durableExecutionPinSessionIdentity(
	pin executionpin.PinID,
	principal serviceauthz.Authority,
) replication.ID128 {
	const domain = "vibedb/durable-request/execution-pin-session/1\x00"
	var material [len(domain) + len(pin) + len(principal.Node) + 8]byte
	cursor := copy(material[:], domain)
	cursor += copy(material[cursor:], pin[:])
	cursor += copy(material[cursor:], principal.Node[:])
	binary.LittleEndian.PutUint64(material[cursor:], principal.Generation)
	digest := sha256.Sum256(material[:])
	var identity replication.ID128
	copy(identity[:], digest[:])
	return identity
}

var _ DurableRequestExecutionPinSessionFactory = (*JournaledDurableRequestExecutionPinSessionFactory)(nil)
