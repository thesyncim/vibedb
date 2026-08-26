package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

const (
	// Target is a bounded physical-wave address/certificate envelope. It never
	// contains mutation bytes; MaxCommandBytes is a conservative closed wire
	// ceiling until the route-target codec exports its smaller exact maximum.
	MaxDurableRequestPendingTargetBytes = replication.MaxCommandBytes
	durableRequestStreamSlotBytes       = durableRequestReaderMaxLiveBytes +
		MaxDurableRequestPendingTargetBytes + replication.MaxCommandBytes +
		requestledger.MaxContinuationRecordBytes + requestledger.MaxTerminalResultBytes
	durableRequestStreamBudgetBytes = 256 << 20
)

var durableRequestStreamSlots = make(
	chan struct{}, durableRequestStreamBudgetBytes/durableRequestStreamSlotBytes,
)

func acquireDurableRequestStream(ctx context.Context) (func(), error) {
	select {
	case durableRequestStreamSlots <- struct{}{}:
		return func() { <-durableRequestStreamSlots }, nil
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), ErrDurableRequestUnavailable)
	}
}

func (executor *DurableRequestExecutor) Execute(
	ctx context.Context,
	request DurableRequest,
) (DurableRequestOutcome, error) {
	program := request.Program
	if executor == nil || executor.ledger == nil || executor.runner == nil ||
		!validDurableRequestLogicalProgram(program) {
		return DurableRequestOutcome{}, ErrDurableRequest
	}
	key, _, err := durableRequestLedgerKey(
		ctx, executor.localPrincipal, program.Tenant, program.RequestID, program.RequestDigest,
	)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	releaseStream, acquireErr := acquireDurableRequestStream(ctx)
	if acquireErr != nil {
		return DurableRequestOutcome{}, acquireErr
	}
	defer releaseStream()
	measurement, measureErr := measureDurableRequestPlan(key, program)
	if measureErr != nil {
		return DurableRequestOutcome{}, measureErr
	}
	descriptor := measurement.descriptor()
	home, _, entry, err := executor.lookupRefreshable(ctx, key)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	if entry.State == DurableRequestLedgerAbsent || entry.State == DurableRequestLedgerCreating {
		entry, err = executor.createAndSeal(ctx, &home, key, measurement, program)
		if err != nil {
			return DurableRequestOutcome{}, err
		}
	} else if !matchesDurableRequestEntryPlanIdentity(entry, descriptor) {
		// The public request identity is never sufficient on its own. Reusing an
		// ID with a forged old digest but different lowered mutations must not
		// replay an unrelated terminal result.
		return DurableRequestOutcome{}, ErrDurableRequestConflict
	}
	return executor.drive(ctx, home, key, entry, true)
}

// Replay consults the ledger before catalog pinning or SQL lowering. found is
// false only when no durable identity exists at the selected stable home.
func (executor *DurableRequestExecutor) Replay(
	ctx context.Context,
	tenant []byte,
	requestID replication.ID128,
	digest replication.Digest,
) (outcome DurableRequestOutcome, found bool, err error) {
	if executor == nil || executor.ledger == nil || executor.runner == nil {
		return outcome, false, ErrDurableRequest
	}
	key, _, err := durableRequestLedgerKey(
		ctx, executor.localPrincipal, tenant, requestID, digest,
	)
	if err != nil {
		return outcome, false, err
	}
	releaseStream, acquireErr := acquireDurableRequestStream(ctx)
	if acquireErr != nil {
		return outcome, false, acquireErr
	}
	defer releaseStream()
	home, _, entry, err := executor.lookupRefreshable(ctx, key)
	if err != nil {
		return outcome, false, err
	}
	if entry.State == DurableRequestLedgerAbsent {
		return outcome, false, nil
	}
	outcome, err = executor.drive(ctx, home, key, entry, true)
	return outcome, true, err
}

// Acknowledge changes a terminal entry into a durable tombstone. It never
// deletes to absent: delayed retries therefore cannot execute again.
func (executor *DurableRequestExecutor) Acknowledge(
	ctx context.Context,
	tenant []byte,
	requestID replication.ID128,
	digest replication.Digest,
	token DurableRequestAckToken,
) error {
	if executor == nil || executor.ledger == nil {
		return ErrDurableRequest
	}
	if token == (DurableRequestAckToken{}) {
		return ErrDurableRequestConflict
	}
	key, _, err := durableRequestLedgerKey(
		ctx, executor.localPrincipal, tenant, requestID, digest,
	)
	if err != nil {
		return err
	}
	home, _, entry, err := executor.lookupRefreshable(ctx, key)
	if err != nil {
		return err
	}
	if entry.State == DurableRequestLedgerAbsent {
		return ErrDurableRequestUnresolved
	}
	if entry.Digest != digest {
		return ErrDurableRequestConflict
	}
	if entry.State == DurableRequestLedgerAcked {
		if entry.AckDigest == (replication.Digest{}) || entry.AckTerminalRevision == 0 ||
			entry.AckResultDigest == (replication.Digest{}) ||
			entry.AckTokenDigest == (replication.Digest{}) ||
			entry.AckTokenDigest != durableRequestAckTokenDigest(token) {
			return ErrDurableRequestConflict
		}
		return nil
	}
	if entry.State != DurableRequestLedgerTerminal {
		return ErrDurableRequestUnresolved
	}
	terminalRevision := entry.Revision
	resultDigest := replication.Digest(requestledger.ResultDigest(entry.Terminal.Result))
	if entry.AckToken == (DurableRequestAckToken{}) || token != entry.AckToken {
		return ErrDurableRequestConflict
	}
	entry, err = executor.entryWithHomeRetry(&home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
		return executor.ledger.Acknowledge(
			ctx, current, key, entry.Revision, terminalRevision, resultDigest, token,
		)
	})
	if err != nil {
		entry, err = executor.lookupWithHomeRetry(ctx, &home, key)
	}
	if err != nil || entry.State != DurableRequestLedgerAcked ||
		entry.Digest != digest || entry.AckDigest == (replication.Digest{}) ||
		entry.AckTerminalRevision != terminalRevision || entry.AckResultDigest != resultDigest ||
		entry.AckTokenDigest != durableRequestAckTokenDigest(token) {
		return errors.Join(err, ErrDurableRequestUnresolved)
	}
	return nil
}

func (executor *DurableRequestExecutor) lookupRefreshable(
	ctx context.Context,
	key DurableRequestLedgerKey,
) (DurableRequestLedgerHome, uint64, DurableRequestLedgerEntry, error) {
	point, pointErr := requestledger.Home(key.RequestKey)
	if pointErr != nil {
		return DurableRequestLedgerHome{}, 0, DurableRequestLedgerEntry{},
			errors.Join(pointErr, ErrDurableRequest)
	}
	home, _, ok := executor.topology.Lookup(point)
	if !ok {
		return DurableRequestLedgerHome{}, 0, DurableRequestLedgerEntry{}, ErrDurableRequestUnavailable
	}
	entry, err := executor.lookupWithHomeRetry(ctx, &home, key)
	return home, home.TopologyGeneration, entry, err
}

func (executor *DurableRequestExecutor) refreshDurableRequestHome(
	home DurableRequestLedgerHome,
) (DurableRequestLedgerHome, error) {
	next, generation, ok := executor.topology.Lookup(home.Point)
	if !ok || generation <= home.TopologyGeneration || next.Identity != home.Identity {
		return DurableRequestLedgerHome{}, ErrDurableRequestUnavailable
	}
	return next, nil
}

// entryWithHomeRetry retries one byte-identical lifecycle operation only after
// an exact stale-home pre-admission result. Ambiguous errors are not retried;
// their caller performs a linearizable lookup first. The range identity must
// remain unchanged, so a refresh cannot create a second ledger authority.
func (executor *DurableRequestExecutor) entryWithHomeRetry(
	home *DurableRequestLedgerHome,
	operation func(DurableRequestLedgerHome) (DurableRequestLedgerEntry, error),
) (DurableRequestLedgerEntry, error) {
	entry, err := operation(*home)
	if !errors.Is(err, ErrDurableRequestStaleHome) {
		return entry, err
	}
	next, refreshErr := executor.refreshDurableRequestHome(*home)
	if refreshErr != nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, refreshErr)
	}
	*home = next
	return operation(next)
}

func (executor *DurableRequestExecutor) lookupWithHomeRetry(
	ctx context.Context,
	home *DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
) (DurableRequestLedgerEntry, error) {
	return executor.entryWithHomeRetry(home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
		return executor.ledger.Lookup(ctx, current, key)
	})
}

func (executor *DurableRequestExecutor) loadPageWithHomeRetry(
	ctx context.Context,
	home *DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	page uint32,
) ([]byte, error) {
	value, err := executor.ledger.LoadPlanPage(ctx, *home, key, page)
	if !errors.Is(err, ErrDurableRequestStaleHome) {
		return value, err
	}
	next, refreshErr := executor.refreshDurableRequestHome(*home)
	if refreshErr != nil {
		return nil, errors.Join(err, refreshErr)
	}
	*home = next
	return executor.ledger.LoadPlanPage(ctx, next, key, page)
}

func (executor *DurableRequestExecutor) createAndSeal(
	ctx context.Context,
	home *DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	measurement durableRequestPlanMeasurement,
	program DurableRequestLogicalProgram,
) (DurableRequestLedgerEntry, error) {
	descriptor := measurement.descriptor()
	if len(descriptor.Inline) != 0 {
		entry, err := executor.entryWithHomeRetry(home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
			return executor.ledger.CreateSealed(
				ctx, current, key, current.TopologyGeneration, descriptor,
			)
		})
		if errors.Is(err, ErrDurableRequestCapacity) {
			return DurableRequestLedgerEntry{}, err
		}
		if err != nil {
			entry, err = executor.lookupWithHomeRetry(ctx, home, key)
		}
		if err != nil || !validDurableRequestSealedEntry(entry, key, descriptor) {
			return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
		}
		return entry, nil
	}
	entry, err := executor.entryWithHomeRetry(home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
		return executor.ledger.CreatePlanning(
			ctx, current, key, current.TopologyGeneration, descriptor,
		)
	})
	if errors.Is(err, ErrDurableRequestCapacity) {
		return DurableRequestLedgerEntry{}, err
	}
	if err != nil {
		entry, err = executor.lookupWithHomeRetry(ctx, home, key)
	}
	if err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	if entry.Digest != key.Digest {
		return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
	}
	if entry.State >= DurableRequestLedgerSealed {
		if !validDurableRequestSealedEntry(entry, key, descriptor) {
			return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
		}
		return entry, nil
	}
	if entry.State != DurableRequestLedgerCreating ||
		!equalDurableRequestDescriptor(entry.Plan, descriptor) {
		return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
	}
	if entry.AppendedPageCount > descriptor.PageCount {
		return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
	}
	resumePage := entry.AppendedPageCount
	err = streamDurableRequestPlan(
		measurement, program,
		durableRequestPlanPageSinkFunc(func(page uint32, raw []byte) error {
			if page < resumePage {
				return nil
			}
			// Another gateway may have appended this exact page (and later
			// pages) while our prior response was lost. Prove the stored bytes
			// before adopting its monotone-ahead revision; never rewrite or
			// treat state alone as proof of the page.
			if page < entry.AppendedPageCount {
				stored, pageErr := executor.loadPageWithHomeRetry(ctx, home, key, page)
				if pageErr != nil || !bytes.Equal(stored, raw) ||
					entry.Digest != key.Digest ||
					!equalDurableRequestDescriptor(entry.Plan, descriptor) {
					return errors.Join(pageErr, ErrDurableRequestConflict)
				}
				if entry.State >= DurableRequestLedgerSealed {
					if !validDurableRequestSealedEntry(entry, key, descriptor) {
						return ErrDurableRequestConflict
					}
				} else if entry.State != DurableRequestLedgerCreating {
					return ErrDurableRequestConflict
				}
				return nil
			}
			if page != entry.AppendedPageCount || entry.State != DurableRequestLedgerCreating {
				return ErrDurableRequestConflict
			}
			seal := page+1 == descriptor.PageCount
			entry, err = executor.entryWithHomeRetry(home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
				return executor.ledger.AppendPlanPage(
					ctx, current, key, entry.Revision, page, raw, seal,
				)
			})
			if err != nil {
				entry, err = executor.lookupWithHomeRetry(ctx, home, key)
				if err != nil {
					return err
				}
				stored, pageErr := executor.loadPageWithHomeRetry(ctx, home, key, page)
				if pageErr != nil || !bytes.Equal(stored, raw) {
					return errors.Join(pageErr, ErrDurableRequestUnresolved)
				}
			}
			if entry.Digest != key.Digest ||
				!equalDurableRequestDescriptor(entry.Plan, descriptor) ||
				entry.AppendedPageCount < page+1 ||
				(entry.State == DurableRequestLedgerCreating &&
					entry.AppendedPageCount > descriptor.PageCount) ||
				(entry.State != DurableRequestLedgerCreating &&
					!validDurableRequestSealedEntry(entry, key, descriptor)) {
				return ErrDurableRequestConflict
			}
			return nil
		}),
	)
	if err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return entry, nil
}

func validDurableRequestSealedEntry(
	entry DurableRequestLedgerEntry,
	key DurableRequestLedgerKey,
	descriptor DurableRequestPlanDescriptor,
) bool {
	return entry.State >= DurableRequestLedgerSealed && entry.Digest == key.Digest &&
		equalDurableRequestDescriptor(entry.Plan, descriptor) &&
		entry.AppendedPageCount == descriptor.PageCount
}

func (executor *DurableRequestExecutor) drive(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	entry DurableRequestLedgerEntry,
	streamSlotHeld bool,
) (DurableRequestOutcome, error) {
	if entry.Digest != key.Digest {
		return DurableRequestOutcome{}, ErrDurableRequestConflict
	}
	if entry.State == DurableRequestLedgerAcked {
		return DurableRequestOutcome{Acknowledged: true}, ErrDurableRequestAcknowledged
	}
	if entry.State == DurableRequestLedgerTerminal {
		return durableRequestOutcome(entry)
	}
	if entry.State != DurableRequestLedgerSealed && entry.State != DurableRequestLedgerPending {
		return DurableRequestOutcome{}, ErrDurableRequestUnresolved
	}
	for {
		release := func() {}
		if !streamSlotHeld {
			var acquireErr error
			release, acquireErr = acquireDurableRequestStream(ctx)
			if acquireErr != nil {
				return DurableRequestOutcome{}, acquireErr
			}
		}
		source := durableRequestPlanPageSourceFunc(func(page uint32) ([]byte, error) {
			return executor.loadPageWithHomeRetry(ctx, &home, key, page)
		})
		reader, openErr := openDurableRequestRecipeStream(key, entry.Plan, source)
		if openErr != nil {
			release()
			return DurableRequestOutcome{}, openErr
		}
		if reader.RequestID != replication.ID128(key.RequestKey.Request) ||
			reader.RequestDigest != key.Digest || reader.KeyDigest != recipeKeyDigest(key) ||
			requestledger.Digest(sha256.Sum256(reader.Tenant)) != key.RequestKey.TenantDigest {
			release()
			return DurableRequestOutcome{}, ErrDurableRequestConflict
		}
		replay := DurableRequestRecipe{
			CatalogGeneration: reader.CatalogGeneration,
			Identity:          reader.Identity,
			Contract:          reader.Contract,
			Tenant:            bytes.Clone(reader.Tenant),
			KeyDigest:         reader.KeyDigest,
			RequestID:         reader.RequestID,
			RequestDigest:     reader.RequestDigest,
			ParticipantCount:  reader.ParticipantCount,
			ParticipantStream: reader,
		}
		if entry.State == DurableRequestLedgerPending {
			replay.Pending = cloneDurableRequestPending(entry.Pending)
		}
		replay.ResumeRevision = entry.Revision
		replay.Progress = bytes.Clone(entry.Progress)
		journal := &durableRequestJournal{
			executor: executor, home: home, key: key, entry: entry,
		}
		terminal, runErr := executor.runner.Run(ctx, replay, journal)
		streamErr := reader.Err()
		streamComplete := reader.Complete()
		release()
		home = journal.home
		if streamErr != nil {
			return DurableRequestOutcome{}, streamErr
		}
		if runErr == nil && !streamComplete {
			return DurableRequestOutcome{}, ErrDurableRequestConflict
		}
		if runErr != nil {
			after, lookupErr := executor.lookupWithHomeRetry(ctx, &home, key)
			if lookupErr != nil || after.Digest != key.Digest || after.Revision <= entry.Revision {
				return DurableRequestOutcome{}, errors.Join(runErr, lookupErr, ErrDurableRequestUnresolved)
			}
			if after.State == DurableRequestLedgerAcked {
				return DurableRequestOutcome{Acknowledged: true}, ErrDurableRequestAcknowledged
			}
			if after.State == DurableRequestLedgerTerminal {
				return durableRequestOutcome(after)
			}
			if after.State != DurableRequestLedgerSealed && after.State != DurableRequestLedgerPending {
				return DurableRequestOutcome{}, errors.Join(runErr, ErrDurableRequestUnresolved)
			}
			entry = after
			continue
		}
		if !validDurableRequestTerminalForRecipe(terminal, replay) {
			return DurableRequestOutcome{}, ErrDurableRequestConflict
		}
		entry = journal.entry
		if entry.State == DurableRequestLedgerPending {
			return DurableRequestOutcome{}, ErrDurableRequestUnresolved
		}
		entry, completeErr := executor.entryWithHomeRetry(&home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
			return executor.ledger.Complete(ctx, current, key, entry.Revision, terminal)
		})
		if completeErr != nil {
			entry, completeErr = executor.lookupWithHomeRetry(ctx, &home, key)
		}
		if completeErr != nil || entry.State != DurableRequestLedgerTerminal ||
			entry.Digest != key.Digest || !equalDurableRequestTerminal(entry.Terminal, terminal) {
			return DurableRequestOutcome{}, errors.Join(completeErr, ErrDurableRequestUnresolved)
		}
		return durableRequestOutcome(entry)
	}
}

func validDurableRequestTerminalForRecipe(
	terminal DurableRequestTerminal,
	recipe DurableRequestRecipe,
) bool {
	result, err := OpenDurableRequestResult(terminal.Result)
	if err != nil || result.Transaction != recipe.Identity.ID ||
		result.CatalogGeneration != recipe.Identity.CatalogGeneration ||
		result.ShardsFanned != recipe.Contract.ParticipantCount ||
		result.TerminalContractDigest != recipe.Contract.TerminalContractDigest ||
		result.RetirementWitnessDigest != recipe.Contract.RetirementWitnessDigest ||
		recipe.Contract.ResultGrammarDigest != durableRequestResultGrammarDigest() {
		return false
	}
	if result.Committed {
		return result.TransitionTag == recipe.Contract.CommitTransitionTag &&
			result.TerminalStateDigest == recipe.Contract.CommitTerminalStateDigest &&
			result.AffectedRows >= 0
	}
	return result.TransitionTag == recipe.Contract.AbortTransitionTag &&
		result.TerminalStateDigest == recipe.Contract.AbortTerminalStateDigest &&
		result.AffectedRows == 0
}

func recipeKeyDigest(key DurableRequestLedgerKey) replication.Digest {
	digest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		return replication.Digest{}
	}
	return replication.Digest(digest)
}

type durableRequestJournal struct {
	executor *DurableRequestExecutor
	home     DurableRequestLedgerHome
	key      DurableRequestLedgerKey
	entry    DurableRequestLedgerEntry
}

func (journal *durableRequestJournal) Stage(
	ctx context.Context,
	target []byte,
	command []byte,
) (uint64, error) {
	if journal == nil || journal.executor == nil || ctx == nil ||
		len(target) == 0 || len(target) > MaxDurableRequestPendingTargetBytes ||
		len(command) == 0 || len(command) > replication.MaxCommandBytes ||
		journal.entry.State != DurableRequestLedgerSealed {
		return 0, ErrDurableRequest
	}
	pending := DurableRequestPending{
		StepRevision: journal.entry.Revision + 1,
		Target:       bytes.Clone(target), Command: bytes.Clone(command),
	}
	entry, err := journal.executor.entryWithHomeRetry(&journal.home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
		return journal.executor.ledger.PutPending(
			ctx, current, journal.key, journal.entry.Revision, pending,
		)
	})
	if err != nil {
		entry, err = journal.executor.lookupWithHomeRetry(ctx, &journal.home, journal.key)
	}
	if err != nil || entry.State != DurableRequestLedgerPending ||
		entry.Digest != journal.key.Digest || !equalDurableRequestPending(entry.Pending, pending) {
		return 0, errors.Join(err, ErrDurableRequestUnresolved)
	}
	journal.entry = entry
	return pending.StepRevision, nil
}

func (journal *durableRequestJournal) Settle(
	ctx context.Context,
	stepRevision uint64,
	observed []byte,
) error {
	if journal == nil || journal.executor == nil || ctx == nil || len(observed) == 0 ||
		journal.entry.State != DurableRequestLedgerPending ||
		journal.entry.Pending.StepRevision != stepRevision {
		return ErrDurableRequest
	}
	entry, err := journal.executor.entryWithHomeRetry(&journal.home, func(current DurableRequestLedgerHome) (DurableRequestLedgerEntry, error) {
		return journal.executor.ledger.Advance(
			ctx, current, journal.key, journal.entry.Revision, stepRevision, observed,
		)
	})
	if err != nil {
		entry, err = journal.executor.lookupWithHomeRetry(ctx, &journal.home, journal.key)
	}
	wantDigest := replication.Digest(requestledger.ObservationDigest(observed))
	if err != nil || entry.State != DurableRequestLedgerSealed ||
		entry.Digest != journal.key.Digest || entry.SettledStepRevision != stepRevision ||
		entry.ProgressDigest != wantDigest || !bytes.Equal(entry.Progress, observed) {
		return errors.Join(err, ErrDurableRequestUnresolved)
	}
	journal.entry = entry
	return nil
}

func durableRequestPlanDescriptor(
	key DurableRequestLedgerKey,
	raw []byte,
) (DurableRequestPlanDescriptor, error) {
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		return DurableRequestPlanDescriptor{}, errors.Join(err, ErrDurableRequest)
	}
	root, err := requestledger.PlanRoot(keyDigest, raw)
	if err != nil {
		return DurableRequestPlanDescriptor{}, errors.Join(err, ErrDurableRequest)
	}
	descriptor := DurableRequestPlanDescriptor{
		TotalBytes: uint64(len(raw)), Root: replication.Digest(root),
	}
	if len(raw) <= DurableRequestInlineBytes {
		descriptor.Inline = bytes.Clone(raw)
		return descriptor, nil
	}
	descriptor.PageCount = uint32((len(raw) + DurableRequestPlanPageBytes - 1) / DurableRequestPlanPageBytes)
	return descriptor, nil
}

func durableRequestPlanRootMatches(
	key DurableRequestLedgerKey,
	raw []byte,
	want replication.Digest,
) bool {
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		return false
	}
	root, err := requestledger.PlanRoot(keyDigest, raw)
	return err == nil && replication.Digest(root) == want
}

func durableRequestOutcome(entry DurableRequestLedgerEntry) (DurableRequestOutcome, error) {
	if entry.State == DurableRequestLedgerAcked {
		return DurableRequestOutcome{Acknowledged: true}, ErrDurableRequestAcknowledged
	}
	result, err := OpenDurableRequestResult(entry.Terminal.Result)
	if err != nil {
		return DurableRequestOutcome{}, err
	}
	if entry.AckToken == (DurableRequestAckToken{}) {
		return DurableRequestOutcome{}, ErrDurableRequestConflict
	}
	return DurableRequestOutcome{
		ReplicatedTransactionResult: ReplicatedTransactionResult{
			ID: result.Transaction, Committed: result.Committed,
			AffectedRows: result.AffectedRows,
		},
		CatalogGeneration: result.CatalogGeneration,
		ShardsFanned:      int(result.ShardsFanned),
		Result:            bytes.Clone(result.Payload),
		AckToken:          entry.AckToken,
		Acknowledged:      entry.State == DurableRequestLedgerAcked,
	}, nil
}

func equalDurableRequestDescriptor(left, right DurableRequestPlanDescriptor) bool {
	return left.TotalBytes == right.TotalBytes && left.PageCount == right.PageCount &&
		left.Root == right.Root && left.Contract == right.Contract && bytes.Equal(left.Inline, right.Inline)
}

func matchesDurableRequestPlanIdentity(left, right DurableRequestPlanDescriptor) bool {
	return left.TotalBytes == right.TotalBytes && left.PageCount == right.PageCount &&
		left.Root != (replication.Digest{}) && left.Root == right.Root && left.Contract == right.Contract
}

func matchesDurableRequestEntryPlanIdentity(
	entry DurableRequestLedgerEntry,
	descriptor DurableRequestPlanDescriptor,
) bool {
	if entry.State != DurableRequestLedgerAcked {
		return matchesDurableRequestPlanIdentity(entry.Plan, descriptor)
	}
	return entry.AckPlanRoot != (replication.Digest{}) &&
		entry.AckPlanRoot == descriptor.Root &&
		entry.AckTerminalContractDigest != (replication.Digest{}) &&
		entry.AckTerminalContractDigest == descriptor.Contract.TerminalContractDigest
}

func equalDurableRequestPending(left, right DurableRequestPending) bool {
	return left.StepRevision == right.StepRevision && bytes.Equal(left.Target, right.Target) &&
		bytes.Equal(left.Command, right.Command)
}

func equalDurableRequestTerminal(left, right DurableRequestTerminal) bool {
	return bytes.Equal(left.Result, right.Result)
}

func cloneDurableRequestPending(value DurableRequestPending) DurableRequestPending {
	value.Target = bytes.Clone(value.Target)
	value.Command = bytes.Clone(value.Command)
	return value
}
