package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const replicatedRequestLedgerProposalDomain = "vibedb/gateway/request-ledger-proposal/format-0\x00"

type ReplicatedRequestLedgerRF3Options struct {
	Executor      *ReplicatedExecutor
	Service       serviceauthz.Authority
	ServiceTenant []byte
}

// ReplicatedRequestLedgerRF3 is the production SQL-free client for one RF3
// ledger range. Outer proposal identity belongs to the authenticated gateway
// service; the immutable end-user issuer remains inside requestledger.Key.
type ReplicatedRequestLedgerRF3 struct {
	executor      *ReplicatedExecutor
	service       serviceauthz.Authority
	serviceTenant []byte
}

type ReplicatedRequestLedgerApplyResult struct {
	Ledger replicatedstate.RequestLedgerCompletionResult
	Native ReplicatedResult
}

func NewReplicatedRequestLedgerRF3(
	options ReplicatedRequestLedgerRF3Options,
) (*ReplicatedRequestLedgerRF3, error) {
	if options.Executor == nil || options.Service.Node == ([16]byte{}) ||
		options.Service.Generation == 0 || len(options.ServiceTenant) == 0 ||
		len(options.ServiceTenant) > replication.MaxIdentityBytes {
		return nil, ErrDurableRequest
	}
	return &ReplicatedRequestLedgerRF3{
		executor: options.Executor, service: options.Service,
		serviceTenant: append([]byte(nil), options.ServiceTenant...),
	}, nil
}

func (client *ReplicatedRequestLedgerRF3) Apply(
	ctx context.Context,
	home DurableRequestLedgerHome,
	inner []byte,
) (ReplicatedRequestLedgerApplyResult, error) {
	if client == nil || client.executor == nil || ctx == nil ||
		home.Identity == (replication.Digest{}) || home.Point == (requestledger.LedgerHome{}) {
		return ReplicatedRequestLedgerApplyResult{}, ErrDurableRequest
	}
	view, err := requestledger.OpenCommandInto(inner, nil)
	if err != nil || view.Home != home.Point ||
		view.ExpectedRangeIdentity != requestledger.Digest(home.Identity) {
		return ReplicatedRequestLedgerApplyResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	route := home.borrowedRoute()
	var attempt [32]byte
	if view.Operation == requestledger.OperationCreate {
		// A new caller must reach the inner Create CAS, not join a previous
		// caller's outer proposal waiter and inherit its Created=true result.
		// Transport retries below retain this same envelope byte-for-byte.
		if _, err := rand.Read(attempt[:]); err != nil {
			return ReplicatedRequestLedgerApplyResult{}, err
		}
	}
	outer, err := client.appendOuterAttempt(nil, route, view, inner, attempt)
	if err != nil {
		return ReplicatedRequestLedgerApplyResult{}, err
	}
	serviceCtx, err := serviceauthz.WithAuthority(ctx, client.service)
	if err != nil {
		return ReplicatedRequestLedgerApplyResult{}, err
	}
	result, err := client.executor.ProposeRequestLedger(serviceCtx, route, outer)
	if err != nil {
		return ReplicatedRequestLedgerApplyResult{}, err
	}
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil || completion.ResultFormat != replicatedstate.ResultFormatRequestLedger ||
		completion.Storage != replication.CompletionInline ||
		completion.ResultLength != uint64(len(completion.InlineResult)) {
		return ReplicatedRequestLedgerApplyResult{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	ledger, err := replicatedstate.OpenRequestLedgerCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil || ledger.Operation != view.Operation || ledger.KeyDigest != view.KeyDigest ||
		ledger.RequestDigest != view.RequestDigest || ledger.PlanRoot != view.PlanRoot ||
		ledger.RangeIdentity != view.ExpectedRangeIdentity {
		return ReplicatedRequestLedgerApplyResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	return ReplicatedRequestLedgerApplyResult{Ledger: ledger, Native: result}, nil
}

func (client *ReplicatedRequestLedgerRF3) Read(
	ctx context.Context,
	home DurableRequestLedgerHome,
	read ReplicatedRequestLedgerRead,
) (ReplicatedRequestLedgerReadResult, error) {
	if client == nil || client.executor == nil || ctx == nil ||
		home.Identity == (replication.Digest{}) || read.Key.Valid() == false ||
		read.ExpectedRangeIdentity != requestledger.Digest(home.Identity) {
		return ReplicatedRequestLedgerReadResult{}, ErrDurableRequest
	}
	point, err := requestledger.Home(read.Key)
	if err != nil || point != home.Point {
		return ReplicatedRequestLedgerReadResult{}, errors.Join(err, ErrDurableRequestConflict)
	}
	serviceCtx, err := serviceauthz.WithAuthority(ctx, client.service)
	if err != nil {
		return ReplicatedRequestLedgerReadResult{}, err
	}
	return client.executor.ReadRequestLedger(serviceCtx, home.borrowedRoute(), read)
}

func (client *ReplicatedRequestLedgerRF3) appendOuter(
	dst []byte,
	route ReplicatedRoute,
	inner requestledger.CommandView,
	raw []byte,
) ([]byte, error) {
	return client.appendOuterAttempt(dst, route, inner, raw, [32]byte{})
}

func (client *ReplicatedRequestLedgerRF3) appendOuterAttempt(dst []byte, route ReplicatedRoute, inner requestledger.CommandView, raw []byte, attempt [32]byte) ([]byte, error) {
	if client == nil || !validReplicatedRoute(route) || len(raw) == 0 ||
		len(raw) > requestledger.MaxCommandBytes {
		return dst, ErrDurableRequest
	}
	fingerprint, retryHome, epoch, sequence := replicatedRequestLedgerProposalIdentity(inner, raw)
	if attempt != ([32]byte{}) {
		var identity [64]byte
		copy(identity[:32], fingerprint[:])
		copy(identity[32:], attempt[:])
		fingerprint = sha256.Sum256(identity[:])
		copy(retryHome[:], fingerprint[:len(retryHome)])
		epoch = max(uint64(1), binary.LittleEndian.Uint64(fingerprint[8:16]))
		sequence = max(uint64(1), binary.LittleEndian.Uint64(fingerprint[16:24]))
	}
	command := replication.Command{
		Kind:           replication.CommandRequestLedger,
		AuthorityClass: replication.CommandAuthorityRequestLedger,
		ClusterID:      route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: route.Group.TopologyRecoveryEpoch,
		Distribution:          string(route.Distribution),
		Shard:                 string(route.Shard),
		AllocationGeneration:  route.AllocationGeneration,
		ShardIncarnation:      route.Group.ShardIncarnation, GroupID: route.Group.GroupID,
		ReplicaSetVersion:      route.Command.ReplicaSetVersion,
		ActivePolicyGeneration: route.Command.ActivePolicyGeneration,
		ProtectionEpoch:        route.Command.ProtectionEpoch, OwnershipEpoch: route.Command.OwnershipEpoch,
		SchemaGeneration: route.Command.SchemaGeneration,
		RoutingVersion:   route.Command.RoutingVersion, RouteGeneration: route.Command.RouteGeneration,
		Tenant: client.serviceTenant, ClientID: replication.ID128(client.service.Node),
		ClientEpoch: epoch, ClientSequence: sequence,
		Fingerprint: fingerprint, RetryHome: retryHome, RequestLedger: raw,
	}
	return replication.AppendCommand(dst, command)
}

func replicatedRequestLedgerProposalIdentity(
	command requestledger.CommandView,
	raw []byte,
) (replication.Digest, replication.RetryHome, uint64, uint64) {
	const fixedBytes = 32 + 1 + 24 + sha256.Size
	var preimage [len(replicatedRequestLedgerProposalDomain) + fixedBytes]byte
	offset := copy(preimage[:], replicatedRequestLedgerProposalDomain)
	copy(preimage[offset:], command.KeyDigest[:])
	offset += len(command.KeyDigest)
	preimage[offset] = byte(command.Operation)
	offset++
	binary.LittleEndian.PutUint64(preimage[offset:offset+8], command.ExpectedRevision)
	binary.LittleEndian.PutUint64(preimage[offset+8:offset+16], command.Revision)
	binary.LittleEndian.PutUint64(preimage[offset+16:offset+24], uint64(len(raw)))
	offset += 24
	body := sha256.Sum256(raw)
	copy(preimage[offset:], body[:])
	digest := replication.Digest(sha256.Sum256(preimage[:]))
	var retryHome replication.RetryHome
	copy(retryHome[:], digest[:len(retryHome)])
	epoch := binary.LittleEndian.Uint64(digest[8:16])
	sequence := binary.LittleEndian.Uint64(digest[16:24])
	if epoch == 0 {
		epoch = 1
	}
	if sequence == 0 {
		sequence = 1
	}
	return digest, retryHome, epoch, sequence
}
