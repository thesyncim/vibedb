package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson/x/byteview"
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
	outer, err := client.appendOuter(nil, route, view, inner)
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
	if client == nil || !validReplicatedRoute(route) || len(raw) == 0 ||
		len(raw) > requestledger.MaxCommandBytes {
		return dst, ErrDurableRequest
	}
	fingerprint, retryHome, epoch, sequence := replicatedRequestLedgerProposalIdentity(inner, raw)
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
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(replicatedRequestLedgerProposalDomain))
	_, _ = hash.Write(command.KeyDigest[:])
	_, _ = hash.Write([]byte{byte(command.Operation)})
	var fixed [24]byte
	binary.LittleEndian.PutUint64(fixed[:8], command.ExpectedRevision)
	binary.LittleEndian.PutUint64(fixed[8:16], command.Revision)
	binary.LittleEndian.PutUint64(fixed[16:24], uint64(len(raw)))
	_, _ = hash.Write(fixed[:])
	body := sha256.Sum256(raw)
	_, _ = hash.Write(body[:])
	var digest replication.Digest
	_ = hash.Sum(digest[:0])
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
