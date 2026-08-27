package splitcontroller

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

var (
	ErrTailStreamControl        = errors.New("splitcontroller: invalid tail stream control")
	ErrTailStreamUnauthorized   = errors.New("splitcontroller: tail stream is not authorized")
	ErrTailStreamConflict       = errors.New("splitcontroller: tail stream conflicts with durable state")
	ErrTailStreamOutcomeUnknown = errors.New("splitcontroller: tail stream outcome is unknown")
	ErrTailStreamBound          = errors.New("splitcontroller: tail stream admission bound reached")
)

const AbsoluteMaxTailStreamConcurrency = 256

var tailStreamRequestDiscriminator = [8]byte{'V', 'D', 'B', 'S', 'T', 'R', 'Q', 0}

// TailStreamRequestDiscriminator identifies the canonical binary service on
// the authenticated shard-control mux.
func TailStreamRequestDiscriminator() [8]byte { return tailStreamRequestDiscriminator }

// TailStreamApplyTarget is the narrow durable destination required by the
// network service. Observe must not scan the child collection.
type TailStreamApplyTarget interface {
	ObserveTail(context.Context) (rangesplit.ChildStageCursor, bool, error)
	ApplyTail(context.Context, rangesplit.TailBatch) error
}

// TailStreamResolvedTarget is independently reconstructed server-side. The
// request cannot select a different partitioner, artifact, or trust domain.
type TailStreamResolvedTarget struct {
	Operation   OperationID
	Partitioner *rangesplit.Partitioner
	Manifest    rangesplit.ChildArtifactManifest
	TrustDomain rafttransport.TrustDomain
	Target      TailStreamApplyTarget
}

type TailStreamTargetResolver interface {
	ResolveSplitTail(context.Context, OperationID, uint8) (TailStreamResolvedTarget, error)
}

type TailStreamAuthorizeFunc func(rafttransport.PeerIdentity, rangesplit.TailStreamBinding) bool

type TailStreamServiceOptions struct {
	Resolver         TailStreamTargetResolver
	Authorize        TailStreamAuthorizeFunc
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxInflightBytes uint64
}

type TailStreamService struct {
	resolver      TailStreamTargetResolver
	authorize     TailStreamAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	bytes         tailStreamByteBudget
}

func NewTailStreamService(options TailStreamServiceOptions) (*TailStreamService, error) {
	if options.Resolver == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxTailStreamConcurrency ||
		options.MaxInflightBytes == 0 ||
		options.MaxInflightBytes > uint64(rangesplit.MaxTailStreamRequestBytes)*uint64(options.MaxConcurrent) {
		return nil, ErrTailStreamControl
	}
	return &TailStreamService{
		resolver: options.Resolver, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots: make(chan struct{}, options.MaxConcurrent),
		bytes: tailStreamByteBudget{limit: options.MaxInflightBytes},
	}, nil
}

// Serve consumes one request on an already mutually authenticated connection.
// It acquires both request and byte admission before allocating the advertised
// variable body, and verifies the entire nested frame before applying bytes.
func (service *TailStreamService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrTailStreamUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrTailStreamBound
	}
	if err := setTailStreamReadDeadline(ctx, connection, service.readDeadline); err != nil {
		return err
	}
	var header [32]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return errors.Join(ErrTailStreamControl, err)
	}
	total, err := validateTailStreamRequestHeader(header[:])
	if err != nil || !service.bytes.acquire(uint64(total)) {
		if err != nil {
			return err
		}
		return ErrTailStreamBound
	}
	defer service.bytes.release(uint64(total))
	raw := make([]byte, total)
	copy(raw, header[:])
	if _, err = io.ReadFull(connection, raw[len(header):]); err != nil {
		return errors.Join(ErrTailStreamControl, err)
	}
	request, err := rangesplit.OpenTailStreamRequest(raw)
	if err != nil {
		return errors.Join(ErrTailStreamControl, err)
	}
	peer := connection.PeerIdentity()
	if !service.authorize(peer, request.Binding) {
		return ErrTailStreamUnauthorized
	}
	resolved, err := service.resolver.ResolveSplitTail(
		ctx, OperationID(request.Binding.Operation), request.Binding.Child,
	)
	if err != nil {
		return err
	}
	if !validTailStreamResolvedTarget(resolved, request) || peer.TrustDomain != resolved.TrustDomain {
		return ErrTailStreamConflict
	}
	if err = resolved.Partitioner.ValidateTailStreamRequest(
		request.Binding.Operation, resolved.Manifest, request, &rangesplit.TailBatchVerifyWorkspace{},
	); err != nil {
		return errors.Join(ErrTailStreamConflict, err)
	}
	current, ok, err := resolved.Target.ObserveTail(ctx)
	if err != nil {
		return err
	}
	switch {
	case !ok:
		return ErrTailStreamConflict
	case current == request.Before || current.ResumesTailBatch(request.Before, request.Batch):
		if err = resolved.Target.ApplyTail(ctx, request.Batch); err != nil {
			return err
		}
	case tailStreamCursorMatchesBatch(current, request.Batch):
		// Exact outcome-unknown retry: the durable result is already present.
	default:
		return ErrTailStreamConflict
	}
	after, ok, err := resolved.Target.ObserveTail(ctx)
	if err != nil {
		return err
	}
	if !ok || !tailStreamCursorMatchesBatch(after, request.Batch) {
		return ErrTailStreamConflict
	}
	response := rangesplit.TailStreamResponse{
		Binding: request.Binding, RequestDigest: request.Digest(),
		BatchDigest: request.Batch.Digest, Cursor: after,
	}
	var responseBuffer [rangesplit.TailStreamResponseBytes]byte
	encoded, err := rangesplit.AppendTailStreamResponse(responseBuffer[:0], response)
	if err != nil {
		return errors.Join(ErrTailStreamControl, err)
	}
	if err = setTailStreamWriteDeadline(ctx, connection, service.writeDeadline); err != nil {
		return err
	}
	return writeTailStreamFull(connection, encoded)
}

type TailStreamStreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type TailStreamClientOptions struct {
	Opener           TailStreamStreamOpener
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxInflightBytes uint64
}

type TailStreamClient struct {
	opener        TailStreamStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	bytes         tailStreamByteBudget
}

func NewTailStreamClient(options TailStreamClientOptions) (*TailStreamClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > AbsoluteMaxTailStreamConcurrency ||
		options.MaxInflightBytes == 0 ||
		options.MaxInflightBytes > uint64(
			rangesplit.MaxTailStreamRequestBytes+rangesplit.TailStreamResponseBytes,
		)*uint64(options.MaxConcurrent) {
		return nil, ErrTailStreamControl
	}
	return &TailStreamClient{
		opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline, slots: make(chan struct{}, options.MaxConcurrent),
		bytes: tailStreamByteBudget{limit: options.MaxInflightBytes},
	}, nil
}

// Apply sends one canonical request. Once writing begins, every transport
// failure is outcome-unknown and must be resolved by replaying the same request.
func (client *TailStreamClient) Apply(
	ctx context.Context,
	destination rafttransport.NodeID,
	trustDomain rafttransport.TrustDomain,
	request rangesplit.TailStreamRequest,
) (rangesplit.ChildStageCursor, error) {
	if client == nil || ctx == nil || destination == (rafttransport.NodeID{}) ||
		trustDomain == (rafttransport.TrustDomain{}) {
		return rangesplit.ChildStageCursor{}, ErrTailStreamControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return rangesplit.ChildStageCursor{}, cause
	}
	select {
	case client.slots <- struct{}{}:
		defer func() { <-client.slots }()
	default:
		return rangesplit.ChildStageCursor{}, ErrTailStreamBound
	}
	size, err := rangesplit.MeasureTailStreamRequest(request)
	if err != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamControl, err)
	}
	resident := uint64(size + rangesplit.TailStreamResponseBytes)
	if !client.bytes.acquire(resident) {
		return rangesplit.ChildStageCursor{}, ErrTailStreamBound
	}
	defer client.bytes.release(resident)
	raw, err := rangesplit.AppendTailStreamRequest(make([]byte, 0, size), request)
	if err != nil || len(raw) != size {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamControl, err)
	}
	sealed, err := rangesplit.OpenTailStreamRequest(raw)
	if err != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamControl, err)
	}
	connection, err := client.opener.OpenShardControl(ctx, destination)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return rangesplit.ChildStageCursor{}, err
	}
	if connection == nil {
		return rangesplit.ChildStageCursor{}, ErrTailStreamControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != destination || peer.TrustDomain != trustDomain {
		return rangesplit.ChildStageCursor{}, ErrTailStreamUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err = setTailStreamWriteDeadline(ctx, connection, client.writeDeadline); err != nil {
		return rangesplit.ChildStageCursor{}, err
	}
	if err = writeTailStreamFull(connection, raw); err != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamOutcomeUnknown, err)
	}
	if err = setTailStreamReadDeadline(ctx, connection, client.readDeadline); err != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamOutcomeUnknown, err)
	}
	var responseRaw [rangesplit.TailStreamResponseBytes]byte
	if _, err = io.ReadFull(connection, responseRaw[:]); err != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamOutcomeUnknown, err)
	}
	response, err := rangesplit.OpenTailStreamResponse(responseRaw[:])
	if err != nil || rangesplit.ValidateTailStreamResponse(sealed, response) != nil {
		return rangesplit.ChildStageCursor{}, errors.Join(ErrTailStreamConflict, err)
	}
	return response.Cursor, nil
}

// LocalTailStreamTarget adapts the existing durable child action surface to
// the transport without introducing a second apply path.
type LocalTailStreamTarget struct {
	Plan      *Plan
	Artifacts rangesplit.ChildArtifactSet
	Child     uint8
	Actions   *LocalChildActions
}

func (target LocalTailStreamTarget) ObserveTail(ctx context.Context) (rangesplit.ChildStageCursor, bool, error) {
	if target.Actions == nil {
		return rangesplit.ChildStageCursor{}, false, ErrTailStreamControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return rangesplit.ChildStageCursor{}, false, cause
	}
	return target.Actions.Observe(target.Plan, target.Artifacts, target.Child)
}

func (target LocalTailStreamTarget) ApplyTail(ctx context.Context, batch rangesplit.TailBatch) error {
	if target.Actions == nil {
		return ErrTailStreamControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return target.Actions.ApplyTailBatch(target.Plan, target.Artifacts, target.Child, batch)
}

// ResolveLocalTailStreamTarget constructs a checked resolver result from the
// retained plan/artifact authorities and existing child actions.
func ResolveLocalTailStreamTarget(
	plan *Plan,
	artifacts rangesplit.ChildArtifactSet,
	child uint8,
	actions *LocalChildActions,
) (TailStreamResolvedTarget, error) {
	if plan == nil || actions == nil || !plan.validNonRetainedChild(child) ||
		plan.partitioner.ValidateChildArtifactSet(artifacts) != nil {
		return TailStreamResolvedTarget{}, ErrInvalidPlan
	}
	target := plan.targets[child]
	return TailStreamResolvedTarget{
		Operation: plan.operation, Partitioner: plan.partitioner,
		Manifest: artifacts.Children[child],
		TrustDomain: rafttransport.TrustDomain{
			ClusterID: target.WAL.ClusterID, ClusterIncarnation: target.WAL.ClusterIncarnation,
		},
		Target: LocalTailStreamTarget{
			Plan: plan, Artifacts: artifacts, Child: child, Actions: actions,
		},
	}, nil
}

type RemoteTailSink struct {
	mu          sync.Mutex
	ctx         context.Context
	client      *TailStreamClient
	destination rafttransport.NodeID
	trustDomain rafttransport.TrustDomain
	binding     rangesplit.TailStreamBinding
	cursor      rangesplit.ChildStageCursor
}

func NewRemoteTailSink(
	ctx context.Context,
	client *TailStreamClient,
	destination rafttransport.NodeID,
	trustDomain rafttransport.TrustDomain,
	binding rangesplit.TailStreamBinding,
	cursor rangesplit.ChildStageCursor,
) (*RemoteTailSink, error) {
	if ctx == nil || client == nil || destination == (rafttransport.NodeID{}) ||
		trustDomain == (rafttransport.TrustDomain{}) || cursor.Child() != binding.Child ||
		cursor.PlanDigest() != binding.PlanDigest ||
		cursor.PlacementDigest() != binding.PlacementDigest ||
		cursor.ArtifactDigest() != binding.ArtifactDigest {
		return nil, ErrTailStreamControl
	}
	return &RemoteTailSink{
		ctx: ctx, client: client, destination: destination, trustDomain: trustDomain,
		binding: binding, cursor: cursor,
	}, nil
}

func (sink *RemoteTailSink) Apply(batch rangesplit.TailBatch) error {
	if sink == nil {
		return ErrTailStreamControl
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	request := rangesplit.TailStreamRequest{Binding: sink.binding, Before: sink.cursor, Batch: batch}
	next, err := sink.client.Apply(sink.ctx, sink.destination, sink.trustDomain, request)
	if err != nil {
		return err
	}
	sink.cursor = next
	return nil
}

func (sink *RemoteTailSink) Cursor() rangesplit.ChildStageCursor {
	if sink == nil {
		return rangesplit.ChildStageCursor{}
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.cursor
}

type tailStreamByteBudget struct {
	mu    sync.Mutex
	limit uint64
	used  uint64
}

func (budget *tailStreamByteBudget) acquire(bytes uint64) bool {
	if budget == nil || bytes == 0 {
		return false
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if bytes > budget.limit || budget.used > budget.limit-bytes {
		return false
	}
	budget.used += bytes
	return true
}

func (budget *tailStreamByteBudget) release(bytes uint64) {
	budget.mu.Lock()
	if bytes > budget.used {
		panic("splitcontroller: tail stream byte budget underflow")
	}
	budget.used -= bytes
	budget.mu.Unlock()
}

func validateTailStreamRequestHeader(header []byte) (int, error) {
	if len(header) != 32 || !bytes.Equal(header[:8], tailStreamRequestDiscriminator[:]) ||
		binary.LittleEndian.Uint16(header[8:10]) != 0 ||
		binary.LittleEndian.Uint16(header[10:12]) != 32 ||
		binary.LittleEndian.Uint32(header[16:20]) != 256 ||
		binary.LittleEndian.Uint32(header[20:24]) != rangesplit.ChildStageCursorEncodedBytes ||
		!allSplitTailZero(header[28:32]) {
		return 0, ErrTailStreamControl
	}
	total := uint64(binary.LittleEndian.Uint32(header[12:16]))
	batch := uint64(binary.LittleEndian.Uint32(header[24:28]))
	minimum := uint64(32 + 256 + rangesplit.ChildStageCursorEncodedBytes + 440 + 2*32)
	if total < minimum || total > rangesplit.MaxTailStreamRequestBytes ||
		batch < 440+32 || batch > uint64(rangesplit.MaxTailBatchWireBytes) ||
		total != 32+256+rangesplit.ChildStageCursorEncodedBytes+batch+32 || total > math.MaxInt {
		return 0, ErrTailStreamControl
	}
	return int(total), nil
}

func validTailStreamResolvedTarget(
	resolved TailStreamResolvedTarget,
	request rangesplit.TailStreamRequest,
) bool {
	return resolved.Operation == OperationID(request.Binding.Operation) &&
		resolved.Partitioner != nil && resolved.Target != nil &&
		resolved.TrustDomain != (rafttransport.TrustDomain{}) && resolved.Manifest.Present &&
		resolved.Manifest.Child == request.Binding.Child &&
		resolved.Manifest.PlanDigest == request.Binding.PlanDigest &&
		resolved.Manifest.PlacementDigest == request.Binding.PlacementDigest &&
		resolved.Manifest.Source == request.Binding.Source &&
		resolved.Manifest.Digest == request.Binding.ArtifactDigest
}

func tailStreamCursorMatchesBatch(
	cursor rangesplit.ChildStageCursor,
	batch rangesplit.TailBatch,
) bool {
	cut := cursor.SourceCut()
	return (cursor.Phase() == rangesplit.ChildStageTail || cursor.Phase() == rangesplit.ChildStageSealed) &&
		cursor.Child() == batch.Child && cursor.PlanDigest() == batch.PlanDigest &&
		cursor.PlacementDigest() == batch.PlacementDigest &&
		cursor.ArtifactDigest() == batch.ChildBaseDigest &&
		cursor.LastBatchDigest() == batch.Digest && cut.Applied == batch.Applied &&
		cut.Term == batch.Term && cut.EntryDigest == batch.EntryDigest &&
		cut.DataChainDigest == batch.AfterDataChainDigest &&
		cut.BaseDigest == batch.SourceBaseDigest && cut.RouteGeneration == batch.AfterRouteGeneration
}

func setTailStreamReadDeadline(
	ctx context.Context,
	connection rafttransport.PeerConnection,
	deadline rafttransport.DeadlineFunc,
) error {
	value := boundedTailStreamDeadline(ctx, deadline())
	if value.IsZero() {
		return ErrTailStreamControl
	}
	return connection.SetReadDeadline(value)
}

func setTailStreamWriteDeadline(
	ctx context.Context,
	connection rafttransport.PeerConnection,
	deadline rafttransport.DeadlineFunc,
) error {
	value := boundedTailStreamDeadline(ctx, deadline())
	if value.IsZero() {
		return ErrTailStreamControl
	}
	return connection.SetWriteDeadline(value)
}

func boundedTailStreamDeadline(ctx context.Context, fallback time.Time) time.Time {
	if ctx == nil || fallback.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(fallback) {
		return deadline
	}
	return fallback
}

func writeTailStreamFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

func allSplitTailZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}
