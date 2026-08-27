package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/autosplit"
)

var ErrTailStream = errors.New("rangesplit: invalid authenticated tail stream")

const (
	tailStreamWireFormat       = uint16(0)
	tailStreamFrameHeaderBytes = 32
	tailStreamBindingBytes     = 256
	tailStreamDigestBytes      = sha256.Size

	TailStreamResponseBytes = tailStreamFrameHeaderBytes + tailStreamBindingBytes +
		2*sha256.Size + childStageCursorBytes + tailStreamDigestBytes
	MaxTailStreamRequestBytes = tailStreamFrameHeaderBytes + tailStreamBindingBytes +
		childStageCursorBytes + MaxTailBatchWireBytes + tailStreamDigestBytes
)

var (
	tailStreamRequestMagic   = [8]byte{'V', 'D', 'B', 'S', 'T', 'R', 'Q', 0}
	tailStreamResponseMagic  = [8]byte{'V', 'D', 'B', 'S', 'T', 'R', 'S', 0}
	tailStreamRequestDomain  = []byte("vibedb/range-split/tail-stream-request\x00")
	tailStreamResponseDomain = []byte("vibedb/range-split/tail-stream-response\x00")
)

// TailStreamBinding identifies one immutable child catch-up stream. Mutable
// endpoints and leaders are deliberately excluded; transport authentication
// binds the currently resolved peers separately.
type TailStreamBinding struct {
	Operation       [sha256.Size]byte
	PlanDigest      [sha256.Size]byte
	PlacementDigest [sha256.Size]byte
	ArtifactDigest  [sha256.Size]byte
	Source          ChildArtifactSourceCut
	Child           uint8
}

// NewTailStreamBinding derives the sole canonical binding from an operation
// and one already authenticated artifact manifest.
func NewTailStreamBinding(
	operation [sha256.Size]byte,
	manifest ChildArtifactManifest,
) (TailStreamBinding, error) {
	binding := TailStreamBinding{
		Operation: operation, PlanDigest: manifest.PlanDigest,
		PlacementDigest: manifest.PlacementDigest, ArtifactDigest: manifest.Digest,
		Source: manifest.Source, Child: manifest.Child,
	}
	if !validTailStreamBinding(binding) || !manifest.Present {
		return TailStreamBinding{}, ErrTailStream
	}
	return binding, nil
}

// TailStreamRequest carries one fully authenticated child-local batch and the
// exact durable cursor it is advancing. Batch operation slices borrow the raw
// request and are valid only while raw remains immutable.
type TailStreamRequest struct {
	Binding TailStreamBinding
	Before  ChildStageCursor
	Batch   TailBatch
	digest  [sha256.Size]byte
}

func (request TailStreamRequest) Digest() [sha256.Size]byte { return request.digest }

// TailStreamResponse proves which exact request was durably recognized and
// returns the resulting authenticated child cursor.
type TailStreamResponse struct {
	Binding       TailStreamBinding
	RequestDigest [sha256.Size]byte
	BatchDigest   [sha256.Size]byte
	Cursor        ChildStageCursor
}

// TailStreamCodecWorkspace owns the reusable hash states and nested codec
// workspaces. Reuse it serially.
type TailStreamCodecWorkspace struct {
	hasher hash.Hash
	digest [sha256.Size]byte
	batch  TailBatchCodecWorkspace
	cursor ChildStageCursorWorkspace
}

// MeasureTailStreamRequest performs the first pass of canonical encoding so a
// transport can reserve the exact resident bytes before allocating the frame.
func MeasureTailStreamRequest(request TailStreamRequest) (int, error) {
	if !validTailStreamBinding(request.Binding) || validateTailStreamRequestShape(request) != nil {
		return 0, ErrTailStream
	}
	operationBytes, err := measureTailBatchOperations(request.Batch)
	if err != nil {
		return 0, errors.Join(ErrTailStream, err)
	}
	batchBytes := tailBatchWireHeaderBytes + operationBytes + sha256.Size
	total := tailStreamFrameHeaderBytes + tailStreamBindingBytes + childStageCursorBytes +
		batchBytes + sha256.Size
	if batchBytes > MaxTailBatchWireBytes || total > MaxTailStreamRequestBytes {
		return 0, ErrTailStream
	}
	return total, nil
}

func AppendTailStreamRequest(dst []byte, request TailStreamRequest) ([]byte, error) {
	return AppendTailStreamRequestWithWorkspace(dst, request, &TailStreamCodecWorkspace{})
}

func AppendTailStreamRequestWithWorkspace(
	dst []byte,
	request TailStreamRequest,
	workspace *TailStreamCodecWorkspace,
) ([]byte, error) {
	if workspace == nil || !validTailStreamBinding(request.Binding) ||
		validateTailStreamRequestShape(request) != nil {
		return dst, ErrTailStream
	}
	start := len(dst)
	dst = append(dst, make([]byte, tailStreamFrameHeaderBytes+tailStreamBindingBytes+childStageCursorBytes)...)
	frame := dst[start:]
	clear(frame)
	copy(frame[:8], tailStreamRequestMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], tailStreamWireFormat)
	binary.LittleEndian.PutUint16(frame[10:12], tailStreamFrameHeaderBytes)
	binary.LittleEndian.PutUint32(frame[16:20], tailStreamBindingBytes)
	binary.LittleEndian.PutUint32(frame[20:24], childStageCursorBytes)
	appendTailStreamBinding(frame[tailStreamFrameHeaderBytes:tailStreamFrameHeaderBytes+tailStreamBindingBytes], request.Binding)
	cursorStart := tailStreamFrameHeaderBytes + tailStreamBindingBytes
	encoded, err := AppendChildStageCursorWithWorkspace(frame[:cursorStart], &request.Before, &workspace.cursor)
	if err != nil {
		return dst[:start], errors.Join(ErrTailStream, err)
	}
	frame = encoded
	batchStart := len(frame)
	frame, err = AppendTailBatchWithWorkspace(frame, request.Batch, &workspace.batch)
	if err != nil {
		return dst[:start], errors.Join(ErrTailStream, err)
	}
	batchBytes := len(frame) - batchStart
	if batchBytes > MaxTailBatchWireBytes || len(frame)+sha256.Size > MaxTailStreamRequestBytes ||
		uint64(len(frame))+sha256.Size > math.MaxUint32 {
		return dst[:start], ErrTailStream
	}
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(frame)+sha256.Size))
	binary.LittleEndian.PutUint32(frame[24:28], uint32(batchBytes))
	tailStreamDigest(workspace, tailStreamRequestDomain, frame)
	frame = append(frame, workspace.digest[:]...)
	if start == 0 {
		return frame, nil
	}
	return append(dst[:start], frame...), nil
}

func OpenTailStreamRequest(raw []byte) (TailStreamRequest, error) {
	return OpenTailStreamRequestWithWorkspace(raw, &TailStreamCodecWorkspace{})
}

func OpenTailStreamRequestWithWorkspace(
	raw []byte,
	workspace *TailStreamCodecWorkspace,
) (TailStreamRequest, error) {
	minimum := tailStreamFrameHeaderBytes + tailStreamBindingBytes + childStageCursorBytes +
		tailBatchWireHeaderBytes + 2*sha256.Size
	if workspace == nil || len(raw) < minimum || len(raw) > MaxTailStreamRequestBytes ||
		!bytes.Equal(raw[:8], tailStreamRequestMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != tailStreamWireFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != tailStreamFrameHeaderBytes ||
		uint64(binary.LittleEndian.Uint32(raw[12:16])) != uint64(len(raw)) ||
		binary.LittleEndian.Uint32(raw[16:20]) != tailStreamBindingBytes ||
		binary.LittleEndian.Uint32(raw[20:24]) != childStageCursorBytes ||
		!allChildArtifactZero(raw[28:32]) {
		return TailStreamRequest{}, ErrTailStream
	}
	tailStreamDigest(workspace, tailStreamRequestDomain, raw[:len(raw)-sha256.Size])
	if !bytes.Equal(workspace.digest[:], raw[len(raw)-sha256.Size:]) {
		return TailStreamRequest{}, ErrTailStream
	}
	batchBytes := int(binary.LittleEndian.Uint32(raw[24:28]))
	batchStart := tailStreamFrameHeaderBytes + tailStreamBindingBytes + childStageCursorBytes
	if batchBytes < tailBatchWireHeaderBytes+sha256.Size || batchBytes > MaxTailBatchWireBytes ||
		batchStart+batchBytes+sha256.Size != len(raw) {
		return TailStreamRequest{}, ErrTailStream
	}
	binding, err := openTailStreamBinding(raw[tailStreamFrameHeaderBytes : tailStreamFrameHeaderBytes+tailStreamBindingBytes])
	if err != nil {
		return TailStreamRequest{}, err
	}
	cursor, err := decodeChildStageCursor(
		raw[tailStreamFrameHeaderBytes+tailStreamBindingBytes:batchStart], &workspace.cursor,
	)
	if err != nil {
		return TailStreamRequest{}, errors.Join(ErrTailStream, err)
	}
	batch, err := OpenTailBatchWithWorkspace(raw[batchStart:batchStart+batchBytes], &workspace.batch)
	if err != nil {
		return TailStreamRequest{}, errors.Join(ErrTailStream, err)
	}
	request := TailStreamRequest{Binding: binding, Before: cursor, Batch: batch, digest: workspace.digest}
	if err = validateTailStreamRequestShape(request); err != nil {
		return TailStreamRequest{}, err
	}
	return request, nil
}

// ValidateTailStreamRequest verifies all immutable operation, plan, artifact,
// source-cut, cursor, and batch bindings before a destination applies bytes.
func (p *Partitioner) ValidateTailStreamRequest(
	operation [sha256.Size]byte,
	manifest ChildArtifactManifest,
	request TailStreamRequest,
	workspace *TailBatchVerifyWorkspace,
) error {
	want, err := NewTailStreamBinding(operation, manifest)
	if err != nil || request.Binding != want || workspace == nil ||
		p.ValidateChildStageCursor(manifest, request.Before) != nil ||
		p.VerifyTailBatch(request.Batch, workspace) != nil ||
		request.Before.phase != ChildStageTail || request.Before.child != want.Child ||
		request.Before.planDigest != want.PlanDigest ||
		request.Before.placementDigest != want.PlacementDigest ||
		request.Before.artifactDigest != want.ArtifactDigest ||
		request.Batch.Child != want.Child || request.Batch.PlanDigest != want.PlanDigest ||
		request.Batch.PlacementDigest != want.PlacementDigest ||
		request.Batch.SourceBaseDigest != want.Source.BaseDigest ||
		request.Batch.ChildBaseDigest != want.ArtifactDigest ||
		!cursorImmediatelyPrecedesBatch(request.Before, request.Batch) {
		return errors.Join(ErrTailStream, err)
	}
	return nil
}

func AppendTailStreamResponse(dst []byte, response TailStreamResponse) ([]byte, error) {
	return AppendTailStreamResponseWithWorkspace(dst, response, &TailStreamCodecWorkspace{})
}

func AppendTailStreamResponseWithWorkspace(
	dst []byte,
	response TailStreamResponse,
	workspace *TailStreamCodecWorkspace,
) ([]byte, error) {
	if workspace == nil || !validTailStreamBinding(response.Binding) ||
		response.RequestDigest == ([sha256.Size]byte{}) ||
		response.BatchDigest == ([sha256.Size]byte{}) ||
		!cursorMatchesTailStreamResult(response.Binding, response.BatchDigest, response.Cursor) {
		return dst, ErrTailStream
	}
	start := len(dst)
	dst = append(dst, make([]byte, TailStreamResponseBytes)...)
	frame := dst[start:]
	clear(frame)
	copy(frame[:8], tailStreamResponseMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], tailStreamWireFormat)
	binary.LittleEndian.PutUint16(frame[10:12], tailStreamFrameHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], TailStreamResponseBytes)
	binary.LittleEndian.PutUint32(frame[16:20], tailStreamBindingBytes)
	binary.LittleEndian.PutUint32(frame[20:24], childStageCursorBytes)
	appendTailStreamBinding(frame[tailStreamFrameHeaderBytes:tailStreamFrameHeaderBytes+tailStreamBindingBytes], response.Binding)
	at := tailStreamFrameHeaderBytes + tailStreamBindingBytes
	copy(frame[at:at+sha256.Size], response.RequestDigest[:])
	at += sha256.Size
	copy(frame[at:at+sha256.Size], response.BatchDigest[:])
	at += sha256.Size
	encoded, err := AppendChildStageCursorWithWorkspace(frame[:at], &response.Cursor, &workspace.cursor)
	if err != nil || len(encoded)+sha256.Size != len(frame) {
		return dst[:start], errors.Join(ErrTailStream, err)
	}
	tailStreamDigest(workspace, tailStreamResponseDomain, encoded)
	copy(frame[len(frame)-sha256.Size:], workspace.digest[:])
	return dst, nil
}

func OpenTailStreamResponse(raw []byte) (TailStreamResponse, error) {
	return OpenTailStreamResponseWithWorkspace(raw, &TailStreamCodecWorkspace{})
}

func OpenTailStreamResponseWithWorkspace(
	raw []byte,
	workspace *TailStreamCodecWorkspace,
) (TailStreamResponse, error) {
	if workspace == nil || len(raw) != TailStreamResponseBytes ||
		!bytes.Equal(raw[:8], tailStreamResponseMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != tailStreamWireFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != tailStreamFrameHeaderBytes ||
		binary.LittleEndian.Uint32(raw[12:16]) != TailStreamResponseBytes ||
		binary.LittleEndian.Uint32(raw[16:20]) != tailStreamBindingBytes ||
		binary.LittleEndian.Uint32(raw[20:24]) != childStageCursorBytes ||
		!allChildArtifactZero(raw[24:32]) {
		return TailStreamResponse{}, ErrTailStream
	}
	tailStreamDigest(workspace, tailStreamResponseDomain, raw[:len(raw)-sha256.Size])
	if !bytes.Equal(workspace.digest[:], raw[len(raw)-sha256.Size:]) {
		return TailStreamResponse{}, ErrTailStream
	}
	binding, err := openTailStreamBinding(raw[tailStreamFrameHeaderBytes : tailStreamFrameHeaderBytes+tailStreamBindingBytes])
	if err != nil {
		return TailStreamResponse{}, err
	}
	at := tailStreamFrameHeaderBytes + tailStreamBindingBytes
	response := TailStreamResponse{Binding: binding}
	copy(response.RequestDigest[:], raw[at:at+sha256.Size])
	at += sha256.Size
	copy(response.BatchDigest[:], raw[at:at+sha256.Size])
	at += sha256.Size
	response.Cursor, err = decodeChildStageCursor(raw[at:at+childStageCursorBytes], &workspace.cursor)
	if err != nil || response.RequestDigest == ([sha256.Size]byte{}) ||
		response.BatchDigest == ([sha256.Size]byte{}) ||
		!cursorMatchesTailStreamResult(response.Binding, response.BatchDigest, response.Cursor) {
		return TailStreamResponse{}, errors.Join(ErrTailStream, err)
	}
	return response, nil
}

func ValidateTailStreamResponse(request TailStreamRequest, response TailStreamResponse) error {
	if request.digest == ([sha256.Size]byte{}) || response.Binding != request.Binding ||
		response.RequestDigest != request.digest || response.BatchDigest != request.Batch.Digest ||
		!cursorMatchesBatchResult(response.Cursor, request.Batch) {
		return ErrTailStream
	}
	return nil
}

func appendTailStreamBinding(dst []byte, binding TailStreamBinding) {
	clear(dst[:tailStreamBindingBytes])
	digests := [...][sha256.Size]byte{
		binding.Operation, binding.PlanDigest, binding.PlacementDigest, binding.ArtifactDigest,
		binding.Source.DataChainDigest, binding.Source.BaseDigest, binding.Source.EntryDigest,
	}
	for index := range digests {
		copy(dst[index*sha256.Size:(index+1)*sha256.Size], digests[index][:])
	}
	binary.LittleEndian.PutUint64(dst[224:232], binding.Source.Applied)
	binary.LittleEndian.PutUint64(dst[232:240], binding.Source.Term)
	binary.LittleEndian.PutUint64(dst[240:248], binding.Source.RouteGeneration)
	dst[248] = binding.Child
}

func openTailStreamBinding(raw []byte) (TailStreamBinding, error) {
	if len(raw) != tailStreamBindingBytes || !allChildArtifactZero(raw[249:256]) {
		return TailStreamBinding{}, ErrTailStream
	}
	binding := TailStreamBinding{Child: raw[248]}
	digests := []*[sha256.Size]byte{
		&binding.Operation, &binding.PlanDigest, &binding.PlacementDigest, &binding.ArtifactDigest,
		&binding.Source.DataChainDigest, &binding.Source.BaseDigest, &binding.Source.EntryDigest,
	}
	for index, digest := range digests {
		copy(digest[:], raw[index*sha256.Size:(index+1)*sha256.Size])
	}
	binding.Source.Applied = binary.LittleEndian.Uint64(raw[224:232])
	binding.Source.Term = binary.LittleEndian.Uint64(raw[232:240])
	binding.Source.RouteGeneration = binary.LittleEndian.Uint64(raw[240:248])
	if !validTailStreamBinding(binding) {
		return TailStreamBinding{}, ErrTailStream
	}
	return binding, nil
}

func validTailStreamBinding(binding TailStreamBinding) bool {
	return binding.Operation != ([sha256.Size]byte{}) &&
		binding.PlanDigest != ([sha256.Size]byte{}) &&
		binding.PlacementDigest != ([sha256.Size]byte{}) &&
		binding.ArtifactDigest != ([sha256.Size]byte{}) &&
		binding.Source.DataChainDigest != ([sha256.Size]byte{}) &&
		binding.Source.BaseDigest != ([sha256.Size]byte{}) &&
		binding.Source.EntryDigest != ([sha256.Size]byte{}) &&
		binding.Source.Applied > 0 && binding.Source.Applied < math.MaxUint64 &&
		binding.Source.Term > 0 && binding.Source.Term < math.MaxUint64 &&
		binding.Source.RouteGeneration > 0 && binding.Source.RouteGeneration < math.MaxUint64 &&
		binding.Child < autosplit.MaxSplitChildren
}

func validateTailStreamRequestShape(request TailStreamRequest) error {
	if !validTailStreamBinding(request.Binding) || request.Batch.Child != request.Binding.Child ||
		request.Batch.PlanDigest != request.Binding.PlanDigest ||
		request.Batch.PlacementDigest != request.Binding.PlacementDigest ||
		request.Batch.SourceBaseDigest != request.Binding.Source.BaseDigest ||
		request.Batch.ChildBaseDigest != request.Binding.ArtifactDigest ||
		request.Before.Child() != request.Binding.Child ||
		request.Before.PlanDigest() != request.Binding.PlanDigest ||
		request.Before.PlacementDigest() != request.Binding.PlacementDigest ||
		request.Before.ArtifactDigest() != request.Binding.ArtifactDigest ||
		!cursorImmediatelyPrecedesBatch(request.Before, request.Batch) {
		return ErrTailStream
	}
	return nil
}

func cursorImmediatelyPrecedesBatch(cursor ChildStageCursor, batch TailBatch) bool {
	cut := cursor.SourceCut()
	return cursor.Phase() == ChildStageTail && cut.Applied < math.MaxUint64 &&
		batch.Applied == cut.Applied+1 && batch.Term >= cut.Term &&
		batch.PreviousEntryDigest == cut.EntryDigest &&
		batch.BeforeDataChainDigest == cut.DataChainDigest &&
		batch.BeforeRouteGeneration == cut.RouteGeneration
}

func cursorMatchesBatchResult(cursor ChildStageCursor, batch TailBatch) bool {
	cut := cursor.SourceCut()
	phaseOK := cursor.Phase() == ChildStageTail ||
		cursor.Phase() == ChildStageSealed && batch.beforeCoordinates() != batch.afterCoordinates()
	return phaseOK && cursor.Child() == batch.Child &&
		cursor.PlanDigest() == batch.PlanDigest && cursor.PlacementDigest() == batch.PlacementDigest &&
		cursor.ArtifactDigest() == batch.ChildBaseDigest && cursor.LastBatchDigest() == batch.Digest &&
		cut.Applied == batch.Applied && cut.Term == batch.Term &&
		cut.EntryDigest == batch.EntryDigest && cut.DataChainDigest == batch.AfterDataChainDigest &&
		cut.BaseDigest == batch.SourceBaseDigest && cut.RouteGeneration == batch.AfterRouteGeneration
}

func cursorMatchesTailStreamResult(
	binding TailStreamBinding,
	batchDigest [sha256.Size]byte,
	cursor ChildStageCursor,
) bool {
	cut := cursor.SourceCut()
	return (cursor.Phase() == ChildStageTail || cursor.Phase() == ChildStageSealed) &&
		cursor.Child() == binding.Child && cursor.PlanDigest() == binding.PlanDigest &&
		cursor.PlacementDigest() == binding.PlacementDigest &&
		cursor.ArtifactDigest() == binding.ArtifactDigest && cursor.LastBatchDigest() == batchDigest &&
		cut.BaseDigest == binding.Source.BaseDigest && cut.Applied >= binding.Source.Applied
}

func tailStreamDigest(workspace *TailStreamCodecWorkspace, domain, raw []byte) {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	workspace.hasher.Reset()
	_, _ = workspace.hasher.Write(domain)
	_, _ = workspace.hasher.Write(raw)
	_ = workspace.hasher.Sum(workspace.digest[:0])
}
