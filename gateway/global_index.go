package gateway

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	ErrIndexNotFound       = errors.New("gateway: distributed index was not found")
	ErrIndexNotReady       = errors.New("gateway: distributed index incarnation is not ready")
	ErrIndexNotGlobal      = errors.New("gateway: distributed index is not global")
	ErrGlobalIndexDocument = errors.New("gateway: document does not satisfy global index key contract")
)

// GlobalIndexProgram is the immutable, compile-once routing program for one
// ready global index incarnation. Its pointer programs and routing metadata
// borrow the pinned Snapshot and are safe for concurrent use. Workspaces are
// per caller.
type GlobalIndexProgram struct {
	snapshot *Snapshot
	metadata IndexMetadata

	indexSpec     distribution.DistributionSpec
	indexManifest *distribution.Manifest
	baseSpec      distribution.DistributionSpec
	baseManifest  *distribution.Manifest

	keyPointers     []vibejson.CompiledPointer
	locatorPointers []vibejson.CompiledPointer
	baseOrdinals    [distribution.KeyspaceWidth]uint8
	baseArity       uint8
}

// GlobalIndexWorkspace owns reusable document-tape, scalar, decoded-string,
// and tuple storage. One workspace is single-consumer and retains only its
// observed high-water marks.
type GlobalIndexWorkspace struct {
	entries      []vibejson.IndexEntry
	decoded      []byte
	scalars      [4 + distribution.KeyspaceWidth]distribution.Scalar
	baseScalars  [distribution.KeyspaceWidth]distribution.Scalar
	keyTuple     []byte
	locatorTuple []byte
	entryKey     []byte
	locatorValue []byte
}

// GlobalIndexRoute is one extracted index entry plus both independently
// resolved owners. Byte slices alias the workspace until its next call.
type GlobalIndexRoute struct {
	Index IndexMetadata

	KeyTuple     []byte
	LocatorTuple []byte
	EntryKey     []byte
	// LocatorValue is a compact vibejson-compatible scalar array persisted as
	// the entry payload. It preserves exact numbers and decodes back to the
	// locator tuple without base64 or a binary-to-text space tax.
	LocatorValue []byte

	IndexPoint      distribution.KeyspacePoint
	IndexTarget     distribution.Target
	IndexAddress    string
	IndexBucketBits uint8
	IndexScope      distributedtxn.IntentScope

	BasePoint      distribution.KeyspacePoint
	BaseTarget     distribution.Target
	BaseAddress    string
	BaseBucketBits uint8
	BaseScope      distributedtxn.IntentScope
}

type globalIndexOwner struct {
	point   distribution.KeyspacePoint
	target  distribution.Target
	address string
	bits    uint8
	scope   distributedtxn.IntentScope
}

// CompileGlobalIndex resolves and pins one ready global index without copying
// its compiled vibejson programs. Catalog publication fences ID, incarnation,
// relation placement, and every key/locator path.
func (s *Snapshot) CompileGlobalIndex(table, name string) (GlobalIndexProgram, error) {
	if s == nil {
		return GlobalIndexProgram{}, ErrNoCatalog
	}
	ordinal, metadata, ok := s.indexOrdinal(table, name)
	if !ok {
		return GlobalIndexProgram{}, ErrIndexNotFound
	}
	if !metadata.Ready() {
		return GlobalIndexProgram{}, ErrIndexNotReady
	}
	if !metadata.Global() {
		return GlobalIndexProgram{}, ErrIndexNotGlobal
	}
	_, indexSpec, indexManifest, ok := s.plannerTableFor(metadata.Relation)
	if !ok {
		return GlobalIndexProgram{}, &CatalogError{Reason: "global index relation lost its placement"}
	}
	basePlacement, baseSpec, baseManifest, ok := s.plannerTableFor(metadata.Table)
	if !ok {
		return GlobalIndexProgram{}, &CatalogError{Reason: "global index base table lost its placement"}
	}
	pointerProgram, ok := s.globalIndexPointerProgram(ordinal)
	if !ok {
		return GlobalIndexProgram{}, &CatalogError{Reason: "global index lost its compiled pointer program"}
	}
	keyBase := pointerProgram.pointerBase
	locatorBase := keyBase + uint32(pointerProgram.keyCount)
	program := GlobalIndexProgram{
		snapshot: s, metadata: metadata,
		indexSpec: indexSpec, indexManifest: indexManifest,
		baseSpec: baseSpec, baseManifest: baseManifest,
		keyPointers:     s.plannerGlobalIndexPointers[keyBase:locatorBase],
		locatorPointers: s.plannerGlobalIndexPointers[locatorBase : locatorBase+uint32(pointerProgram.locatorCount)],
		baseArity:       uint8(len(basePlacement.Columns)),
	}
	for i, path := range basePlacement.Columns {
		found := false
		for locator := uint8(0); locator < metadata.LocatorCount; locator++ {
			if metadata.LocatorPaths[locator] == path {
				program.baseOrdinals[i] = locator
				found = true
				break
			}
		}
		if !found {
			return GlobalIndexProgram{}, &CatalogError{Reason: "global index locator lost a base placement path"}
		}
	}
	return program, nil
}

func (s *Snapshot) globalIndexPointerProgram(ordinal uint32) (plannerGlobalIndexProgram, bool) {
	lo, hi := 0, len(s.plannerGlobalIndexPrograms)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.plannerGlobalIndexPrograms[mid].ordinal < ordinal {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(s.plannerGlobalIndexPrograms) ||
		s.plannerGlobalIndexPrograms[lo].ordinal != ordinal {
		return plannerGlobalIndexProgram{}, false
	}
	return s.plannerGlobalIndexPrograms[lo], true
}

func (s *Snapshot) indexOrdinal(table, name string) (uint32, IndexMetadata, bool) {
	set := s.Indexes(table)
	if set.snapshot == nil {
		return 0, IndexMetadata{}, false
	}
	lo, hi := uint32(0), set.span.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		ordinal := set.span.first + mid
		candidate := s.indexName(s.plannerIndexes[ordinal].name)
		if candidate < name {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == set.span.count {
		return 0, IndexMetadata{}, false
	}
	ordinal := set.span.first + lo
	if s.indexName(s.plannerIndexes[ordinal].name) != name {
		return 0, IndexMetadata{}, false
	}
	return ordinal, s.indexMetadata(set.table, ordinal), true
}

// RouteDocument extracts the ordered index key and base locator through one
// vibejson structural-index build, encodes canonical tuples, and resolves the
// global-index and base owners independently. It never constructs a JSON tree
// or converts numbers through floating point.
func (p GlobalIndexProgram) RouteDocument(
	document []byte,
	workspace *GlobalIndexWorkspace,
) (GlobalIndexRoute, error) {
	if p.snapshot == nil || workspace == nil {
		return GlobalIndexRoute{}, ErrGlobalIndexDocument
	}
	needed, err := vibejson.RequiredIndexEntries(document)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: invalid JSON: %v", ErrGlobalIndexDocument, err)
	}
	if cap(workspace.entries) < needed {
		workspace.entries = make([]vibejson.IndexEntry, needed)
	} else {
		workspace.entries = workspace.entries[:needed]
	}
	index, err := vibejson.BuildIndex(document, workspace.entries)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: invalid JSON: %v", ErrGlobalIndexDocument, err)
	}
	if len(document) > int(^uint(0)>>1)/2 {
		return GlobalIndexRoute{}, fmt.Errorf("%w: document is too large", ErrGlobalIndexDocument)
	}
	decodedCapacity := len(document) * 2
	if cap(workspace.decoded) < decodedCapacity {
		workspace.decoded = make([]byte, 0, decodedCapacity)
	} else {
		workspace.decoded = workspace.decoded[:0]
	}
	root := index.Root()
	key := workspace.scalars[:len(p.keyPointers)]
	locator := workspace.scalars[len(p.keyPointers) : len(p.keyPointers)+len(p.locatorPointers)]
	workspace.decoded, err = extractGlobalIndexScalars(
		root, p.keyPointers, key, workspace.decoded, "key", nil,
	)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	workspace.decoded, err = extractGlobalIndexScalars(
		root, p.locatorPointers, locator, workspace.decoded, "locator",
		&workspace.locatorValue,
	)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	return p.routeScalars(key, locator, workspace, false)
}

// RouteScalars routes an already extracted index key and base locator. It is
// the flat-INSERT fast path: prepared column ordinals feed canonical scalars
// directly, avoiding construction and reparsing of an intermediate document.
func (p GlobalIndexProgram) RouteScalars(
	key, locator []distribution.Scalar,
	workspace *GlobalIndexWorkspace,
) (GlobalIndexRoute, error) {
	if p.snapshot == nil || workspace == nil || len(key) != len(p.keyPointers) ||
		len(locator) != len(p.locatorPointers) {
		return GlobalIndexRoute{}, ErrGlobalIndexDocument
	}
	return p.routeScalars(key, locator, workspace, true)
}

// RouteKey resolves an equality key to the independently sharded index owner
// without requiring a base locator. It is the gateway read-planning fast path:
// the canonical tuple is sent byte-for-byte to the shard lookup envelope.
func (p GlobalIndexProgram) RouteKey(
	key []distribution.Scalar,
	workspace *GlobalIndexWorkspace,
) (GlobalIndexRoute, error) {
	if p.snapshot == nil || workspace == nil || len(key) != len(p.keyPointers) {
		return GlobalIndexRoute{}, ErrGlobalIndexDocument
	}
	var err error
	workspace.keyTuple, err = distribution.CurrentTupleCodec.AppendTuple(
		workspace.keyTuple[:0], key,
	)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: encode key: %v", ErrGlobalIndexDocument, err)
	}
	owner, err := p.resolveIndexOwner(key)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	return GlobalIndexRoute{
		Index: p.metadata, KeyTuple: workspace.keyTuple,
		IndexPoint: owner.point, IndexTarget: owner.target,
		IndexAddress: owner.address, IndexBucketBits: owner.bits,
		IndexScope: owner.scope,
	}, nil
}

// RouteLocatorValue validates and decodes one compact locator array returned by
// an index shard, then resolves its base owner. Strings remain byte-native and
// exact numbers never cross floating point. Returned slices alias workspace.
func (p GlobalIndexProgram) RouteLocatorValue(
	value []byte,
	workspace *GlobalIndexWorkspace,
) (GlobalIndexRoute, error) {
	if p.snapshot == nil || workspace == nil || len(value) == 0 {
		return GlobalIndexRoute{}, ErrGlobalIndexDocument
	}
	needed, err := vibejson.RequiredIndexEntries(value)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: invalid locator JSON: %v", ErrGlobalIndexDocument, err)
	}
	if cap(workspace.entries) < needed {
		workspace.entries = make([]vibejson.IndexEntry, needed)
	} else {
		workspace.entries = workspace.entries[:needed]
	}
	index, err := vibejson.BuildIndex(value, workspace.entries)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: invalid locator JSON: %v", ErrGlobalIndexDocument, err)
	}
	root := index.Root()
	length, ok := root.ArrayLen()
	if !ok || length != len(p.locatorPointers) {
		return GlobalIndexRoute{}, fmt.Errorf(
			"%w: locator has %d values, want %d",
			ErrGlobalIndexDocument, length, len(p.locatorPointers),
		)
	}
	workspace.decoded = workspace.decoded[:0]
	locator := workspace.scalars[len(p.keyPointers) : len(p.keyPointers)+length]
	for i := range locator {
		node, _ := root.Index(i)
		workspace.decoded, err = extractGlobalIndexScalar(
			node.Raw(), &locator[i], workspace.decoded, "locator", i,
		)
		if err != nil {
			return GlobalIndexRoute{}, err
		}
	}
	workspace.locatorTuple, err = distribution.CurrentTupleCodec.AppendTuple(
		workspace.locatorTuple[:0], locator,
	)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: encode locator: %v", ErrGlobalIndexDocument, err)
	}
	owner, err := p.resolveBaseOwner(locator, workspace)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	return GlobalIndexRoute{
		Index: p.metadata, LocatorTuple: workspace.locatorTuple,
		LocatorValue: value,
		BasePoint:    owner.point, BaseTarget: owner.target,
		BaseAddress: owner.address, BaseBucketBits: owner.bits,
		BaseScope: owner.scope,
	}, nil
}

func (p GlobalIndexProgram) routeScalars(
	key, locator []distribution.Scalar,
	workspace *GlobalIndexWorkspace,
	encodeLocator bool,
) (GlobalIndexRoute, error) {
	var err error
	workspace.keyTuple, err = distribution.CurrentTupleCodec.AppendTuple(workspace.keyTuple[:0], key)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: encode key: %v", ErrGlobalIndexDocument, err)
	}
	workspace.locatorTuple, err = distribution.CurrentTupleCodec.AppendTuple(
		workspace.locatorTuple[:0], locator,
	)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: encode locator: %v", ErrGlobalIndexDocument, err)
	}
	workspace.entryKey = append(workspace.entryKey[:0], workspace.keyTuple...)
	if p.metadata.Flags&IndexUnique == 0 {
		workspace.entryKey = append(workspace.entryKey, workspace.locatorTuple...)
	}
	if encodeLocator {
		workspace.locatorValue, err = appendGlobalIndexLocatorValue(
			workspace.locatorValue[:0], locator,
		)
		if err != nil {
			return GlobalIndexRoute{}, err
		}
	}

	indexOwner, err := p.resolveIndexOwner(key)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	baseOwner, err := p.resolveBaseOwner(locator, workspace)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	return GlobalIndexRoute{
		Index:    p.metadata,
		KeyTuple: workspace.keyTuple, LocatorTuple: workspace.locatorTuple,
		EntryKey: workspace.entryKey, LocatorValue: workspace.locatorValue,
		IndexPoint: indexOwner.point, IndexTarget: indexOwner.target,
		IndexAddress: indexOwner.address, IndexBucketBits: indexOwner.bits,
		IndexScope: indexOwner.scope,
		BasePoint:  baseOwner.point, BaseTarget: baseOwner.target,
		BaseAddress: baseOwner.address, BaseBucketBits: baseOwner.bits,
		BaseScope: baseOwner.scope,
	}, nil
}

func (p GlobalIndexProgram) resolveIndexOwner(
	key []distribution.Scalar,
) (globalIndexOwner, error) {
	mapper := distribution.NewNativeMapperWithBucketBits(
		p.indexSpec.Arity, p.indexSpec.EffectiveBucketBits(),
	)
	point, err := mapper.PointFor(key)
	if err != nil {
		return globalIndexOwner{}, fmt.Errorf("%w: route index key: %v", ErrGlobalIndexDocument, err)
	}
	target, ok := p.indexManifest.ResolvePointTarget(point)
	if !ok {
		return globalIndexOwner{}, &CatalogError{Reason: "global index manifest does not own mapped point"}
	}
	address, err := p.snapshot.Address(target.Endpoint)
	if err != nil {
		return globalIndexOwner{}, err
	}
	bucket, _ := distribution.VirtualBucketForPoint(point, mapper.VirtualBucketBits())
	return globalIndexOwner{
		point: point, target: target, address: address, bits: mapper.VirtualBucketBits(),
		scope: distributedtxn.IntentScope{Start: uint32(bucket), End: uint32(bucket) + 1},
	}, nil
}

func (p GlobalIndexProgram) resolveBaseOwner(
	locator []distribution.Scalar,
	workspace *GlobalIndexWorkspace,
) (globalIndexOwner, error) {
	baseKey := workspace.baseScalars[:p.baseArity]
	for i := range baseKey {
		baseKey[i] = locator[p.baseOrdinals[i]]
	}
	mapper := distribution.NewNativeMapperWithBucketBits(
		p.baseSpec.Arity, p.baseSpec.EffectiveBucketBits(),
	)
	point, err := mapper.PointFor(baseKey)
	if err != nil {
		return globalIndexOwner{}, fmt.Errorf("%w: route base locator: %v", ErrGlobalIndexDocument, err)
	}
	target, ok := p.baseManifest.ResolvePointTarget(point)
	if !ok {
		return globalIndexOwner{}, &CatalogError{Reason: "base manifest does not own global index locator"}
	}
	address, err := p.snapshot.Address(target.Endpoint)
	if err != nil {
		return globalIndexOwner{}, err
	}
	bucket, _ := distribution.VirtualBucketForPoint(point, mapper.VirtualBucketBits())
	return globalIndexOwner{
		point: point, target: target, address: address, bits: mapper.VirtualBucketBits(),
		scope: distributedtxn.IntentScope{Start: uint32(bucket), End: uint32(bucket) + 1},
	}, nil
}

var globalIndexStringEncoder, globalIndexStringEncoderError = vibejson.CompileEncoder[string](vibejson.EncoderOptions{DisableHTMLEscaping: true})

func appendGlobalIndexLocatorValue(
	dst []byte,
	locator []distribution.Scalar,
) ([]byte, error) {
	if globalIndexStringEncoderError != nil {
		return dst, fmt.Errorf("%w: compile string encoder: %v",
			ErrGlobalIndexDocument, globalIndexStringEncoderError)
	}
	start := len(dst)
	dst = append(dst, '[')
	for i := range locator {
		if i != 0 {
			dst = append(dst, ',')
		}
		switch locator[i].Kind() {
		case distribution.KindString:
			value, _ := locator[i].StringValue()
			if !utf8.ValidString(value) {
				return dst[:start], fmt.Errorf(
					"%w: locator %d is not valid UTF-8",
					ErrGlobalIndexDocument, i,
				)
			}
			var err error
			dst, err = globalIndexStringEncoder.AppendJSON(dst, &value)
			if err != nil {
				return dst[:start], fmt.Errorf(
					"%w: encode locator %d: %v",
					ErrGlobalIndexDocument, i, err,
				)
			}
		case distribution.KindNumber:
			value, _ := locator[i].NumberSpelling()
			dst = append(dst, value...)
		default:
			return dst[:start], fmt.Errorf(
				"%w: locator %d has invalid scalar kind",
				ErrGlobalIndexDocument, i,
			)
		}
	}
	return append(dst, ']'), nil
}

func extractGlobalIndexScalars(
	root vibejson.Node,
	pointers []vibejson.CompiledPointer,
	destination []distribution.Scalar,
	decoded []byte,
	label string,
	rawArray *[]byte,
) ([]byte, error) {
	if rawArray != nil {
		*rawArray = append((*rawArray)[:0], '[')
	}
	for i := range pointers {
		node, found, err := root.PointerCompiled(pointers[i])
		if err != nil {
			return decoded, fmt.Errorf("%w: %s path %d: %v", ErrGlobalIndexDocument, label, i, err)
		}
		if !found {
			return decoded, fmt.Errorf("%w: %s path %d is missing", ErrGlobalIndexDocument, label, i)
		}
		value := node.Raw()
		if value.IsNull() {
			return decoded, fmt.Errorf("%w: %s path %d is null", ErrGlobalIndexDocument, label, i)
		}
		decoded, err = extractGlobalIndexScalar(
			value, &destination[i], decoded, label, i,
		)
		if err != nil {
			return decoded, err
		}
		if rawArray != nil {
			if i != 0 {
				*rawArray = append(*rawArray, ',')
			}
			*rawArray = value.AppendJSON(*rawArray)
		}
	}
	if rawArray != nil {
		*rawArray = append(*rawArray, ']')
	}
	return decoded, nil
}

func extractGlobalIndexScalar(
	value vibejson.RawValue,
	destination *distribution.Scalar,
	decoded []byte,
	label string,
	ordinal int,
) ([]byte, error) {
	if value.IsNull() {
		return decoded, fmt.Errorf(
			"%w: %s path %d is null", ErrGlobalIndexDocument, label, ordinal,
		)
	}
	switch value.Kind() {
	case jsondoc.String:
		if text, ok := value.StringBytes(); ok {
			*destination = distribution.NewString(byteview.String(text))
			return decoded, nil
		}
		start := len(decoded)
		var ok bool
		var err error
		decoded, ok, err = value.AppendText(decoded)
		if err != nil || !ok {
			return decoded, fmt.Errorf(
				"%w: %s path %d has an invalid string",
				ErrGlobalIndexDocument, label, ordinal,
			)
		}
		*destination = distribution.NewString(byteview.String(decoded[start:]))
		return decoded, nil
	case jsondoc.Number:
		number, ok := value.NumberText()
		if !ok {
			return decoded, fmt.Errorf(
				"%w: %s path %d has an invalid number",
				ErrGlobalIndexDocument, label, ordinal,
			)
		}
		parsed, err := distribution.NewNumber(number)
		if err != nil {
			return decoded, fmt.Errorf(
				"%w: %s path %d: %v", ErrGlobalIndexDocument, label, ordinal, err,
			)
		}
		*destination = parsed
		return decoded, nil
	default:
		return decoded, fmt.Errorf(
			"%w: %s path %d is not a string or number",
			ErrGlobalIndexDocument, label, ordinal,
		)
	}
}
