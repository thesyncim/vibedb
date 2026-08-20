package gateway

import (
	"errors"
	"fmt"

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
}

// GlobalIndexRoute is one extracted index entry plus both independently
// resolved owners. Byte slices alias the workspace until its next call.
type GlobalIndexRoute struct {
	Index IndexMetadata

	KeyTuple     []byte
	LocatorTuple []byte
	EntryKey     []byte

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
		root, p.keyPointers, key, workspace.decoded, "key",
	)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	workspace.decoded, err = extractGlobalIndexScalars(
		root, p.locatorPointers, locator, workspace.decoded, "locator",
	)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
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

	indexMapper := distribution.NewNativeMapperWithBucketBits(
		p.indexSpec.Arity, p.indexSpec.EffectiveBucketBits(),
	)
	indexPoint, err := indexMapper.PointFor(key)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: route index key: %v", ErrGlobalIndexDocument, err)
	}
	indexTarget, ok := p.indexManifest.ResolvePointTarget(indexPoint)
	if !ok {
		return GlobalIndexRoute{}, &CatalogError{Reason: "global index manifest does not own mapped point"}
	}
	indexAddress, err := p.snapshot.Address(indexTarget.Endpoint)
	if err != nil {
		return GlobalIndexRoute{}, err
	}

	baseKey := workspace.baseScalars[:p.baseArity]
	for i := range baseKey {
		baseKey[i] = locator[p.baseOrdinals[i]]
	}
	baseMapper := distribution.NewNativeMapperWithBucketBits(
		p.baseSpec.Arity, p.baseSpec.EffectiveBucketBits(),
	)
	basePoint, err := baseMapper.PointFor(baseKey)
	if err != nil {
		return GlobalIndexRoute{}, fmt.Errorf("%w: route base locator: %v", ErrGlobalIndexDocument, err)
	}
	baseTarget, ok := p.baseManifest.ResolvePointTarget(basePoint)
	if !ok {
		return GlobalIndexRoute{}, &CatalogError{Reason: "base manifest does not own global index locator"}
	}
	baseAddress, err := p.snapshot.Address(baseTarget.Endpoint)
	if err != nil {
		return GlobalIndexRoute{}, err
	}
	indexBucket, _ := distribution.VirtualBucketForPoint(indexPoint, indexMapper.VirtualBucketBits())
	baseBucket, _ := distribution.VirtualBucketForPoint(basePoint, baseMapper.VirtualBucketBits())
	return GlobalIndexRoute{
		Index:    p.metadata,
		KeyTuple: workspace.keyTuple, LocatorTuple: workspace.locatorTuple,
		EntryKey:   workspace.entryKey,
		IndexPoint: indexPoint, IndexTarget: indexTarget, IndexAddress: indexAddress,
		IndexBucketBits: indexMapper.VirtualBucketBits(),
		IndexScope:      distributedtxn.IntentScope{Start: uint32(indexBucket), End: uint32(indexBucket) + 1},
		BasePoint:       basePoint, BaseTarget: baseTarget, BaseAddress: baseAddress,
		BaseBucketBits: baseMapper.VirtualBucketBits(),
		BaseScope:      distributedtxn.IntentScope{Start: uint32(baseBucket), End: uint32(baseBucket) + 1},
	}, nil
}

func extractGlobalIndexScalars(
	root vibejson.Node,
	pointers []vibejson.CompiledPointer,
	destination []distribution.Scalar,
	decoded []byte,
	label string,
) ([]byte, error) {
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
		switch value.Kind() {
		case jsondoc.String:
			if text, ok := value.StringBytes(); ok {
				destination[i] = distribution.NewString(byteview.String(text))
				continue
			}
			start := len(decoded)
			var ok bool
			decoded, ok, err = value.AppendText(decoded)
			if err != nil || !ok {
				return decoded, fmt.Errorf("%w: %s path %d has an invalid string", ErrGlobalIndexDocument, label, i)
			}
			destination[i] = distribution.NewString(byteview.String(decoded[start:]))
		case jsondoc.Number:
			number, ok := value.NumberText()
			if !ok {
				return decoded, fmt.Errorf("%w: %s path %d has an invalid number", ErrGlobalIndexDocument, label, i)
			}
			destination[i], err = distribution.NewNumber(number)
			if err != nil {
				return decoded, fmt.Errorf("%w: %s path %d: %v", ErrGlobalIndexDocument, label, i, err)
			}
		default:
			return decoded, fmt.Errorf("%w: %s path %d is not a string or number", ErrGlobalIndexDocument, label, i)
		}
	}
	return decoded, nil
}
