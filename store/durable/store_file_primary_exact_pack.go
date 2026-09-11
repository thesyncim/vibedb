package durable

import (
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
)

type packPendingLeaf struct {
	leaf      *primaryExactLeaf
	encoded   []byte
	firstKey  []byte
	firstTile uint32
	piece     bool
	runCut    bool
}

// primaryExactPackBuilder fills per-index LZ4/raw packs from dirty term
// leaves. Carried leaves keep their existing pack identity; dirty leaves may
// share a pack even when they are not adjacent in catalog/term order. Member
// ordinals are pack-local; the catalog maps each leaf independently.
type primaryExactPackBuilder struct {
	sink                  storeio.PrimaryGraphBuildSink
	pageSize, maxPageSize uint32
	indexID               uint32
	owned                 storeio.PrimaryExactPackEncoder
	encoder               *storeio.PrimaryExactPackEncoder
	pending               []packPendingLeaf
	staged                []primaryExactStagedLeaf
	wire                  []byte
}

func newPrimaryExactPackBuilder(
	sink storeio.PrimaryGraphBuildSink, pageSize, maxPageSize, indexID uint32,
	reuse *storeio.PrimaryExactPackEncoder,
	wire []byte,
) (*primaryExactPackBuilder, error) {
	maxLeaf := storeio.IndexTermLeafPackCutBudget(maxPageSize)
	if sink == nil || pageSize == 0 || maxPageSize < pageSize || maxLeaf < 1 {
		return nil, storeio.ErrInvalidWrite
	}
	if cap(wire) < storeio.PrimaryExactPackMaxPayloadBytes {
		wire = make([]byte, 0, storeio.PrimaryExactPackMaxPayloadBytes)
	} else {
		wire = wire[:0]
	}
	builder := &primaryExactPackBuilder{
		sink: sink, pageSize: pageSize, maxPageSize: maxPageSize, indexID: indexID,
		wire: wire,
	}
	if reuse != nil {
		builder.encoder = reuse
	} else {
		builder.encoder = &builder.owned
	}
	if err := builder.encoder.Prepare(storeio.PrimaryExactPackMaxMembers, maxLeaf); err != nil {
		return nil, err
	}
	return builder, nil
}

func (b *primaryExactPackBuilder) AddLeaf(leaf *primaryExactLeaf) error {
	if b == nil || leaf == nil {
		return storeio.ErrInvalidWrite
	}
	return b.add(leaf, leaf.encoded, leaf.firstKey, leaf.firstTile, leaf.piece, leaf.runCut)
}

func (b *primaryExactPackBuilder) AddEncoded(
	encoded, firstKey []byte, firstTile uint32, piece, runCut bool,
) error {
	return b.add(nil, encoded, firstKey, firstTile, piece, runCut)
}

func (b *primaryExactPackBuilder) add(
	leaf *primaryExactLeaf, encoded, firstKey []byte, firstTile uint32, piece, runCut bool,
) error {
	if b == nil || len(encoded) == 0 || len(firstKey) == 0 {
		return storeio.ErrInvalidWrite
	}
	if err := b.encoder.Append(b.indexID, encoded); err != nil {
		if len(b.pending) == 0 {
			return err
		}
		if err := b.flush(); err != nil {
			return err
		}
		if err := b.encoder.Append(b.indexID, encoded); err != nil {
			return err
		}
	}
	key := firstKey
	if leaf == nil {
		key = append([]byte(nil), firstKey...)
	}
	b.pending = append(b.pending, packPendingLeaf{
		leaf: leaf, encoded: encoded,
		firstKey:  key,
		firstTile: firstTile, piece: piece, runCut: runCut,
	})
	return nil
}

func (b *primaryExactPackBuilder) Finish() error {
	if b == nil {
		return storeio.ErrInvalidWrite
	}
	return b.flush()
}

func (b *primaryExactPackBuilder) appendStagedTo(dst []primaryExactStagedLeaf) []primaryExactStagedLeaf {
	if b == nil || len(b.staged) == 0 {
		return dst
	}
	dst = append(dst, b.staged...)
	b.staged = b.staged[:0]
	return dst
}

func (b *primaryExactPackBuilder) flush() error {
	if len(b.pending) == 0 {
		return nil
	}
	b.wire = slices.Grow(b.wire[:0], storeio.PrimaryExactPackMaxPayloadBytes)
	wire, err := b.encoder.Encode(b.wire[:0], int(b.pageSize))
	if err != nil {
		return err
	}
	b.wire = wire
	extent, ok := primaryExactPackExtent(len(wire), b.pageSize, b.maxPageSize)
	if !ok {
		return fmt.Errorf("%w: exact pack extent", storeio.ErrPrimaryExactIndexCorrupt)
	}
	page, err := b.sink.AllocatePage(storeio.PagePrimaryExactPack, extent, 0)
	if err != nil {
		return err
	}
	if _, err := storeio.EncodePrimaryExactPackPage(
		page.Bytes(), b.sink.StoreIdentity(), b.sink.BuildGeneration(),
		page.Ref().LogicalID, wire,
	); err != nil {
		return err
	}
	if err := page.Stage(); err != nil {
		return err
	}
	ref := page.Ref()
	for i := range b.pending {
		member := uint16(i)
		if b.pending[i].leaf != nil {
			b.pending[i].leaf.ref = ref
			b.pending[i].leaf.member = member
		}
		b.staged = append(b.staged, primaryExactStagedLeaf{
			ref: ref, member: member, firstKey: b.pending[i].firstKey,
			firstTile: b.pending[i].firstTile, piece: b.pending[i].piece,
			runCut: b.pending[i].runCut,
		})
	}
	b.encoder.Reset()
	b.pending = b.pending[:0]
	return nil
}

func drainExactPackStaged(
	builder *primaryExactPackBuilder, catalog *primaryExactCatalogStream,
) error {
	if builder == nil || catalog == nil {
		return storeio.ErrInvalidWrite
	}
	for _, staged := range builder.staged {
		if err := catalog.Add(
			staged.ref, staged.member, staged.firstKey, staged.firstTile,
			staged.piece, staged.runCut,
		); err != nil {
			return err
		}
	}
	builder.staged = builder.staged[:0]
	return nil
}

// stagePackedExactLeaves persists dirty leaves as dense per-index packs and
// carries already-durable members unchanged. Catalog order stays term order;
// pack member order is the dirty-leaf encounter order and need not match.
func stagePackedExactLeaves(
	sink storeio.PrimaryGraphBuildSink,
	pageSize, maxPageSize, indexID uint32,
	leaves []primaryExactLeaf,
	staged []primaryExactStagedLeaf,
	reuse *storeio.PrimaryExactPackEncoder,
	wire *[]byte,
) ([]primaryExactStagedLeaf, error) {
	var reuseWire []byte
	if wire != nil {
		reuseWire = *wire
	}
	pack, err := newPrimaryExactPackBuilder(
		sink, pageSize, maxPageSize, indexID, reuse, reuseWire,
	)
	if err != nil {
		return nil, err
	}
	if wire != nil {
		defer func() { *wire = pack.wire }()
	}
	for at := range leaves {
		if leaves[at].ref != (storeio.PageRef{}) {
			continue
		}
		if err := pack.AddLeaf(&leaves[at]); err != nil {
			return nil, err
		}
	}
	if err := pack.Finish(); err != nil {
		return nil, err
	}
	if cap(staged) < len(leaves) {
		staged = make([]primaryExactStagedLeaf, 0, len(leaves))
	} else {
		staged = staged[:0]
	}
	for at := range leaves {
		leaf := &leaves[at]
		if leaf.ref == (storeio.PageRef{}) {
			return nil, fmt.Errorf("%w: exact pack member", storeio.ErrPrimaryExactIndexCorrupt)
		}
		staged = append(staged, primaryExactStagedLeaf{
			ref: leaf.ref, member: leaf.member, firstKey: leaf.firstKey,
			firstTile: leaf.firstTile, piece: leaf.piece, runCut: leaf.runCut,
		})
	}
	return staged, nil
}

// primaryExactPackExtent rounds a sealed pack payload up to the configured
// allocation quantum instead of the next power of two.
func primaryExactPackExtent(packPayload int, pageSize, maxPageSize uint32) (uint32, bool) {
	if packPayload < 1 || pageSize == 0 {
		return 0, false
	}
	need := uint64(packPayload) + uint64(storeio.PageHeaderSize) + uint64(storeio.PageTrailerSize)
	quantum := uint64(pageSize)
	extent := (need + quantum - 1) / quantum * quantum
	return uint32(extent), extent >= quantum &&
		extent <= uint64(maxPageSize) &&
		extent <= uint64(storeio.PrimaryExactPackMaxDecodedBytes)
}
