package durable

import (
	"crypto/rand"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Create initializes an empty durable collection in file and fences its
// first root before returning.
func Create(file *os.File, options Options) (*Collection, error) {
	if file == nil {
		return nil, fmt.Errorf("vibedb: nil collection file")
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, err
	}
	locked := true
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != 0 {
		return nil, ErrNotEmpty
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	var storeID [16]byte
	if _, err := rand.Read(storeID[:]); err != nil {
		return nil, fmt.Errorf("vibedb: create collection identity: %w", err)
	}
	// A freshly created store is ordered-primary from its first byte:
	// createInitialState publishes an empty primary graph, and the committer's
	// deferred-root checkpoint mode is selected for the buffered and journal-backed
	// synchronous lanes exactly as it is for an opened primary store.
	collection, err := newCollectionResources(
		file, normalized, storeID,
		normalized.Durability != DurabilityAsyncVisible,
	)
	if err != nil {
		return nil, err
	}
	collection.writerLocked = true
	locked = false
	if err := collection.createInitialState(); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	return collection, nil
}

// Open performs bounded recovery: it reads the two superblocks, the
// selected state root, and its top-level directory pages, then starts with an
// empty read cache. It does not scan keys, documents, or postings.
func Open(file *os.File, options Options) (*Collection, error) {
	if file == nil {
		return nil, fmt.Errorf("vibedb: nil collection file")
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, err
	}
	locked := true
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
	}()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		return nil, err
	}
	if options.PageSize != 0 &&
		options.PageSize != int(bootstrap.PageSize) ||
		options.MaxPageSize != 0 &&
			options.MaxPageSize != int(bootstrap.MaxPageSize) ||
		options.MaterializationDamageGranule !=
			int(bootstrap.MaterializationDamageGranule) {
		return nil, fmt.Errorf(
			"vibedb: collection persisted geometry mismatch",
		)
	}
	scratch := make([]byte, int(bootstrap.MaxPageSize))
	recovery, err := storeio.RecoverMutableInlineStateRoot(
		file, bootstrap.PageSize,
		bootstrap.MaterializationDamageGranule, scratch,
	)
	if err != nil {
		return nil, err
	}
	inline, root := recovery.Root, recovery.State
	rootSlot, fallbackGeneration :=
		recovery.RootSlot, recovery.FallbackGeneration
	normalized, err := normalizeOpenedFileStoreOptions(
		options, root, recovery.Catalog,
	)
	if err != nil {
		return nil, err
	}
	// The ordered-primary root is mandatory in the sole current format.
	if root.PrimaryRoot == (storeio.PageRef{}) {
		return nil, fmt.Errorf(
			"vibedb: collection has no ordered-primary root",
		)
	}
	if root.PageSize != uint32(normalized.PageSize) ||
		root.MaxPageSize != uint32(normalized.MaxPageSize) {
		return nil, fmt.Errorf("vibedb: collection options or unsupported durable catalog mismatch")
	}
	collection, err := newCollectionResources(
		file, normalized, root.StoreID,
		root.JournalID != ([16]byte{}),
	)
	if err != nil {
		return nil, err
	}
	collection.writerLocked = true
	locked = false
	if err := collection.committer.InitializeRecovery(
		root.Generation, rootSlot, fallbackGeneration,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	if recovery.JournalSequence != 0 {
		if err := collection.committer.InitializeMaterializationRecovery(
			recovery.JournalSequence, recovery.JournalSlot,
		); err != nil {
			_ = collection.closeResources()
			return nil, err
		}
	}
	freeHead := inline.FreeDelta.ExternalPrev()
	state := &fileStoreState{
		root: root, fileEnd: inline.FileEnd,
		freeHead: freeHead,
	}
	collection.inlineFree = inline.FreeDelta
	collection.pageValidator.update(state)
	if err := validateOpenedPrimaryGraph(
		collection.cache, root, state.fileEnd,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	if err := collection.setupResidentPrimaryLocked(state); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	collection.initializeFileState(state)
	// A root that names a journal must pair, replay, and recycle it before the
	// collection is reachable. A non-zero JournalID is authoritative regardless of
	// the caller's option: the store may have acknowledged mutations only the
	// journal records, so a missing or mismatched file fails closed here.
	if root.JournalID != ([16]byte{}) {
		// A journaled root reopens only on a lane that consults the journal:
		// buffered-visible (its opt-in) or the synchronous lane (where the journal
		// is how sync acknowledges). Async-visible has no journal lane, so it must
		// not adopt a root that may reference acknowledgements only the journal
		// records.
		if !collection.buffered() && !collection.synchronous() {
			_ = collection.closeResources()
			return nil, fmt.Errorf(
				"vibedb: journaled store must reopen buffered-visible or synchronous")
		}
		if err := collection.openRecoveryJournalLocked(
			root.JournalID, root.Generation,
		); err != nil {
			_ = collection.closeResources()
			return nil, err
		}
		if err := collection.replayRecoveryJournalLocked(root.Generation); err != nil {
			_ = collection.closeResources()
			return nil, err
		}
	}
	return collection, nil
}

// createInitialState builds a fresh, empty ordered-primary collection. A newly
// created store is primary-layout from its first byte: the root names an empty
// primary graph (one empty leaf spanning the whole key range) and, when the
// collection is indexed, an empty exact-index root. The first Put routes to that
// empty leaf and fills it. A configured schema is recorded in the root's option
// flags and catalog and enforced on every primary Put.
func (c *Collection) createInitialState() error {
	if c.options.PageSize != 4096 {
		return fmt.Errorf(
			"vibedb: ordered-primary collection requires 4 KiB pages",
		)
	}
	if uint32(c.options.MaxPageSize) < storeio.GlobalTabletCatalogRootBytes {
		return fmt.Errorf(
			"vibedb: ordered-primary collection requires a %d-byte maximum page",
			storeio.GlobalTabletCatalogRootBytes,
		)
	}
	layout, err := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if err != nil {
		return err
	}
	catalog, err := planFilePageCatalog(
		c.options.pageCatalog, c.cacheStoreID(), 1,
		uint32(c.options.PageSize), layout.DataStart,
		1,
	)
	if err != nil {
		return err
	}
	initialFileEnd := catalog.fileEnd
	if err := c.file.Truncate(int64(initialFileEnd)); err != nil {
		return err
	}
	if catalog.segments != 0 {
		catalogScratch := make([]byte, c.options.PageSize)
		if err := catalog.write(
			c.file, initialFileEnd, catalog.nextID, catalogScratch,
		); err != nil {
			return err
		}
		if err := c.file.Sync(); err != nil {
			return err
		}
	}
	// One leaf/tablet/catalog for the empty graph, plus one exact-index root when
	// indexed. PublishInline uses the Batch's dedicated alternate-root buffer, so
	// it does not consume a transaction-page slot.
	reserve := storeio.EmptyPrimaryGraphPageCount
	if len(c.options.indexes) != 0 {
		reserve++
	}
	// The primary graph draws its dynamically allocated pages from the reserved
	// primary namespace, so the transaction starts at PrimaryFirstDynamicLogicalID
	// exactly as CreateFromPrimary does; the page catalog's own logical IDs live
	// below that range and are written with catalog.nextID above.
	tx, err := c.beginWriteTransaction(reserve, storeio.WriteTransactionOptions{
		StoreID: c.cacheStoreID(), Generation: 1, PageSize: uint32(c.options.PageSize),
		FileEnd: initialFileEnd, NextLogicalID: storeio.PrimaryFirstDynamicLogicalID,
	})
	if err != nil {
		return err
	}
	primaryRoot, err := storeio.BuildEmptyPrimaryGraph(tx)
	if err != nil {
		_ = tx.Abort()
		return err
	}
	exactIndexRoot, err := buildPrimaryExactIndexes(
		tx, nil, nil, c.options.indexes,
		uint32(c.options.PageSize), uint32(c.options.MaxPageSize),
	)
	if err != nil {
		_ = tx.Abort()
		return err
	}
	root := storeio.StateRoot{
		StoreID: c.cacheStoreID(), Generation: 1, PageSize: uint32(c.options.PageSize),
		NextLogicalID: tx.NextLogicalID(),
		IndexCount:    uint32(len(c.options.indexes)), IndexCatalogHash: c.options.indexCatalogHash,
		IndexMaxDepth:    uint32(max(c.options.Collection.IndexOptions.MaxDepth, 0)),
		MaxKeyBytes:      uint32(c.options.MaxKeyBytes),
		InlineValueBytes: uint32(c.options.InlineValueBytes),
		MaxDocumentBytes: uint32(c.options.MaxDocumentBytes),
		PrimaryRoot:      primaryRoot,
		ExactIndexRoot:   exactIndexRoot,
	}
	root.Options = fileStoreCollectionOptionFlags(c.options.Collection)
	if c.options.MaterializationDamageGranule != 0 {
		root.Options |= storeio.StateOptionCanonicalMaterialization
		root.MaterializationDamageGranule =
			uint32(c.options.MaterializationDamageGranule)
	}
	if err := catalog.apply(&root, uint32(c.options.MaxPageSize)); err != nil {
		_ = tx.Abort()
		return err
	}
	// Mint the paired journal before the root that names it is published, so a
	// crash after the root is durable finds the journal file present. The
	// synchronous lane is journal-backed on the primary graph unconditionally --
	// it is how sync acknowledges -- and explicit RecoveryJournal mode needs one
	// before its first acknowledged mutation. Ordinary buffered-visible stores
	// defer the sibling until their first valid mutation. Async-visible never
	// carries one. This is the
	// creation-time counterpart of CreateFromPrimary's journal mint.
	journalRequired := c.options.Durability == DurabilitySync ||
		c.journalConfigured()
	if journalRequired {
		var journalID [16]byte
		if _, err := rand.Read(journalID[:]); err != nil {
			_ = tx.Abort()
			return fmt.Errorf("vibedb: mint recovery journal identity: %w", err)
		}
		if err := createSiblingRecoveryJournal(
			c.file.Name(),
			recoveryJournalHeaderFor(
				c.cacheStoreID(), journalID, uint32(c.options.PageSize),
				c.options.MaxKeyBytes, c.options.InlineValueBytes,
				c.options.MaxDocumentBytes, 1,
				func() int {
					if c.buffered() && !c.options.RecoveryJournal {
						return c.options.primaryUnifiedOverlayBytes
					}
					return 0
				}(),
			),
		); err != nil {
			_ = tx.Abort()
			return err
		}
		root.JournalID = journalID
	}
	inlineFree := storeio.NewInlineFreeDelta(storeio.PageRef{}, storeio.PageRef{})
	if err := tx.PublishInline(root, inlineFree); err != nil {
		_ = tx.Abort()
		return err
	}
	if err := c.committer.Flush(); err != nil {
		return err
	}
	c.cache.MarkDurable(1)
	state := &fileStoreState{root: root, fileEnd: tx.FileEnd()}
	c.inlineFree = inlineFree
	c.pageValidator.update(state)
	c.initializeFileState(state)
	c.freeLoaded = true
	if err := c.setupResidentPrimaryLocked(state); err != nil {
		return err
	}
	// Pair, replay, and recycle the fresh journal exactly as Open does, so a
	// created and an opened journaled collection reach an identical live state.
	// A freshly minted journal holds no records, so replay is a clean no-op.
	if journalRequired {
		if err := c.openRecoveryJournalLocked(root.JournalID, root.Generation); err != nil {
			return err
		}
		if err := c.replayRecoveryJournalLocked(root.Generation); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collection) cacheStoreID() [16]byte {
	return c.storeID
}
