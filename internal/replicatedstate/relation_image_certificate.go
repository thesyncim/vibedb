package replicatedstate

import (
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
)

var relationImageCertificateDomain = []byte(
	"vibedb/replicated-state/relation-image-certificate\x00",
)

// RelationImageCertificate is a constant-size proof over one fully opened,
// schema-generation-bound target bundle. CardinalityRoot commits every dense
// relation's row count and canonical content root; PlacementDigest separately
// commits global-index ownership, outside-range counts, XOR, and modular-sum
// accumulators. Witness binds the complete certificate for a rollout receipt.
type RelationImageCertificate struct {
	SchemaGeneration  uint64
	RelationCount     uint16
	TotalRows         uint64
	NonEmptyRelations uint64
	ManifestDigest    [sha256.Size]byte
	ImageRoot         [sha256.Size]byte
	CardinalityRoot   [sha256.Size]byte
	PlacementDigest   [sha256.Size]byte
	Witness           [sha256.Size]byte
}

// Valid verifies every certificate field against its existing fixed witness.
// This does not certify new images: it authenticates a detached certificate
// against the witness retained by the prepared rollout owner.
func (certificate RelationImageCertificate) Valid() bool {
	return certificate.SchemaGeneration != 0 && certificate.RelationCount != 0 &&
		certificate.RelationCount <= replication.MaxRelationsPerBundle &&
		certificate.ManifestDigest != ([sha256.Size]byte{}) && certificate.ImageRoot != ([sha256.Size]byte{}) &&
		certificate.CardinalityRoot != ([sha256.Size]byte{}) && certificate.Witness == relationImageCertificateWitness(certificate)
}

// BundleApplyContractOptions is the constant-size part of Options that enters
// deterministic command/result semantics. Storage, checkpoint, capture, and
// transaction-log handles are deliberately absent because healthy replicas
// may differ in those local details.
type BundleApplyContractOptions struct {
	MaxSessions                      uint64
	RetryWindow                      uint16
	RequestLedgerCapacityBytes       uint64
	RequestLedgerCleanupReserveBytes uint64
	RequestLedgerRange               RequestLedgerRange
}

// RelationBundleApplyContractDigest derives the exact target contract from
// the same validated relation descriptors used by OpenBundle.
func RelationBundleApplyContractDigest(
	binding Binding,
	specs []RelationCollection,
	options BundleApplyContractOptions,
) ([sha256.Size]byte, error) {
	relations, manifest, err := prepareRelationCollections(binding, specs)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return bundleApplyContractDigest(
		manifest, relations, options.MaxSessions, options.RetryWindow,
		options.RequestLedgerCapacityBytes, options.RequestLedgerCleanupReserveBytes,
		options.RequestLedgerRange, routeGateRecordLimit(),
	)
}

// CertifyRelationImages scans each already materialized relation exactly once
// at a durable snapshot. The scan validates native exact-index catalogs,
// deterministic row grammars, global-index locator values, and placement.
// Memory is O(relation count), never O(row count).
func CertifyRelationImages(
	binding Binding,
	specs []RelationCollection,
) (RelationImageCertificate, error) {
	relations, manifest, err := prepareRelationCollections(binding, specs)
	if err != nil {
		return RelationImageCertificate{}, err
	}
	cardinality := sha256.New()
	_, _ = cardinality.Write(relationImageCertificateDomain)
	_, _ = cardinality.Write(manifest[:])
	var fixed [18]byte
	binary.LittleEndian.PutUint64(fixed[0:8], binding.SchemaGeneration)
	binary.LittleEndian.PutUint16(fixed[8:10], uint16(len(relations)))
	_, _ = cardinality.Write(fixed[:10])
	var total, nonEmpty uint64
	for ordinal := range relations {
		snapshot, snapshotErr := relations[ordinal].target.Collection.Snapshot()
		if snapshotErr != nil {
			return RelationImageCertificate{}, snapshotErr
		}
		indexErr := validateRelationIndexCatalog(snapshot, relations[ordinal].localIndexes)
		image, placement, imageErr := openedRelationImageDigest(
			&relations[ordinal], snapshot, binding.OwnedRange,
		)
		rows := snapshot.Len()
		closeErr := snapshot.Close()
		if indexErr != nil {
			return RelationImageCertificate{}, indexErr
		}
		if imageErr != nil {
			return RelationImageCertificate{}, imageErr
		}
		if closeErr != nil || rows > math.MaxUint64-total {
			if closeErr != nil {
				return RelationImageCertificate{}, closeErr
			}
			return RelationImageCertificate{}, ErrAdmissionBound
		}
		relations[ordinal].openedImage = image
		relations[ordinal].placement = placement
		total += rows
		if rows != 0 {
			nonEmpty |= uint64(1) << uint(ordinal)
		}
		binary.LittleEndian.PutUint16(fixed[0:2], uint16(relations[ordinal].id))
		binary.LittleEndian.PutUint64(fixed[2:10], rows)
		_, _ = cardinality.Write(fixed[:10])
		_, _ = cardinality.Write(image[:])
	}
	var cardinalityRoot [sha256.Size]byte
	_ = cardinality.Sum(cardinalityRoot[:0])
	imageRoot, err := canonicalRelationImageDigest(relations)
	if err != nil {
		return RelationImageCertificate{}, err
	}
	placement := relationPlacementStateDigest(binding.SchemaGeneration, manifest, relations)
	certificate := RelationImageCertificate{
		SchemaGeneration: binding.SchemaGeneration,
		RelationCount:    uint16(len(relations)), TotalRows: total,
		NonEmptyRelations: nonEmpty,
		ManifestDigest:    manifest, ImageRoot: imageRoot,
		CardinalityRoot: cardinalityRoot, PlacementDigest: placement,
	}
	certificate.Witness = relationImageCertificateWitness(certificate)
	return certificate, nil
}

func relationImageCertificateWitness(certificate RelationImageCertificate) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(relationImageCertificateDomain)
	var fixed [18]byte
	binary.LittleEndian.PutUint64(fixed[0:8], certificate.SchemaGeneration)
	binary.LittleEndian.PutUint16(fixed[8:10], certificate.RelationCount)
	binary.LittleEndian.PutUint64(fixed[10:18], certificate.TotalRows)
	_, _ = h.Write(fixed[:])
	binary.LittleEndian.PutUint64(fixed[0:8], certificate.NonEmptyRelations)
	_, _ = h.Write(fixed[:8])
	_, _ = h.Write(certificate.ManifestDigest[:])
	_, _ = h.Write(certificate.ImageRoot[:])
	_, _ = h.Write(certificate.CardinalityRoot[:])
	_, _ = h.Write(certificate.PlacementDigest[:])
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}
