package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/distribution"
)

var ErrRetainedPruneCertificate = errors.New("gateway: invalid retained prune certificate")

const RetainedPruneCertificateBytes = 512

var (
	retainedPruneCertificateMagic  = [8]byte{'V', 'D', 'B', 'R', 'P', 'R', 'N', 0}
	retainedPruneCertificateDomain = []byte("vibedb/gateway/retained-prune-certificate\x00")
	manifestDigestDomain           = []byte("vibedb/gateway/distribution-manifest\x00")
	retainedRangeLineageDomain     = []byte("vibedb/gateway/retained-range-lineage\x00")
)

// RetainedPruneCertificateBinding is the complete fixed-width destructive
// authority cut. TargetManifestDigest authenticates the exact published
// routing manifest; RetainedRangeLineage authenticates the retained interval
// within that manifest rather than merely naming its endpoints.
type RetainedPruneCertificateBinding struct {
	Generation           uint64
	Operation            [sha256.Size]byte
	PlanDigest           [sha256.Size]byte
	CutoverDigest        [sha256.Size]byte
	TargetManifestDigest [sha256.Size]byte
	RetainedRange        distribution.KeyRange
	RetainedRangeLineage [sha256.Size]byte
}

func (binding RetainedPruneCertificateBinding) valid() bool {
	return binding.Generation != 0 && binding.Operation != ([sha256.Size]byte{}) &&
		binding.PlanDigest != ([sha256.Size]byte{}) && binding.CutoverDigest != ([sha256.Size]byte{}) &&
		binding.TargetManifestDigest != ([sha256.Size]byte{}) && binding.RetainedRange.Valid() &&
		binding.RetainedRangeLineage == RetainedRangeLineageDigest(
			binding.TargetManifestDigest, binding.RetainedRange,
		)
}

// RetainedPruneCertificate is immutable gateway evidence that the exact target
// catalog was published and the exact serving-gateway roster drained every
// lease older than that publication. Its canonical codec is the sole wire
// grammar; no interface or process-local catalog handle crosses to a shard.
type RetainedPruneCertificate struct {
	binding RetainedPruneCertificateBinding
	drain   ClusterCatalogDrainCertificate
	proof   [sha256.Size]byte
}

func NewRetainedPruneCertificate(
	binding RetainedPruneCertificateBinding,
	drain ClusterCatalogDrainCertificate,
) (RetainedPruneCertificate, error) {
	if !binding.valid() || !validClusterCatalogDrainCertificate(drain) ||
		drain.Request.Operation != binding.Operation ||
		drain.Request.Generation != binding.Generation {
		return RetainedPruneCertificate{}, ErrRetainedPruneCertificate
	}
	certificate := RetainedPruneCertificate{binding: binding, drain: drain}
	certificate.proof = retainedPruneCertificateProof(binding, drain)
	return certificate, nil
}

func (certificate RetainedPruneCertificate) Binding() RetainedPruneCertificateBinding {
	return certificate.binding
}

func (certificate RetainedPruneCertificate) CatalogDrain() ClusterCatalogDrainCertificate {
	return certificate.drain
}

func (certificate RetainedPruneCertificate) Digest() [sha256.Size]byte { return certificate.proof }

func (certificate RetainedPruneCertificate) ValidFor(binding RetainedPruneCertificateBinding) bool {
	return binding.valid() && certificate.binding == binding &&
		validClusterCatalogDrainCertificate(certificate.drain) &&
		certificate.drain.Request.Operation == binding.Operation &&
		certificate.drain.Request.Generation == binding.Generation &&
		certificate.proof == retainedPruneCertificateProof(binding, certificate.drain)
}

func validClusterCatalogDrainCertificate(certificate ClusterCatalogDrainCertificate) bool {
	return certificate.ValidFor(certificate.Request) &&
		certificate.Proof == clusterCatalogDrainCertificateProof(
			certificate.FenceDigest, certificate.RosterDigest,
		)
}

// DistributionManifestDigest hashes every routing field in manifest order.
// Length prefixes make the mapping injective without allocating a JSON image.
func DistributionManifestDigest(manifest *distribution.Manifest) ([sha256.Size]byte, error) {
	if manifest == nil || manifest.ShardCount() == 0 {
		return [sha256.Size]byte{}, ErrRetainedPruneCertificate
	}
	hash := sha256.New()
	_, _ = hash.Write(manifestDigestDomain)
	writeRetainedPruneString(hash, string(manifest.Distribution()))
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(manifest.Version()))
	_, _ = hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(manifest.ShardCount()))
	_, _ = hash.Write(scalar[:])
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		shard, ok := manifest.ShardInfo(ordinal)
		if !ok || !shard.Range.Valid() {
			return [sha256.Size]byte{}, ErrRetainedPruneCertificate
		}
		writeRetainedPruneString(hash, string(shard.ID))
		binary.LittleEndian.PutUint64(scalar[:], uint64(shard.AllocationGeneration))
		_, _ = hash.Write(scalar[:])
		binary.LittleEndian.PutUint64(scalar[:], uint64(shard.Epoch))
		_, _ = hash.Write(scalar[:])
		_, _ = hash.Write(shard.Range.Start[:])
		_, _ = hash.Write(shard.Range.End.Point[:])
		if shard.Range.End.Max {
			_, _ = hash.Write([]byte{1})
		} else {
			_, _ = hash.Write([]byte{0})
		}
		binary.LittleEndian.PutUint64(scalar[:], uint64(len(shard.Leaders)))
		_, _ = hash.Write(scalar[:])
		for _, leader := range shard.Leaders {
			writeRetainedPruneString(hash, string(leader))
		}
	}
	var result [sha256.Size]byte
	hash.Sum(result[:0])
	return result, nil
}

func writeRetainedPruneString(hash interface{ Write([]byte) (int, error) }, value string) {
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write([]byte(value))
}

func RetainedRangeLineageDigest(
	targetManifest [sha256.Size]byte, retained distribution.KeyRange,
) [sha256.Size]byte {
	if targetManifest == ([sha256.Size]byte{}) || !retained.Valid() {
		return [sha256.Size]byte{}
	}
	hash := sha256.New()
	_, _ = hash.Write(retainedRangeLineageDomain)
	_, _ = hash.Write(targetManifest[:])
	_, _ = hash.Write(retained.Start[:])
	_, _ = hash.Write(retained.End.Point[:])
	if retained.End.Max {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	var result [sha256.Size]byte
	hash.Sum(result[:0])
	return result
}

func AppendRetainedPruneCertificate(
	dst []byte, certificate RetainedPruneCertificate,
) ([]byte, error) {
	if !certificate.ValidFor(certificate.binding) ||
		len(dst) > math.MaxInt-RetainedPruneCertificateBytes {
		return dst, ErrRetainedPruneCertificate
	}
	start := len(dst)
	dst = append(dst, make([]byte, RetainedPruneCertificateBytes)...)
	raw := dst[start:]
	encodeRetainedPruneCertificateBody(raw[:448], certificate.binding, certificate.drain)
	copy(raw[448:480], certificate.proof[:])
	checksum := sha256.Sum256(raw[:480])
	copy(raw[480:], checksum[:])
	return dst, nil
}

func OpenRetainedPruneCertificate(raw []byte) (RetainedPruneCertificate, error) {
	if len(raw) != RetainedPruneCertificateBytes ||
		!bytes.Equal(raw[:8], retainedPruneCertificateMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != 1 ||
		!allZeroRetainedPrune(raw[10:16]) ||
		sha256.Sum256(raw[:480]) != [sha256.Size]byte(raw[480:512]) {
		return RetainedPruneCertificate{}, ErrRetainedPruneCertificate
	}
	binding := RetainedPruneCertificateBinding{Generation: binary.LittleEndian.Uint64(raw[16:24])}
	copy(binding.Operation[:], raw[24:56])
	copy(binding.PlanDigest[:], raw[56:88])
	copy(binding.CutoverDigest[:], raw[88:120])
	copy(binding.TargetManifestDigest[:], raw[120:152])
	copy(binding.RetainedRange.Start[:], raw[152:160])
	copy(binding.RetainedRange.End.Point[:], raw[160:168])
	if raw[168] > 1 || !allZeroRetainedPrune(raw[169:176]) {
		return RetainedPruneCertificate{}, ErrRetainedPruneCertificate
	}
	binding.RetainedRange.End.Max = raw[168] == 1
	copy(binding.RetainedRangeLineage[:], raw[176:208])
	drain, err := OpenClusterCatalogDrainCertificate(raw[208:448])
	if err != nil {
		return RetainedPruneCertificate{}, errors.Join(ErrRetainedPruneCertificate, err)
	}
	certificate, err := NewRetainedPruneCertificate(binding, drain)
	if err != nil || certificate.proof != [sha256.Size]byte(raw[448:480]) {
		return RetainedPruneCertificate{}, errors.Join(ErrRetainedPruneCertificate, err)
	}
	return certificate, nil
}

func encodeRetainedPruneCertificateBody(
	raw []byte, binding RetainedPruneCertificateBinding, drain ClusterCatalogDrainCertificate,
) {
	copy(raw[:8], retainedPruneCertificateMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], 1)
	binary.LittleEndian.PutUint64(raw[16:24], binding.Generation)
	copy(raw[24:56], binding.Operation[:])
	copy(raw[56:88], binding.PlanDigest[:])
	copy(raw[88:120], binding.CutoverDigest[:])
	copy(raw[120:152], binding.TargetManifestDigest[:])
	copy(raw[152:160], binding.RetainedRange.Start[:])
	copy(raw[160:168], binding.RetainedRange.End.Point[:])
	if binding.RetainedRange.End.Max {
		raw[168] = 1
	}
	copy(raw[176:208], binding.RetainedRangeLineage[:])
	drainRaw, _ := AppendClusterCatalogDrainCertificate(raw[:208], drain)
	_ = drainRaw
}

func retainedPruneCertificateProof(
	binding RetainedPruneCertificateBinding, drain ClusterCatalogDrainCertificate,
) [sha256.Size]byte {
	var raw [448]byte
	encodeRetainedPruneCertificateBody(raw[:], binding, drain)
	hash := sha256.New()
	_, _ = hash.Write(retainedPruneCertificateDomain)
	_, _ = hash.Write(raw[:])
	var proof [sha256.Size]byte
	hash.Sum(proof[:0])
	return proof
}

func allZeroRetainedPrune(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}
