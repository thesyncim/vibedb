package replicatedstate

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/store/durable"
)

// SchemaImageAudit retains a completed row audit, not serving authority. Its
// opaque durable identities allow first schema activation to reuse canonical
// roots after reopening the exact unchanged files. It retains no collection
// handles or row buffers. A zero value, changed image, or different binding is
// rejected; callers must re-audit outside the write fence after any mismatch.
type SchemaImageAudit struct {
	binding     Binding
	certificate RelationImageCertificate
	images      []auditedSchemaImage
}

type auditedSchemaImage struct {
	identity  durable.ImageIdentity
	root      [sha256.Size]byte
	placement relationPlacementAccumulator
}

// AuditSchemaImages performs the same complete audit as CertifyRelationImages
// and additionally binds its roots to unchanged durable image identities.
// Callers must exclude target writes through the later activation handoff.
func AuditSchemaImages(binding Binding, specs []RelationCollection) (*SchemaImageAudit, error) {
	audit := &SchemaImageAudit{}
	if _, err := certifyRelationImages(binding, specs, audit); err != nil {
		return nil, err
	}
	return audit, nil
}

func (a *SchemaImageAudit) Certificate() RelationImageCertificate {
	if a == nil {
		return RelationImageCertificate{}
	}
	return a.certificate
}

func (a *SchemaImageAudit) matches(binding Binding, manifest [32]byte, relations []relationCollection) bool {
	if a == nil || a.binding != binding || !a.certificate.Valid() ||
		a.certificate.ManifestDigest != manifest || len(a.images) != len(relations) ||
		len(a.images) != int(a.certificate.RelationCount) {
		return false
	}
	for i := range relations {
		if !relations[i].target.Collection.MatchesDurableImage(a.images[i].identity) {
			return false
		}
	}
	return true
}
