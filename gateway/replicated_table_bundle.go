package gateway

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/thesyncim/vibejson"
)

// ReplicatedTableProvisionBundleFormat is the format of the local, immutable
// table-provisioning bundle. The bundle is a supervisor/runtime handoff; it is
// deliberately separate from the public replica-control manifest.
const ReplicatedTableProvisionBundleFormat uint16 = 1

// ErrInvalidTableProvisionBundle identifies a malformed or non-canonical
// local provisioning bundle. The catalog fragment inside a valid bundle is
// still checked by OpenReplicatedTableProvision.
var ErrInvalidTableProvisionBundle = errors.New("gateway: invalid table provision bundle")

// replicatedTableProvisionBundleDocument stores the two independently useful
// canonical documents as strings. Keeping their exact bytes avoids decoding
// and re-encoding the catalog or the internal split-source proof at this
// boundary, while the enclosing document remains canonical vibejson.
type replicatedTableProvisionBundleDocument struct {
	Format      uint16 `json:"format"`
	Catalog     string `json:"catalog"`
	SplitSource string `json:"split_source"`
}

// AppendReplicatedTableProvisionBundle appends one canonical local
// provisioning bundle. catalogRaw must be the bytes produced by
// AppendReplicatedTableProvision and splitSourceRaw must be one canonical
// persisted split-source record. Runtime performs the type-specific source
// validation against the enrolled roster and provision fragment.
func AppendReplicatedTableProvisionBundle(dst, catalogRaw, splitSourceRaw []byte) ([]byte, error) {
	if len(catalogRaw) == 0 || len(splitSourceRaw) == 0 || len(catalogRaw) > 4<<20 ||
		len(splitSourceRaw) > 4<<20 || !json.Valid(catalogRaw) || !json.Valid(splitSourceRaw) {
		return nil, ErrInvalidTableProvisionBundle
	}
	if _, err := OpenReplicatedTableProvision(catalogRaw); err != nil {
		return nil, errors.Join(ErrInvalidTableProvisionBundle, err)
	}
	document := replicatedTableProvisionBundleDocument{
		Format: ReplicatedTableProvisionBundleFormat, Catalog: string(catalogRaw), SplitSource: string(splitSourceRaw),
	}
	raw, err := vibejson.Marshal(&document)
	if err != nil {
		return nil, err
	}
	if len(raw) > 4<<20 {
		return nil, ErrCatalogTooLarge
	}
	return append(dst, raw...), nil
}

// OpenReplicatedTableProvisionBundle opens one canonical local provisioning
// bundle and returns owned copies of its two embedded documents. It does not
// accept a legacy catalog fragment: callers that support legacy startup
// inventories must select that grammar explicitly before calling this method.
func OpenReplicatedTableProvisionBundle(raw []byte) (catalogRaw, splitSourceRaw []byte, err error) {
	if len(raw) == 0 || len(raw) > 4<<20 {
		return nil, nil, ErrCatalogTooLarge
	}
	var document replicatedTableProvisionBundleDocument
	if err := vibejson.Unmarshal(raw, &document); err != nil {
		return nil, nil, errors.Join(ErrInvalidTableProvisionBundle, err)
	}
	canonical, err := vibejson.Marshal(&document)
	if err != nil || !bytes.Equal(canonical, raw) || document.Format != ReplicatedTableProvisionBundleFormat ||
		document.Catalog == "" || document.SplitSource == "" ||
		!json.Valid([]byte(document.Catalog)) || !json.Valid([]byte(document.SplitSource)) {
		return nil, nil, ErrInvalidTableProvisionBundle
	}
	if _, err := OpenReplicatedTableProvision([]byte(document.Catalog)); err != nil {
		return nil, nil, errors.Join(ErrInvalidTableProvisionBundle, err)
	}
	return append([]byte(nil), document.Catalog...), append([]byte(nil), document.SplitSource...), nil
}
