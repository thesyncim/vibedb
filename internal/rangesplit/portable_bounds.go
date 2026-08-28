package rangesplit

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcapture"
	vibejson "github.com/thesyncim/vibejson"
)

// Preserve the original topology budget independently of the schema profile.
// A JSON string takes at most six encoded bytes per UTF-8 input byte. The
// numeric template uses the widest representation of each declared field,
// including zero-valued JSON profiles, so every legal 59-relation/name shape
// fits without weakening the original geometry or relation-count limits.
const portableGeometryBytes = 32 << 10
const portableRelationScalarShape = `{"relation":65535,"kind":255,"collection":"","global_index":{"IndexID":18446744073709551615,"Incarnation":18446744073709551615,"LocatorCount":255,"Unique":false,"KeyEncoding":255,"KeyArity":255,"TupleVersion":4294967295,"MapperVersion":4294967295,"BucketBits":255}}`
const maxPortableRelationBytes = len(portableRelationScalarShape) + 6*replication.MaxIdentityBytes
const portableBundleHeaderShape = `{"schema_generation":18446744073709551615,"source_manifest":[],"child_manifests":[],"relations":[]}`
const maxPortableDigestBytes = 1 + 4*sha256.Size // brackets, 32 three-digit bytes, 31 commas
const maxPortableBundleBytes = len(portableBundleHeaderShape) +
	(1+autosplit.MaxSplitChildren)*(maxPortableDigestBytes+1) +
	replication.MaxRelationsPerBundle*(maxPortableRelationBytes+1)
const MaxPortablePartitionerBytes = splitcapture.MaxPortableSpecBytes

// A negative result cannot convert to uint: schema expansion that exceeds the
// capture envelope fails compilation without allocating a maximum-size buffer.
const _ = uint(MaxPortablePartitionerBytes - (portableGeometryBytes + len(`,"bundle":`) + maxPortableBundleBytes))

type portableBounds struct {
	bundleBytes int
	bundleSeen  bool
}

type portableBundleBounds struct{}

var portableBoundsDecoder = mustPortableDecoder[portableBounds]()
var portableBundleBoundsDecoder = mustPortableDecoder[portableBundleBounds]()

func mustPortableDecoder[T any]() vibejson.Decoder[T] {
	decoder, err := vibejson.CompileDecoder[T](vibejson.DecoderOptions{MaxDepth: 32, ZeroCopy: true, CaseSensitive: true})
	if err != nil {
		panic(err)
	}
	return decoder
}

func validatePortableBounds(raw []byte) error {
	if len(raw) == 0 || len(raw) > MaxPortablePartitionerBytes {
		return ErrPortablePartitioner
	}
	var bounds portableBounds
	if portableBoundsDecoder.Decode(raw, &bounds) != nil {
		return ErrPortablePartitioner
	}
	geometry := len(raw)
	if bounds.bundleSeen {
		geometry -= bounds.bundleBytes + len(`,"bundle":`)
	}
	if geometry > portableGeometryBytes {
		return ErrPortablePartitioner
	}
	return nil
}

func (bounds *portableBounds) UnmarshalVibeJSON(cursor vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	if cursor.BeginObject("portable partitioner") != nil {
		return cursor, ErrPortablePartitioner
	}
	for first := true; ; first = false {
		key, more, err := cursor.NextField(first)
		if err != nil {
			return cursor, ErrPortablePartitioner
		}
		if !more {
			return cursor, nil
		}
		value, err := cursor.Raw()
		if err != nil {
			return cursor, ErrPortablePartitioner
		}
		if key == "bundle" {
			if bounds.bundleSeen || len(value.Bytes()) > maxPortableBundleBytes {
				return cursor, ErrPortablePartitioner
			}
			bounds.bundleSeen, bounds.bundleBytes = true, len(value.Bytes())
			var bundle portableBundleBounds
			if portableBundleBoundsDecoder.Decode(value.Bytes(), &bundle) != nil {
				return cursor, ErrPortablePartitioner
			}
		}
	}
}

func (*portableBundleBounds) UnmarshalVibeJSON(cursor vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	if cursor.BeginObject("split bundle") != nil {
		return cursor, ErrPortablePartitioner
	}
	var seen uint8
	for first := true; ; first = false {
		key, more, err := cursor.NextField(first)
		if err != nil {
			return cursor, ErrPortablePartitioner
		}
		if !more {
			return cursor, nil
		}
		limit, byteLimit, flag := 0, 0, uint8(0)
		switch key {
		case "relations":
			limit, byteLimit, flag = replication.MaxRelationsPerBundle, maxPortableRelationBytes, 1
		case "child_manifests":
			limit, byteLimit, flag = autosplit.MaxSplitChildren, maxPortableDigestBytes, 2
		default:
			if cursor.Skip() != nil {
				return cursor, ErrPortablePartitioner
			}
			continue
		}
		if seen&flag != 0 || cursor.BeginArray("split profile array") != nil {
			return cursor, ErrPortablePartitioner
		}
		seen |= flag
		for count := 0; ; count++ {
			more, err := cursor.NextElement(count == 0)
			if err != nil {
				return cursor, ErrPortablePartitioner
			}
			if !more {
				break
			}
			if count == limit {
				return cursor, ErrPortablePartitioner
			}
			value, err := cursor.Raw()
			if err != nil || len(value.Bytes()) > byteLimit {
				return cursor, ErrPortablePartitioner
			}
		}
	}
}
