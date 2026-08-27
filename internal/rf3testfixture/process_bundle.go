package rf3testfixture

import (
	"bytes"
	"errors"

	vibejson "github.com/thesyncim/vibejson"
)

var ErrProcessManifestBundle = errors.New("rf3 process fixture: invalid manifest bundle")

var processManifestCommonFields = [...]string{
	"listeners",
	"tls",
	"authorization_policy",
	"replica_control",
	"split_control",
}

var processManifestGroupFields = [...]string{
	"wal",
	"sql",
	"route",
	"members",
	"enrolled_target",
}

// CombineProcessManifests composes independently prepared singleton groups
// into the strict serve-rf3 multi-group grammar. The common listener, TLS,
// authorization, and control cuts must be byte-identical after vibejson
// canonicalization. Every group retains its own WAL, SQL, route, membership,
// and optional enrolled-target state.
func CombineProcessManifests(documents ...[]byte) ([]byte, error) {
	if len(documents) < 2 || len(documents) > 64 {
		return nil, ErrProcessManifestBundle
	}
	groups := make([]processManifestFields, len(documents))
	var common processManifestFields
	for index, document := range documents {
		fields, err := openProcessManifestFields(document)
		if err != nil {
			return nil, err
		}
		if index == 0 {
			common = fields.commonOnly()
		} else if !common.sameCommon(fields) {
			return nil, ErrProcessManifestBundle
		}
		groups[index] = fields.groupOnly()
	}

	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	if err := writer.BeginObject(); err != nil {
		return nil, err
	}
	for _, name := range processManifestCommonFields {
		if err := writer.Key(name); err != nil {
			return nil, err
		}
		if err := writer.RawUnchecked(common.values[name]); err != nil {
			return nil, err
		}
	}
	if err := writer.Key("groups"); err != nil {
		return nil, err
	}
	if err := writer.BeginArray(); err != nil {
		return nil, err
	}
	for _, group := range groups {
		if err := writer.BeginObject(); err != nil {
			return nil, err
		}
		for _, name := range processManifestGroupFields {
			value, found := group.values[name]
			if !found {
				continue
			}
			if err := writer.Key(name); err != nil {
				return nil, err
			}
			if err := writer.RawUnchecked(value); err != nil {
				return nil, err
			}
		}
		if err := writer.EndObject(); err != nil {
			return nil, err
		}
	}
	if err := writer.EndArray(); err != nil {
		return nil, err
	}
	if err := writer.EndObject(); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

type processManifestFields struct {
	values map[string][]byte
}

func openProcessManifestFields(document []byte) (processManifestFields, error) {
	parsed, err := vibejson.Parse(document)
	if err != nil {
		return processManifestFields{}, errors.Join(ErrProcessManifestBundle, err)
	}
	members, ok := parsed.Object()
	if !ok {
		return processManifestFields{}, ErrProcessManifestBundle
	}
	fields := processManifestFields{values: make(map[string][]byte, len(members))}
	for _, member := range members {
		if _, duplicate := fields.values[member.Key]; duplicate ||
			!processManifestFieldAllowed(member.Key) {
			return processManifestFields{}, ErrProcessManifestBundle
		}
		raw, marshalErr := member.Value.MarshalJSON()
		if marshalErr != nil {
			return processManifestFields{}, errors.Join(ErrProcessManifestBundle, marshalErr)
		}
		fields.values[member.Key] = raw
	}
	for _, name := range processManifestCommonFields {
		if len(fields.values[name]) == 0 {
			return processManifestFields{}, ErrProcessManifestBundle
		}
	}
	for _, name := range processManifestGroupFields[:4] {
		if len(fields.values[name]) == 0 {
			return processManifestFields{}, ErrProcessManifestBundle
		}
	}
	return fields, nil
}

func processManifestFieldAllowed(name string) bool {
	for _, candidate := range processManifestCommonFields {
		if name == candidate {
			return true
		}
	}
	for _, candidate := range processManifestGroupFields {
		if name == candidate {
			return true
		}
	}
	return false
}

func (fields processManifestFields) commonOnly() processManifestFields {
	common := processManifestFields{values: make(map[string][]byte, len(processManifestCommonFields))}
	for _, name := range processManifestCommonFields {
		common.values[name] = fields.values[name]
	}
	return common
}

func (fields processManifestFields) groupOnly() processManifestFields {
	group := processManifestFields{values: make(map[string][]byte, len(processManifestGroupFields))}
	for _, name := range processManifestGroupFields {
		if value, found := fields.values[name]; found {
			group.values[name] = value
		}
	}
	return group
}

func (fields processManifestFields) sameCommon(other processManifestFields) bool {
	for _, name := range processManifestCommonFields {
		if !bytes.Equal(fields.values[name], other.values[name]) {
			return false
		}
	}
	return true
}
