package serviceauthz

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	vibejson "github.com/thesyncim/vibejson"
)

// Load accepts one deliberately small canonical policy grammar. Object member
// order and spellings are exact, so duplicate, escaped, unknown, and reordered
// security fields cannot be interpreted differently by another decoder.
func Load(data []byte) (*Policy, error) {
	if len(data) == 0 || len(data) > AbsoluteMaxPolicyBytes {
		return nil, ErrInvalidPolicy
	}
	document, err := vibejson.ParseOptions(data, vibejson.Options{ZeroCopy: true, MaxDepth: 4})
	if err != nil {
		return nil, errors.Join(ErrInvalidPolicy, err)
	}
	fields, ok := document.Node().ObjectIter()
	if !ok {
		return nil, ErrInvalidPolicy
	}
	key, generationNode, ok := fields.Next()
	if !ok || !rawEqual(key, `"generation"`) {
		return nil, ErrInvalidPolicy
	}
	generation, ok := generationNode.Uint64()
	if !ok || generation == 0 {
		return nil, ErrInvalidPolicy
	}
	key, principalsNode, ok := fields.Next()
	if !ok || !rawEqual(key, `"principals"`) {
		return nil, ErrInvalidPolicy
	}
	if _, _, extra := fields.Next(); extra {
		return nil, ErrInvalidPolicy
	}
	count, ok := principalsNode.ArrayLen()
	if !ok || count == 0 || count > AbsoluteMaxPrincipals {
		return nil, ErrInvalidPolicy
	}
	principals, _ := principalsNode.ArrayIter()
	entries := make([]Entry, 0, count)
	for {
		principal, present := principals.Next()
		if !present {
			break
		}
		entry, parseErr := parsePrincipal(principal)
		if parseErr != nil || len(entries) != 0 && bytes.Compare(entries[len(entries)-1].Node[:], entry.Node[:]) >= 0 {
			return nil, ErrInvalidPolicy
		}
		entries = append(entries, entry)
	}
	return NewPolicy(generation, entries)
}

func parsePrincipal(node vibejson.Node) (Entry, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return Entry{}, ErrInvalidPolicy
	}
	key, nodeValue, ok := fields.Next()
	if !ok || !rawEqual(key, `"node"`) {
		return Entry{}, ErrInvalidPolicy
	}
	principal, err := parseNodeRaw(nodeValue.Raw().Bytes())
	if err != nil {
		return Entry{}, err
	}
	key, capabilitiesNode, ok := fields.Next()
	if !ok || !rawEqual(key, `"capabilities"`) {
		return Entry{}, ErrInvalidPolicy
	}
	if _, _, extra := fields.Next(); extra {
		return Entry{}, ErrInvalidPolicy
	}
	count, ok := capabilitiesNode.ArrayLen()
	if !ok || count == 0 || count > 4 {
		return Entry{}, ErrInvalidPolicy
	}
	values, _ := capabilitiesNode.ArrayIter()
	var capabilities, previous Capability
	for {
		value, present := values.Next()
		if !present {
			break
		}
		capability := parseCapabilityRaw(value.Raw().Bytes())
		if capability == 0 || capability <= previous {
			return Entry{}, ErrInvalidPolicy
		}
		capabilities |= capability
		previous = capability
	}
	return Entry{Node: principal, Capabilities: capabilities}, nil
}

func rawEqual(node vibejson.Node, canonical string) bool {
	return bytes.Equal(node.Raw().Bytes(), []byte(canonical))
}

func parseNodeRaw(raw []byte) (rafttransport.NodeID, error) {
	var node rafttransport.NodeID
	if len(raw) != 2+hex.EncodedLen(len(node)) || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return node, ErrInvalidPolicy
	}
	if _, err := hex.Decode(node[:], raw[1:len(raw)-1]); err != nil || node == (rafttransport.NodeID{}) {
		return rafttransport.NodeID{}, ErrInvalidPolicy
	}
	return node, nil
}

func parseCapabilityRaw(raw []byte) Capability {
	switch {
	case bytes.Equal(raw, []byte(`"data_read"`)):
		return CapabilityDataRead
	case bytes.Equal(raw, []byte(`"data_write"`)):
		return CapabilityDataWrite
	case bytes.Equal(raw, []byte(`"schema"`)):
		return CapabilitySchema
	case bytes.Equal(raw, []byte(`"delegate"`)):
		return CapabilityDelegate
	default:
		return 0
	}
}

func LoadFile(path string) (*Policy, error) {
	if path == "" {
		return nil, ErrInvalidPolicy
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Join(ErrInvalidPolicy, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > AbsoluteMaxPolicyBytes {
		return nil, errors.Join(ErrInvalidPolicy, err)
	}
	data := make([]byte, int(info.Size()))
	if _, err = io.ReadFull(file, data); err != nil {
		return nil, errors.Join(ErrInvalidPolicy, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nil, ErrInvalidPolicy
	}
	return Load(data)
}
