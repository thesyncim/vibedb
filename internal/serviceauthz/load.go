package serviceauthz

import (
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	vibejson "github.com/thesyncim/vibejson"
)

type policyDocument struct {
	Generation uint64                    `json:"generation"`
	Principals []policyPrincipalDocument `json:"principals"`
}

type policyPrincipalDocument struct {
	Node         string   `json:"node"`
	Capabilities []string `json:"capabilities"`
}

func Load(data []byte) (*Policy, error) {
	if len(data) == 0 || len(data) > AbsoluteMaxPolicyBytes {
		return nil, ErrInvalidPolicy
	}
	var document policyDocument
	if err := vibejson.Unmarshal(data, &document); err != nil {
		return nil, errors.Join(ErrInvalidPolicy, err)
	}
	if len(document.Principals) == 0 || len(document.Principals) > AbsoluteMaxPrincipals {
		return nil, ErrInvalidPolicy
	}
	entries := make([]Entry, len(document.Principals))
	for index, principal := range document.Principals {
		node, err := parseNode(principal.Node)
		if err != nil || len(principal.Capabilities) == 0 || len(principal.Capabilities) > 11 {
			return nil, ErrInvalidPolicy
		}
		var capabilities Capability
		for _, name := range principal.Capabilities {
			capability := parseCapability(name)
			if capability == 0 || capabilities&capability != 0 {
				return nil, ErrInvalidPolicy
			}
			capabilities |= capability
		}
		entries[index] = Entry{Node: node, Capabilities: capabilities}
	}
	return NewPolicy(document.Generation, entries)
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

func parseNode(value string) (rafttransport.NodeID, error) {
	var node rafttransport.NodeID
	if len(value) != hex.EncodedLen(len(node)) {
		return node, ErrInvalidPolicy
	}
	if _, err := hex.Decode(node[:], []byte(value)); err != nil || node == (rafttransport.NodeID{}) {
		return rafttransport.NodeID{}, ErrInvalidPolicy
	}
	return node, nil
}

func parseCapability(name string) Capability {
	switch name {
	case "data_read":
		return CapabilityDataRead
	case "data_write":
		return CapabilityDataWrite
	case "schema":
		return CapabilitySchema
	case "topology":
		return CapabilityTopology
	case "membership":
		return CapabilityMembership
	case "split":
		return CapabilitySplit
	case "move":
		return CapabilityMove
	case "backup":
		return CapabilityBackup
	case "restore":
		return CapabilityRestore
	case "operator":
		return CapabilityOperator
	case "delegate":
		return CapabilityDelegate
	default:
		return 0
	}
}
