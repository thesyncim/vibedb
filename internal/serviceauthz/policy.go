// Package serviceauthz provides fixed-width authorization for authenticated
// application-service principals. TLS establishes the trust domain; policies
// bind exact binary NodeIDs to compact capability bitsets.
package serviceauthz

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var ErrInvalidPolicy = errors.New("serviceauthz: invalid policy")

const (
	AbsoluteMaxPrincipals  = 65536
	AbsoluteMaxPolicyBytes = 4 << 20
)

type Capability uint64

const (
	CapabilityDataRead Capability = 1 << iota
	CapabilityDataWrite
	CapabilitySchema
	// CapabilityDelegate permits an authenticated service principal to forward
	// an exact end-user authority. It grants no data or control-plane action by
	// itself; the forwarded principal is checked independently at the receiver.
	CapabilityDelegate
	// CapabilityMembership permits an exact forwarded operator to execute the
	// sealed RF3 learner, promotion, removal, and leader-transfer operation set.
	// It grants no data, schema, or delegation authority.
	CapabilityMembership
	// CapabilityTopology permits replicated catalog publication and split/move
	// operation journal access. It is deliberately separate from DataWrite: a
	// data writer cannot acquire routing or controller authority merely because
	// the catalog is stored in an ordinary replicated relation.
	CapabilityTopology
	// CapabilityTransactionRecovery permits the replacement-gateway recovery
	// reader to inspect replicated transaction control state. It is deliberately
	// separate from ordinary data reads, writes, and topology authority: none of
	// those capabilities can discover transaction participants or decisions.
	CapabilityTransactionRecovery
	// CapabilityRequestLedger permits only the internal durable request-result
	// ledger grammar. It is separate from data, topology, and transaction
	// recovery so an ordinary writer cannot forge or acknowledge idempotency
	// state and a ledger principal cannot mutate user relations.
	CapabilityRequestLedger
)

const AllCapabilities = CapabilityDataRead | CapabilityDataWrite | CapabilitySchema |
	CapabilityDelegate | CapabilityMembership | CapabilityTopology |
	CapabilityTransactionRecovery | CapabilityRequestLedger

func (capability Capability) Valid() bool {
	return capability != 0 && capability&^AllCapabilities == 0
}

type DecisionCode uint8

const (
	DecisionAllow DecisionCode = iota + 1
	DecisionDenyNoPrincipal
	DecisionDenyGeneration
	DecisionDenyCapability
	DecisionDenyInvalid
)

type Entry struct {
	Node         rafttransport.NodeID
	Capabilities Capability
}

type Policy struct {
	generation uint64
	entries    []Entry
}

func NewPolicy(generation uint64, entries []Entry) (*Policy, error) {
	if generation == 0 || len(entries) == 0 || len(entries) > AbsoluteMaxPrincipals {
		return nil, ErrInvalidPolicy
	}
	owned := slices.Clone(entries)
	for _, entry := range owned {
		if entry.Node == (rafttransport.NodeID{}) || !entry.Capabilities.Valid() {
			return nil, ErrInvalidPolicy
		}
	}
	slices.SortFunc(owned, func(left, right Entry) int {
		return bytes.Compare(left.Node[:], right.Node[:])
	})
	for index := 1; index < len(owned); index++ {
		if owned[index].Node == owned[index-1].Node {
			return nil, ErrInvalidPolicy
		}
	}
	return &Policy{generation: generation, entries: owned}, nil
}

func (policy *Policy) Generation() uint64 {
	if policy == nil {
		return 0
	}
	return policy.generation
}

func (policy *Policy) Nodes() []rafttransport.NodeID {
	if policy == nil {
		return nil
	}
	nodes := make([]rafttransport.NodeID, len(policy.entries))
	for index := range policy.entries {
		nodes[index] = policy.entries[index].Node
	}
	return nodes
}

func (policy *Policy) NodesWith(capability Capability) []rafttransport.NodeID {
	if policy == nil || !capability.Valid() {
		return nil
	}
	count := 0
	for index := range policy.entries {
		if policy.entries[index].Capabilities&capability == capability {
			count++
		}
	}
	nodes := make([]rafttransport.NodeID, 0, count)
	for index := range policy.entries {
		if policy.entries[index].Capabilities&capability == capability {
			nodes = append(nodes, policy.entries[index].Node)
		}
	}
	return nodes
}

func (policy *Policy) Check(node rafttransport.NodeID, capability Capability) DecisionCode {
	if policy == nil || node == (rafttransport.NodeID{}) || !capability.Valid() {
		return DecisionDenyInvalid
	}
	index, found := slices.BinarySearchFunc(policy.entries, node,
		func(entry Entry, node rafttransport.NodeID) int {
			return bytes.Compare(entry.Node[:], node[:])
		})
	if !found {
		return DecisionDenyNoPrincipal
	}
	if policy.entries[index].Capabilities&capability != capability {
		return DecisionDenyCapability
	}
	return DecisionAllow
}

type Gate struct{ current atomic.Pointer[Policy] }

func (gate *Gate) Generation() uint64 {
	if gate == nil {
		return 0
	}
	policy := gate.current.Load()
	if policy == nil {
		return 0
	}
	return policy.Generation()
}

func NewGate(policy *Policy) (*Gate, error) {
	if policy == nil || policy.Generation() == 0 {
		return nil, ErrInvalidPolicy
	}
	gate := new(Gate)
	gate.current.Store(policy)
	return gate, nil
}

func (gate *Gate) Rotate(policy *Policy) error {
	if gate == nil || policy == nil {
		return ErrInvalidPolicy
	}
	for {
		current := gate.current.Load()
		if current == nil || policy.Generation() <= current.Generation() {
			return ErrInvalidPolicy
		}
		if gate.current.CompareAndSwap(current, policy) {
			return nil
		}
	}
}

func (gate *Gate) Check(node rafttransport.NodeID, generation uint64, capability Capability) DecisionCode {
	if gate == nil {
		return DecisionDenyInvalid
	}
	policy := gate.current.Load()
	if policy == nil || generation == 0 || policy.Generation() != generation {
		return DecisionDenyGeneration
	}
	return policy.Check(node, capability)
}

type AuditEvent struct {
	Node       rafttransport.NodeID
	Generation uint64
	Capability Capability
	Decision   DecisionCode
}

// Authority is the fixed-width authorization identity carried through a
// trusted service hop. Generation binds the principal to one immutable policy
// publication and prevents a retry from silently acquiring newer privileges.
type Authority struct {
	Node       rafttransport.NodeID
	Generation uint64
}

func (authority Authority) Valid() bool {
	return authority.Node != (rafttransport.NodeID{}) && authority.Generation != 0
}

func (gate *Gate) CheckAuthority(authority Authority, capability Capability) DecisionCode {
	if !authority.Valid() {
		return DecisionDenyInvalid
	}
	return gate.Check(authority.Node, authority.Generation, capability)
}

type authorityContextKey struct{}

// WithAuthority binds one exact authenticated principal to request context.
// The value is fixed-width and copied; no certificate strings or policy object
// escape into downstream request construction.
func WithAuthority(parent context.Context, authority Authority) (context.Context, error) {
	if parent == nil || !authority.Valid() {
		return nil, ErrInvalidPolicy
	}
	return context.WithValue(parent, authorityContextKey{}, authority), nil
}

func FromContext(ctx context.Context) (Authority, bool) {
	if ctx == nil {
		return Authority{}, false
	}
	authority, ok := ctx.Value(authorityContextKey{}).(Authority)
	return authority, ok && authority.Valid()
}

type AuditSink interface{ RecordAuthorization(AuditEvent) }

func CheckAndAudit(gate *Gate, sink AuditSink, node rafttransport.NodeID, generation uint64, capability Capability) DecisionCode {
	decision := gate.Check(node, generation, capability)
	if sink != nil {
		sink.RecordAuthorization(AuditEvent{Node: node, Generation: generation,
			Capability: capability, Decision: decision})
	}
	return decision
}
