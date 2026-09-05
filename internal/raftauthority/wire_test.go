package raftauthority

import (
	"bytes"
	"testing"
	"time"
)

func TestAuthorityWireRoundTripRequestAndGrant(t *testing.T) {
	policy := authorityTestPolicy(true)
	request := AuthorityRequest{
		Group: authorityTestGroup(), Term: 9, Holder: 1, HolderIncarnation: 22,
		Config: authorityTestConfig(), PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 44, StartAt: 12 * time.Millisecond,
	}
	cases := []Message{
		{Kind: MessageRequest, Request: request},
		{Kind: MessageGrant, Request: request, Grant: AuthorityGrant{
			Request: request, Voter: 2, GrantedAt: 13 * time.Millisecond,
			PromiseUntil: 1_013 * time.Millisecond,
		}},
	}
	for _, original := range cases {
		raw, err := AppendCanonical([]byte{0xaa}, original)
		if err != nil {
			t.Fatalf("AppendCanonical(%d): %v", original.Kind, err)
		}
		if len(raw) != 1+CanonicalMessageBytes || raw[0] != 0xaa {
			t.Fatalf("encoded size=%d", len(raw))
		}
		opened, err := OpenCanonical(raw[1:])
		if err != nil || opened != original {
			t.Fatalf("opened=%+v original=%+v err=%v", opened, original, err)
		}
	}
}

func TestAuthorityWireRejectsReservedAndTruncatedBytes(t *testing.T) {
	policy := authorityTestPolicy(true)
	request := AuthorityRequest{
		Group: authorityTestGroup(), Term: 9, Holder: 1, HolderIncarnation: 22,
		Config: authorityTestConfig(), PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 44,
	}
	raw, err := AppendCanonical(nil, Message{Kind: MessageRequest, Request: request})
	if err != nil {
		t.Fatalf("AppendCanonical: %v", err)
	}
	for _, malformed := range [][]byte{nil, raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenCanonical(malformed); err != ErrInvalidWire {
			t.Fatalf("malformed size %d err=%v", len(malformed), err)
		}
	}
	reserved := bytes.Clone(raw)
	reserved[9] = 1
	if _, err := OpenCanonical(reserved); err != ErrInvalidWire {
		t.Fatalf("reserved header err=%v", err)
	}
	invalidKind := bytes.Clone(raw)
	invalidKind[8] = 99
	if _, err := OpenCanonical(invalidKind); err != ErrInvalidWire {
		t.Fatalf("invalid kind err=%v", err)
	}
}
