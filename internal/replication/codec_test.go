package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func testID(seed byte) ID128 {
	var id ID128
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testDigest(seed byte) Digest {
	var digest Digest
	for i := range digest {
		digest[i] = seed + byte(i)
	}
	return digest
}

func testRetryHome() RetryHome {
	return RetryHome{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
}

const (
	testLeaseExpectedDeadlineUnixNano int64 = 1_700_000_000_123_456_789
	testLeaseNextDeadlineUnixNano     int64 = 1_700_000_001_123_456_789
)

func testCommand() Command {
	return Command{
		ClusterID: testID(0x01), ClusterIncarnation: testID(0x21),
		TopologyRecoveryEpoch: 3,
		Distribution:          "tenant_data", Shard: "-80", AllocationGeneration: 5,
		ShardIncarnation: testID(0x41), GroupID: testID(0x61),
		ReplicaSetVersion: 7, ActivePolicyGeneration: 11,
		ProtectionEpoch: 13, OwnershipEpoch: 17, SchemaGeneration: 19,
		RoutingVersion: 23, RouteGeneration: 29,
		Tenant: []byte("tenant\x00one"), ClientID: testID(0x81),
		ClientEpoch: 31, ClientSequence: 37,
		AckThrough:  31,
		Fingerprint: testDigest(0xa1), RetryHome: testRetryHome(),
		Collection: "messages",
		Mutations: []Mutation{
			{Kind: MutationPut, Key: []byte("alpha"), Value: []byte(`{"id":"alpha","v":1}`)},
			{Kind: MutationDelete, Key: []byte("omega")},
		},
	}
}

func testSessionRetireCommand() Command {
	command := testCommand()
	command.Kind = CommandSessionRetire
	command.AckThrough = 0
	command.Mutations = nil
	return command
}

func testSessionReleaseCommand() Command {
	command := testSessionRetireCommand()
	command.Kind = CommandSessionRelease
	return command
}

func testSessionOpenCommand() Command {
	command := testSessionRetireCommand()
	command.Kind = CommandSessionOpen
	command.ClientEpoch = 0
	command.ClientSequence = 1
	command.NextDeadlineUnixNano = testLeaseExpectedDeadlineUnixNano
	return command
}

func testSessionRenewCommand() Command {
	command := testSessionRetireCommand()
	command.Kind = CommandSessionRenew
	command.ExpectedDeadlineUnixNano = testLeaseExpectedDeadlineUnixNano
	command.NextDeadlineUnixNano = testLeaseNextDeadlineUnixNano
	return command
}

func testSessionRevokeCommand() Command {
	command := testSessionRetireCommand()
	command.Kind = CommandSessionRevoke
	command.ExpectedDeadlineUnixNano = testLeaseExpectedDeadlineUnixNano
	return command
}

func testInlineCompletion() Completion {
	result := []byte{0x01, 0x00, 0xff, 'o', 'k'}
	completion := Completion{
		ClusterID: testID(0x01), ClusterIncarnation: testID(0x21),
		TopologyRecoveryEpoch: 3,
		Distribution:          "tenant_data", Shard: "-80", AllocationGeneration: 5,
		ShardIncarnation: testID(0x41), GroupID: testID(0x61),
		ReplicaSetVersion: 7, ActivePolicyGeneration: 11,
		ProtectionEpoch: 13, RoutingVersion: 23, RouteGeneration: 29,
		Tenant: []byte("tenant\x00one"), ClientID: testID(0x81),
		ClientEpoch: 31, ClientSequence: 37,
		Fingerprint: testDigest(0xa1), RetryHome: testRetryHome(),
		AppliedSequence: 41,
		ResultCode:      0, ResultFormat: 1, Storage: CompletionInline,
		ResultLength: uint64(len(result)), InlineResult: result,
	}
	completion.ResultDigest = CompletionResultDigest(
		completion.ResultCode, completion.ResultFormat, result,
	)
	return completion
}

func testReferenceCompletion() Completion {
	completion := testInlineCompletion()
	completion.Storage = CompletionDigestReference
	completion.ResultLength = MaxInlineCompletionBytes + 1
	completion.InlineResult = nil
	completion.ResultDigest = testDigest(0xc1)
	return completion
}

func testCompletionBytes(completion Completion) CompletionBytes {
	return CompletionBytes{
		ClusterID:              completion.ClusterID,
		ClusterIncarnation:     completion.ClusterIncarnation,
		TopologyRecoveryEpoch:  completion.TopologyRecoveryEpoch,
		Distribution:           []byte(completion.Distribution),
		Shard:                  []byte(completion.Shard),
		AllocationGeneration:   completion.AllocationGeneration,
		ShardIncarnation:       completion.ShardIncarnation,
		GroupID:                completion.GroupID,
		ReplicaSetVersion:      completion.ReplicaSetVersion,
		ActivePolicyGeneration: completion.ActivePolicyGeneration,
		ProtectionEpoch:        completion.ProtectionEpoch,
		RoutingVersion:         completion.RoutingVersion,
		RouteGeneration:        completion.RouteGeneration,
		Tenant:                 completion.Tenant,
		ClientID:               completion.ClientID,
		ClientEpoch:            completion.ClientEpoch,
		ClientSequence:         completion.ClientSequence,
		Fingerprint:            completion.Fingerprint,
		RetryHome:              completion.RetryHome,
		AppliedSequence:        completion.AppliedSequence,
		ResultCode:             completion.ResultCode,
		ResultFormat:           completion.ResultFormat,
		Storage:                completion.Storage,
		ResultLength:           completion.ResultLength,
		ResultDigest:           completion.ResultDigest,
		InlineResult:           completion.InlineResult,
	}
}

func encodeCommand(t testing.TB, command Command) []byte {
	t.Helper()
	encoded, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	return encoded
}

func encodeCompletion(t testing.TB, completion Completion) []byte {
	t.Helper()
	encoded, err := AppendCompletion(nil, completion)
	if err != nil {
		t.Fatalf("AppendCompletion: %v", err)
	}
	return encoded
}

func TestCommandRoundTripAndIterator(t *testing.T) {
	command := testCommand()
	prefix := []byte("prefix:")
	dst := make([]byte, len(prefix), len(prefix)+1024)
	copy(dst, prefix)
	encoded, err := AppendCommand(dst, command)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len(prefix)], prefix) {
		t.Fatal("AppendCommand changed destination prefix")
	}
	frame := encoded[len(prefix):]
	view, err := OpenCommand(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(view.Bytes(), frame) || view.Kind() != command.Kind ||
		view.ClusterID != command.ClusterID ||
		view.ClusterIncarnation != command.ClusterIncarnation ||
		view.ShardIncarnation != command.ShardIncarnation || view.GroupID != command.GroupID ||
		view.ClientID != command.ClientID || view.Fingerprint != command.Fingerprint ||
		view.RetryHome != command.RetryHome ||
		!bytes.Equal(view.Tenant, command.Tenant) ||
		string(view.Distribution) != command.Distribution || string(view.Shard) != command.Shard ||
		string(view.Collection) != command.Collection || view.MutationCount() != len(command.Mutations) {
		t.Fatalf("decoded command identity mismatch: %+v", view)
	}
	if view.TopologyRecoveryEpoch != command.TopologyRecoveryEpoch ||
		view.AllocationGeneration != command.AllocationGeneration ||
		view.ReplicaSetVersion != command.ReplicaSetVersion ||
		view.ActivePolicyGeneration != command.ActivePolicyGeneration ||
		view.ProtectionEpoch != command.ProtectionEpoch ||
		view.OwnershipEpoch != command.OwnershipEpoch ||
		view.SchemaGeneration != command.SchemaGeneration ||
		view.RoutingVersion != command.RoutingVersion ||
		view.RouteGeneration != command.RouteGeneration ||
		view.ClientEpoch != command.ClientEpoch || view.ClientSequence != command.ClientSequence ||
		view.AckThrough != command.AckThrough || view.ExpectedDeadlineUnixNano != 0 ||
		view.NextDeadlineUnixNano != 0 {
		t.Fatal("decoded command scalar mismatch")
	}
	iterator := view.Mutations()
	for index, want := range command.Mutations {
		if !iterator.Next() {
			t.Fatalf("iterator stopped before mutation %d", index)
		}
		got := iterator.Mutation()
		if got.Kind != want.Kind || !bytes.Equal(got.Key, want.Key) ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("mutation %d = %+v, want %+v", index, got, want)
		}
	}
	if iterator.Next() {
		t.Fatal("iterator produced a trailing mutation")
	}
	var empty MutationIterator
	if empty.Next() || (*MutationIterator)(nil).Next() {
		t.Fatal("empty or nil iterator advanced")
	}
}

func TestSessionRetireRoundTrip(t *testing.T) {
	command := testSessionRetireCommand()
	encoded := encodeCommand(t, command)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind() != CommandSessionRetire || view.MutationCount() != 0 ||
		view.AckThrough != command.AckThrough ||
		view.ClientSequence != command.ClientSequence {
		t.Fatalf("decoded session retire mismatch: %+v", view)
	}
	iterator := view.Mutations()
	if iterator.Next() {
		t.Fatal("session retire exposed a mutation")
	}
	if len(encoded) != commandMutationOffset(encoded)+envelopeChecksumBytes {
		t.Fatalf("session retire length = %d, want empty mutation body", len(encoded))
	}
}

func TestSessionReleaseRoundTrip(t *testing.T) {
	command := testSessionReleaseCommand()
	encoded := encodeCommand(t, command)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind() != CommandSessionRelease || view.MutationCount() != 0 ||
		view.AckThrough != command.AckThrough ||
		view.ClientEpoch != command.ClientEpoch ||
		view.ClientSequence != command.ClientSequence ||
		view.Fingerprint != command.Fingerprint {
		t.Fatalf("decoded session release mismatch: %+v", view)
	}
	iterator := view.Mutations()
	if iterator.Next() {
		t.Fatal("session release exposed a mutation")
	}
	if len(encoded) != commandMutationOffset(encoded)+envelopeChecksumBytes {
		t.Fatalf("session release length = %d, want empty mutation body", len(encoded))
	}
}

func TestSessionOpenRoundTrip(t *testing.T) {
	command := testSessionOpenCommand()
	encoded := encodeCommand(t, command)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind() != CommandSessionOpen || view.MutationCount() != 0 ||
		view.ClientEpoch != 0 || view.ClientSequence != 1 || view.AckThrough != 0 ||
		view.Fingerprint != command.Fingerprint ||
		view.ExpectedDeadlineUnixNano != command.ExpectedDeadlineUnixNano ||
		view.NextDeadlineUnixNano != command.NextDeadlineUnixNano {
		t.Fatalf("decoded session open mismatch: %+v", view)
	}
	iterator := view.Mutations()
	if iterator.Next() {
		t.Fatal("session open exposed a mutation")
	}
	if len(encoded) != commandMutationOffset(encoded)+sessionLeaseBodyBytes+envelopeChecksumBytes {
		t.Fatalf("session open length = %d, want exact lease body", len(encoded))
	}
}

func TestSessionRenewAndRevokeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command Command
	}{
		{"renew", testSessionRenewCommand()},
		{"revoke", testSessionRevokeCommand()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeCommand(t, tc.command)
			view, err := OpenCommand(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if view.Kind() != tc.command.Kind || view.MutationCount() != 0 ||
				view.ClientEpoch != tc.command.ClientEpoch ||
				view.ClientSequence != tc.command.ClientSequence ||
				view.AckThrough != tc.command.AckThrough ||
				view.ExpectedDeadlineUnixNano != tc.command.ExpectedDeadlineUnixNano ||
				view.NextDeadlineUnixNano != tc.command.NextDeadlineUnixNano {
				t.Fatalf("decoded session %s mismatch: %+v", tc.name, view)
			}
			mutations := view.Mutations()
			if mutations.Next() {
				t.Fatalf("session %s exposed a mutation", tc.name)
			}
			if len(encoded) != commandMutationOffset(encoded)+sessionLeaseBodyBytes+envelopeChecksumBytes {
				t.Fatalf("session %s length = %d, want exact lease body", tc.name, len(encoded))
			}
		})
	}
}

func TestSessionOpenClientTupleValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Command)
	}{
		{"nonzero_epoch", func(command *Command) { command.ClientEpoch = 1 }},
		{"zero_sequence", func(command *Command) { command.ClientSequence = 0 }},
		{"later_sequence", func(command *Command) { command.ClientSequence = 2 }},
		{"acknowledgement", func(command *Command) { command.AckThrough = 1 }},
	} {
		t.Run("encode_"+tc.name, func(t *testing.T) {
			command := testSessionOpenCommand()
			tc.mutate(&command)
			prefix := []byte("unchanged")
			got, err := AppendCommand(prefix, command)
			if !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
			if !bytes.Equal(got, prefix) {
				t.Fatal("failed session-open encode changed destination")
			}
		})

		t.Run("decode_"+tc.name, func(t *testing.T) {
			command := testSessionOpenCommand()
			encoded := encodeCommand(t, command)
			tc.mutate(&command)
			binary.LittleEndian.PutUint64(encoded[184:192], command.ClientEpoch)
			binary.LittleEndian.PutUint64(encoded[192:200], command.ClientSequence)
			binary.LittleEndian.PutUint64(encoded[248:256], command.AckThrough)
			sealEnvelope(encoded)
			if _, err := OpenCommand(encoded); !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
		})
	}
}

func TestSessionLeaseDeadlineValidation(t *testing.T) {
	tests := []struct {
		name    string
		command func() Command
		mutate  func(*Command)
	}{
		{"open_nonzero_expected", testSessionOpenCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = 1 }},
		{"open_zero_next", testSessionOpenCommand, func(c *Command) { c.NextDeadlineUnixNano = 0 }},
		{"open_negative_next", testSessionOpenCommand, func(c *Command) { c.NextDeadlineUnixNano = -1 }},
		{"renew_zero_expected", testSessionRenewCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = 0 }},
		{"renew_negative_expected", testSessionRenewCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = -1 }},
		{"renew_equal", testSessionRenewCommand, func(c *Command) { c.NextDeadlineUnixNano = c.ExpectedDeadlineUnixNano }},
		{"renew_regression", testSessionRenewCommand, func(c *Command) { c.NextDeadlineUnixNano = c.ExpectedDeadlineUnixNano - 1 }},
		{"renew_negative_next", testSessionRenewCommand, func(c *Command) { c.NextDeadlineUnixNano = -1 }},
		{"revoke_zero_expected", testSessionRevokeCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = 0 }},
		{"revoke_negative_expected", testSessionRevokeCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = -1 }},
		{"revoke_nonzero_next", testSessionRevokeCommand, func(c *Command) { c.NextDeadlineUnixNano = 1 }},
	}
	for _, tc := range tests {
		t.Run("encode_"+tc.name, func(t *testing.T) {
			command := tc.command()
			tc.mutate(&command)
			prefix := []byte("unchanged")
			got, err := AppendCommand(prefix, command)
			if !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
			if !bytes.Equal(got, prefix) {
				t.Fatal("failed lease encode changed destination")
			}
		})

		t.Run("decode_"+tc.name, func(t *testing.T) {
			command := tc.command()
			encoded := encodeCommand(t, command)
			tc.mutate(&command)
			lease := commandMutationOffset(encoded)
			binary.LittleEndian.PutUint64(encoded[lease:lease+8], uint64(command.ExpectedDeadlineUnixNano))
			binary.LittleEndian.PutUint64(encoded[lease+8:lease+16], uint64(command.NextDeadlineUnixNano))
			sealEnvelope(encoded)
			if _, err := OpenCommand(encoded); !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		command func() Command
		mutate  func(*Command)
	}{
		{"mutation_expected", testCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = 1 }},
		{"mutation_next", testCommand, func(c *Command) { c.NextDeadlineUnixNano = 1 }},
		{"retire_expected", testSessionRetireCommand, func(c *Command) { c.ExpectedDeadlineUnixNano = 1 }},
		{"release_next", testSessionReleaseCommand, func(c *Command) { c.NextDeadlineUnixNano = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := tc.command()
			tc.mutate(&command)
			if _, err := AppendCommand(nil, command); !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
		})
	}
}

func TestSessionLeaseBodyLengthIsExact(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command Command
	}{
		{"open", testSessionOpenCommand()},
		{"renew", testSessionRenewCommand()},
		{"revoke", testSessionRevokeCommand()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valid := encodeCommand(t, tc.command)
			for cut := 0; cut < len(valid); cut++ {
				if _, err := OpenCommand(valid[:cut]); err == nil {
					t.Fatalf("accepted raw truncation at %d", cut)
				}
			}
			for _, bodyBytes := range []int{0, 1, sessionLeaseBodyBytes - 1, sessionLeaseBodyBytes + 1, 2 * sessionLeaseBodyBytes} {
				candidate := commandWithLeaseBodyBytes(valid, bodyBytes)
				if _, err := OpenCommand(candidate); !errors.Is(err, ErrEnvelopeSemantic) {
					t.Fatalf("accepted %d-byte lease body: %v", bodyBytes, err)
				}
			}
		})
	}
}

func commandWithLeaseBodyBytes(frame []byte, bodyBytes int) []byte {
	lease := commandMutationOffset(frame)
	trailer := len(frame) - envelopeChecksumBytes
	result := make([]byte, lease+bodyBytes+envelopeChecksumBytes)
	copy(result[:lease], frame[:lease])
	copyBytes := min(bodyBytes, sessionLeaseBodyBytes)
	copy(result[lease:lease+copyBytes], frame[lease:lease+copyBytes])
	copy(result[len(result)-envelopeChecksumBytes:], frame[trailer:])
	binary.LittleEndian.PutUint32(result[16:20], uint32(len(result)))
	originalBody := binary.LittleEndian.Uint32(frame[20:24])
	binary.LittleEndian.PutUint32(
		result[20:24], originalBody-sessionLeaseBodyBytes+uint32(bodyBytes),
	)
	sealEnvelope(result)
	return result
}

func TestNonOpenCommandsRetainClientTupleRules(t *testing.T) {
	commands := []struct {
		name    string
		command Command
	}{
		{"mutation", testCommand()},
		{"retire", testSessionRetireCommand()},
		{"release", testSessionReleaseCommand()},
		{"renew", testSessionRenewCommand()},
		{"revoke", testSessionRevokeCommand()},
	}
	for _, commandCase := range commands {
		for _, tc := range []struct {
			name   string
			mutate func(*Command)
		}{
			{"zero_epoch", func(command *Command) { command.ClientEpoch = 0 }},
			{"zero_sequence", func(command *Command) { command.ClientSequence = 0 }},
			{"ack_equal_sequence", func(command *Command) { command.AckThrough = command.ClientSequence }},
			{"ack_after_sequence", func(command *Command) { command.AckThrough = command.ClientSequence + 1 }},
		} {
			t.Run(commandCase.name+"_"+tc.name, func(t *testing.T) {
				testCommand := commandCase.command
				tc.mutate(&testCommand)
				if _, err := AppendCommand(nil, testCommand); !errors.Is(err, ErrEnvelopeSemantic) {
					t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
				}
			})
		}
	}
}

func TestAckThroughBoundaries(t *testing.T) {
	for _, ackThrough := range []uint64{0, testCommand().ClientSequence - 1} {
		command := testCommand()
		command.AckThrough = ackThrough
		view, err := OpenCommand(encodeCommand(t, command))
		if err != nil {
			t.Fatalf("AckThrough %d: %v", ackThrough, err)
		}
		if view.AckThrough != ackThrough {
			t.Fatalf("AckThrough = %d, want %d", view.AckThrough, ackThrough)
		}
	}
}

func TestCommandPreservesMutationOrdinalsAndDuplicateKeys(t *testing.T) {
	command := testCommand()
	command.Mutations = []Mutation{
		{Kind: MutationPut, Key: []byte("same"), Value: []byte("first")},
		{Kind: MutationPut, Key: []byte("same"), Value: []byte("second")},
		{Kind: MutationDelete, Key: []byte("same")},
		{Kind: MutationPut, Key: []byte("before"), Value: []byte("descending")},
	}
	encoded := encodeCommand(t, command)
	view, err := OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	iterator := view.Mutations()
	for ordinal, want := range command.Mutations {
		if !iterator.Next() {
			t.Fatalf("iterator stopped before ordinal %d", ordinal)
		}
		got := iterator.Mutation()
		if got.Kind != want.Kind || !bytes.Equal(got.Key, want.Key) ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("ordinal %d = %+v, want %+v", ordinal, got, want)
		}
	}
	if iterator.Next() {
		t.Fatal("iterator produced an extra ordinal")
	}

	reordered := command
	reordered.Mutations = append([]Mutation(nil), command.Mutations...)
	reordered.Mutations[0], reordered.Mutations[1] =
		reordered.Mutations[1], reordered.Mutations[0]
	if bytes.Equal(encoded, encodeCommand(t, reordered)) {
		t.Fatal("reordering duplicate-key mutations did not change command bytes")
	}
}

func TestCompletionRoundTrip(t *testing.T) {
	for _, completion := range []Completion{
		testInlineCompletion(), testReferenceCompletion(),
	} {
		encoded := encodeCompletion(t, completion)
		view, err := OpenCompletion(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(view.Bytes(), encoded) || view.ClusterID != completion.ClusterID ||
			view.ClusterIncarnation != completion.ClusterIncarnation ||
			view.ShardIncarnation != completion.ShardIncarnation || view.GroupID != completion.GroupID ||
			view.ClientID != completion.ClientID || view.Fingerprint != completion.Fingerprint ||
			view.ResultDigest != completion.ResultDigest || view.RetryHome != completion.RetryHome ||
			!bytes.Equal(view.Tenant, completion.Tenant) ||
			string(view.Distribution) != completion.Distribution || string(view.Shard) != completion.Shard ||
			view.Storage != completion.Storage || view.ResultLength != completion.ResultLength ||
			view.ResultCode != completion.ResultCode || view.ResultFormat != completion.ResultFormat ||
			!bytes.Equal(view.InlineResult, completion.InlineResult) {
			t.Fatalf("decoded completion mismatch: %+v", view)
		}
		if view.TopologyRecoveryEpoch != completion.TopologyRecoveryEpoch ||
			view.AllocationGeneration != completion.AllocationGeneration ||
			view.ReplicaSetVersion != completion.ReplicaSetVersion ||
			view.ActivePolicyGeneration != completion.ActivePolicyGeneration ||
			view.ProtectionEpoch != completion.ProtectionEpoch ||
			view.RoutingVersion != completion.RoutingVersion ||
			view.RouteGeneration != completion.RouteGeneration ||
			view.ClientEpoch != completion.ClientEpoch ||
			view.ClientSequence != completion.ClientSequence ||
			view.AppliedSequence != completion.AppliedSequence {
			t.Fatal("decoded completion scalar mismatch")
		}
	}
}

func TestCompletionByteInputIsWireIdentical(t *testing.T) {
	for _, completion := range []Completion{
		testInlineCompletion(), testReferenceCompletion(),
	} {
		want := encodeCompletion(t, completion)
		prefix := []byte("prefix:")
		dst := make([]byte, len(prefix), len(prefix)+len(want))
		copy(dst, prefix)
		got, err := AppendCompletionBytes(dst, testCompletionBytes(completion))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:len(prefix)], prefix) || !bytes.Equal(got[len(prefix):], want) {
			t.Fatal("byte-native completion differs from canonical completion bytes")
		}
	}
}

func TestCompletionByteInputUsesCanonicalValidation(t *testing.T) {
	completion := testCompletionBytes(testInlineCompletion())
	completion.Distribution = []byte{0xff}
	prefix := []byte("unchanged")
	got, err := AppendCompletionBytes(prefix, completion)
	if !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
	}
	if !bytes.Equal(got, prefix) {
		t.Fatal("validation failure changed destination")
	}
}

// These vectors freeze every byte of the current envelopes. They change only
// when the single supported grammar intentionally changes.
func TestGoldenVectors(t *testing.T) {
	const commandHex = "564442434d4400000100010000010000560100004e00000002000000000000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d000000000000001100000000000000130000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f000000000000002500000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00123456789abcdef0a000b00030008001f0000000000000074656e616e74006f6e6574656e616e745f646174612d38306d657373616765730100050014000000616c7068617b226964223a22616c706861222c2276223a317d02000500000000006f6d6567610b481f1bf4b7e0e4"
	const retireHex = "564442434d4400000100020000010000280100002000000000000000000000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d000000000000001100000000000000130000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f000000000000002500000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00123456789abcdef0a000b0003000800000000000000000074656e616e74006f6e6574656e616e745f646174612d38306d65737361676573844f206c7bb0df93"
	const releaseHex = "564442434d4400000100030000010000280100002000000000000000000000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d000000000000001100000000000000130000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f000000000000002500000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00123456789abcdef0a000b0003000800000000000000000074656e616e74006f6e6574656e616e745f646174612d38306d6573736167657318fc9ad0e703652f"
	const openHex = "564442434d4400000100040000010000380100003000000000000000000000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d000000000000001100000000000000130000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f9000000000000000000100000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00123456789abcdef0a000b0003000800000000000000000074656e616e74006f6e6574656e616e745f646174612d38306d65737361676573000000000000000015cd853dfe9c97170f6468bcf09b9743"
	const renewHex = "564442434d4400000100050000010000380100003000000000000000000000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d000000000000001100000000000000130000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f000000000000002500000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00123456789abcdef0a000b0003000800000000000000000074656e616e74006f6e6574656e616e745f646174612d38306d6573736167657315cd853dfe9c971715972079fe9c9717f0eff2370f100dc8"
	const revokeHex = "564442434d4400000100060000010000380100003000000000000000000000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d000000000000001100000000000000130000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f000000000000002500000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00123456789abcdef0a000b0003000800000000000000000074656e616e74006f6e6574656e616e745f646174612d38306d6573736167657315cd853dfe9c97170000000000000000f50786bc0af87943"
	const inlineHex = "564442434d50000001000100200101004501000005000000000000001d0000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d0000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f0000000000000025000000000000002900000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc00ef4fa8c2f3151c0180717a46d7a6b869a9b85ccd1e82be796cf4ea1eb9af0650123456789abcdef05000000000000000a000b0003000000000000000000000074656e616e74006f6e6574656e616e745f646174612d38300100ff6f6b19f88b6ce6077493"
	const referenceHex = "564442434d5000000100020020010100400100000000000000000000180000000102030405060708090a0b0c0d0e0f102122232425262728292a2b2c2d2e2f3003000000000000004142434445464748494a4b4c4d4e4f506162636465666768696a6b6c6d6e6f70050000000000000007000000000000000b000000000000000d0000000000000017000000000000001d000000000000008182838485868788898a8b8c8d8e8f901f0000000000000025000000000000002900000000000000a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe00123456789abcdef01000100000000000a000b0003000000000000000000000074656e616e74006f6e6574656e616e745f646174612d38304a28f106b5d70ef9"
	for _, tc := range []struct {
		name string
		got  []byte
		want string
	}{
		{"command", encodeCommand(t, testCommand()), commandHex},
		{"session_retire", encodeCommand(t, testSessionRetireCommand()), retireHex},
		{"session_release", encodeCommand(t, testSessionReleaseCommand()), releaseHex},
		{"session_open", encodeCommand(t, testSessionOpenCommand()), openHex},
		{"session_renew", encodeCommand(t, testSessionRenewCommand()), renewHex},
		{"session_revoke", encodeCommand(t, testSessionRevokeCommand()), revokeHex},
		{"inline_completion", encodeCompletion(t, testInlineCompletion()), inlineHex},
		{"reference_completion", encodeCompletion(t, testReferenceCompletion()), referenceHex},
	} {
		if got := hex.EncodeToString(tc.got); got != tc.want {
			t.Fatalf("%s golden mismatch:\ngot  %s\nwant %s", tc.name, got, tc.want)
		}
	}
}

func TestCommandEncodeRejectionsLeaveDestinationUnchanged(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Command)
		want   error
	}{
		{"zero_cluster", func(c *Command) { c.ClusterID = ID128{} }, ErrEnvelopeSemantic},
		{"zero_generation", func(c *Command) { c.RouteGeneration = 0 }, ErrEnvelopeSemantic},
		{"zero_client_epoch", func(c *Command) { c.ClientEpoch = 0 }, ErrEnvelopeSemantic},
		{"zero_client_sequence", func(c *Command) { c.ClientSequence = 0 }, ErrEnvelopeSemantic},
		{"unknown_command_kind", func(c *Command) { c.Kind = 99 }, ErrEnvelopeSemantic},
		{"ack_equal_sequence", func(c *Command) { c.AckThrough = c.ClientSequence }, ErrEnvelopeSemantic},
		{"ack_after_sequence", func(c *Command) { c.AckThrough = c.ClientSequence + 1 }, ErrEnvelopeSemantic},
		{"zero_fingerprint", func(c *Command) { c.Fingerprint = Digest{} }, ErrEnvelopeSemantic},
		{"empty_tenant", func(c *Command) { c.Tenant = nil }, ErrEnvelopeSemantic},
		{"long_tenant", func(c *Command) { c.Tenant = bytes.Repeat([]byte{'x'}, MaxIdentityBytes+1) }, ErrEnvelopeSemantic},
		{"invalid_distribution_utf8", func(c *Command) { c.Distribution = "\xff" }, ErrEnvelopeSemantic},
		{"long_collection", func(c *Command) { c.Collection = strings.Repeat("x", MaxCollectionBytes+1) }, ErrEnvelopeSemantic},
		{"no_mutations", func(c *Command) { c.Mutations = nil }, ErrEnvelopeSemantic},
		{"retire_with_mutations", func(c *Command) { c.Kind = CommandSessionRetire }, ErrEnvelopeSemantic},
		{"release_with_mutations", func(c *Command) { c.Kind = CommandSessionRelease }, ErrEnvelopeSemantic},
		{"open_with_mutations", func(c *Command) { c.Kind = CommandSessionOpen }, ErrEnvelopeSemantic},
		{"too_many_mutations", func(c *Command) { c.Mutations = make([]Mutation, MaxMutations+1) }, ErrEnvelopeSemantic},
		{"empty_key", func(c *Command) { c.Mutations[0].Key = nil }, ErrEnvelopeSemantic},
		{"long_key", func(c *Command) { c.Mutations[0].Key = bytes.Repeat([]byte{'a'}, MaxMutationKeyBytes+1) }, ErrEnvelopeSemantic},
		{"empty_put", func(c *Command) { c.Mutations[0].Value = nil }, ErrEnvelopeSemantic},
		{"long_value", func(c *Command) { c.Mutations[0].Value = make([]byte, MaxMutationValueBytes+1) }, ErrEnvelopeSemantic},
		{"delete_value", func(c *Command) { c.Mutations[1].Value = []byte("x") }, ErrEnvelopeSemantic},
		{"unknown_kind", func(c *Command) { c.Mutations[0].Kind = 99 }, ErrEnvelopeSemantic},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			command := testCommand()
			tc.mutate(&command)
			prefix := []byte("unchanged")
			got, err := AppendCommand(prefix, command)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if !bytes.Equal(got, prefix) {
				t.Fatal("failed encode changed destination")
			}
		})
	}
}

func TestCompletionEncodeRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Completion)
		want   error
	}{
		{"zero_identity", func(c *Completion) { c.GroupID = ID128{} }, ErrEnvelopeSemantic},
		{"zero_scalar", func(c *Completion) { c.AppliedSequence = 0 }, ErrEnvelopeSemantic},
		{"zero_format", func(c *Completion) { c.ResultFormat = 0 }, ErrEnvelopeSemantic},
		{"bad_digest", func(c *Completion) { c.ResultDigest[0] ^= 1 }, ErrEnvelopeSemantic},
		{"inline_length", func(c *Completion) { c.ResultLength++ }, ErrEnvelopeSemantic},
		{"small_reference", func(c *Completion) {
			c.Storage = CompletionDigestReference
			c.InlineResult = nil
			c.ResultLength = MaxInlineCompletionBytes
			c.ResultDigest = testDigest(1)
		}, ErrEnvelopeSemantic},
		{"reference_inline", func(c *Completion) {
			c.Storage = CompletionDigestReference
			c.ResultLength = MaxInlineCompletionBytes + 1
			c.ResultDigest = testDigest(1)
		}, ErrEnvelopeSemantic},
		{"unknown_storage", func(c *Completion) { c.Storage = 99 }, ErrEnvelopeSemantic},
		{"result_too_large", func(c *Completion) {
			c.Storage = CompletionDigestReference
			c.InlineResult = nil
			c.ResultLength = MaxCompletionResultBytes + 1
			c.ResultDigest = testDigest(1)
		}, ErrEnvelopeTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			completion := testInlineCompletion()
			tc.mutate(&completion)
			prefix := []byte("unchanged")
			got, err := AppendCompletion(prefix, completion)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if !bytes.Equal(got, prefix) {
				t.Fatal("failed encode changed destination")
			}
		})
	}
}

func TestBoundaries(t *testing.T) {
	command := testCommand()
	command.Tenant = bytes.Repeat([]byte{'t'}, MaxIdentityBytes)
	command.Distribution = strings.Repeat("d", MaxIdentityBytes)
	command.Shard = strings.Repeat("s", MaxIdentityBytes)
	command.Collection = strings.Repeat("c", MaxCollectionBytes)
	command.Mutations = []Mutation{{
		Kind:  MutationPut,
		Key:   bytes.Repeat([]byte{'k'}, MaxMutationKeyBytes),
		Value: bytes.Repeat([]byte{'v'}, MaxMutationValueBytes),
	}}
	if _, err := OpenCommand(encodeCommand(t, command)); err != nil {
		t.Fatalf("maximum field boundary: %v", err)
	}

	inline := testInlineCompletion()
	inline.InlineResult = bytes.Repeat([]byte{'r'}, MaxInlineCompletionBytes)
	inline.ResultLength = MaxInlineCompletionBytes
	inline.ResultDigest = CompletionResultDigest(
		inline.ResultCode, inline.ResultFormat, inline.InlineResult,
	)
	if _, err := OpenCompletion(encodeCompletion(t, inline)); err != nil {
		t.Fatalf("maximum inline completion: %v", err)
	}

	reference := testReferenceCompletion()
	reference.ResultLength = MaxCompletionResultBytes
	if _, err := OpenCompletion(encodeCompletion(t, reference)); err != nil {
		t.Fatalf("maximum referenced result: %v", err)
	}
}

func TestCommandExactAdmissionLimit(t *testing.T) {
	command := testCommand()
	const mutationCount = 4
	fixedBytes := commandHeaderBytes + envelopeChecksumBytes +
		len(command.Tenant) + len(command.Distribution) + len(command.Shard) +
		len(command.Collection) + mutationCount*mutationHeaderBytes + mutationCount
	lastValueBytes := MaxCommandBytes - fixedBytes - 3*MaxMutationValueBytes
	if lastValueBytes <= 0 || lastValueBytes >= MaxMutationValueBytes {
		t.Fatalf("invalid exact-limit arithmetic: final value = %d", lastValueBytes)
	}
	value := bytes.Repeat([]byte{'v'}, MaxMutationValueBytes)
	command.Mutations = []Mutation{
		{Kind: MutationPut, Key: []byte("a"), Value: value},
		{Kind: MutationPut, Key: []byte("b"), Value: value},
		{Kind: MutationPut, Key: []byte("c"), Value: value},
		{Kind: MutationPut, Key: []byte("d"), Value: value[:lastValueBytes]},
	}
	encoded := encodeCommand(t, command)
	if len(encoded) != MaxCommandBytes {
		t.Fatalf("exact-limit command length = %d, want %d", len(encoded), MaxCommandBytes)
	}
	if _, err := OpenCommand(encoded); err != nil {
		t.Fatalf("open exact-limit command: %v", err)
	}

	command.Mutations[3].Value = value[:lastValueBytes+1]
	prefix := []byte("unchanged")
	got, err := AppendCommand(prefix, command)
	if !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("MaxCommandBytes+1 error = %v, want %v", err, ErrEnvelopeTooLarge)
	}
	if !bytes.Equal(got, prefix) {
		t.Fatal("MaxCommandBytes+1 rejection changed destination")
	}
}

func TestCommandDecodeRejectsDamageAndSemanticCorruption(t *testing.T) {
	valid := encodeCommand(t, testCommand())
	for cut := 0; cut < len(valid); cut++ {
		if _, err := OpenCommand(valid[:cut]); err == nil {
			t.Fatalf("accepted truncation at %d", cut)
		}
	}
	trailing := append(append([]byte(nil), valid...), 0)
	if _, err := OpenCommand(trailing); !errors.Is(err, ErrEnvelopeCorrupt) {
		t.Fatalf("trailing byte = %v", err)
	}
	for offset := range valid {
		corruptBytes := append([]byte(nil), valid...)
		corruptBytes[offset] ^= 0x80
		if _, err := OpenCommand(corruptBytes); err == nil {
			t.Fatalf("accepted one-bit corruption at %d", offset)
		}
	}

	semanticCases := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{"format", func(b []byte) { binary.LittleEndian.PutUint16(b[8:10], 2) }, ErrUnsupportedFormat},
		{"header_bytes", func(b []byte) { binary.LittleEndian.PutUint16(b[12:14], commandHeaderBytes-1) }, ErrEnvelopeCorrupt},
		{"total_bytes", func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], uint32(len(b)-1)) }, ErrEnvelopeCorrupt},
		{"body_bytes", func(b []byte) { binary.LittleEndian.PutUint32(b[20:24], uint32(len(b))) }, ErrEnvelopeCorrupt},
		{"unknown_command_kind", func(b []byte) { b[10] = 99 }, ErrEnvelopeSemantic},
		{"retire_with_mutations", func(b []byte) { b[10] = commandWireSessionRetire }, ErrEnvelopeSemantic},
		{"release_with_mutations", func(b []byte) { b[10] = commandWireSessionRelease }, ErrEnvelopeSemantic},
		{"open_with_mutations", func(b []byte) { b[10] = commandWireSessionOpen }, ErrEnvelopeSemantic},
		{"zero_mutation_count", func(b []byte) { clear(b[24:28]) }, ErrEnvelopeSemantic},
		{"excess_mutation_count", func(b []byte) { binary.LittleEndian.PutUint32(b[24:28], 3) }, ErrEnvelopeCorrupt},
		{"flags", func(b []byte) { b[11] = 1 }, ErrEnvelopeSemantic},
		{"reserved", func(b []byte) { b[28] = 1 }, ErrEnvelopeSemantic},
		{"ack_equal_sequence", func(b []byte) {
			copy(b[248:256], b[192:200])
		}, ErrEnvelopeSemantic},
		{"ack_after_sequence", func(b []byte) {
			binary.LittleEndian.PutUint64(b[248:256], binary.LittleEndian.Uint64(b[192:200])+1)
		}, ErrEnvelopeSemantic},
		{"zero_identity", func(b []byte) { clear(b[32:48]) }, ErrEnvelopeSemantic},
		{"zero_generation", func(b []byte) { clear(b[160:168]) }, ErrEnvelopeSemantic},
		{"zero_tenant_length", func(b []byte) { clear(b[240:242]) }, ErrEnvelopeSemantic},
		{"long_tenant_length", func(b []byte) { binary.LittleEndian.PutUint16(b[240:242], MaxIdentityBytes+1) }, ErrEnvelopeSemantic},
		{"zero_collection_length", func(b []byte) { clear(b[246:248]) }, ErrEnvelopeSemantic},
		{"bad_utf8", func(b []byte) {
			tenant := int(binary.LittleEndian.Uint16(b[240:242]))
			b[commandHeaderBytes+tenant] = 0xff
		}, ErrEnvelopeSemantic},
		{"mutation_reserved", func(b []byte) { b[commandMutationOffset(b)+1] = 1 }, ErrEnvelopeSemantic},
		{"unknown_mutation", func(b []byte) { b[commandMutationOffset(b)] = 99 }, ErrEnvelopeSemantic},
		{"empty_key", func(b []byte) { clear(b[commandMutationOffset(b)+2 : commandMutationOffset(b)+4]) }, ErrEnvelopeSemantic},
		{"put_empty", func(b []byte) { binary.LittleEndian.PutUint32(b[commandMutationOffset(b)+4:], 0) }, ErrEnvelopeSemantic},
		{"value_too_large", func(b []byte) {
			binary.LittleEndian.PutUint32(b[commandMutationOffset(b)+4:], MaxMutationValueBytes+1)
		}, ErrEnvelopeSemantic},
		{"payload_overrun", func(b []byte) {
			binary.LittleEndian.PutUint32(b[commandMutationOffset(b)+4:], 1024)
		}, ErrEnvelopeCorrupt},
	}
	for _, tc := range semanticCases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := append([]byte(nil), valid...)
			tc.mutate(candidate)
			sealEnvelope(candidate)
			if _, err := OpenCommand(candidate); !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func commandMutationOffset(frame []byte) int {
	return commandHeaderBytes +
		int(binary.LittleEndian.Uint16(frame[240:242])) +
		int(binary.LittleEndian.Uint16(frame[242:244])) +
		int(binary.LittleEndian.Uint16(frame[244:246])) +
		int(binary.LittleEndian.Uint16(frame[246:248]))
}

func TestCompletionDecodeRejectsDamageAndDigestMismatch(t *testing.T) {
	valid := encodeCompletion(t, testInlineCompletion())
	for cut := 0; cut < len(valid); cut++ {
		if _, err := OpenCompletion(valid[:cut]); err == nil {
			t.Fatalf("accepted truncation at %d", cut)
		}
	}
	for offset := range valid {
		corruptBytes := append([]byte(nil), valid...)
		corruptBytes[offset] ^= 1
		if _, err := OpenCompletion(corruptBytes); err == nil {
			t.Fatalf("accepted one-bit corruption at %d", offset)
		}
	}
	inlineStart := completionInlineOffset(valid)
	structuredCases := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{"format", func(b []byte) { binary.LittleEndian.PutUint16(b[8:10], 2) }, ErrUnsupportedFormat},
		{"header_bytes", func(b []byte) { binary.LittleEndian.PutUint16(b[12:14], completionHeaderBytes-1) }, ErrEnvelopeCorrupt},
		{"total_bytes", func(b []byte) { binary.LittleEndian.PutUint32(b[16:20], uint32(len(b)-1)) }, ErrEnvelopeCorrupt},
		{"inline_bytes", func(b []byte) { binary.LittleEndian.PutUint32(b[20:24], uint32(len(b))) }, ErrEnvelopeSemantic},
		{"body_bytes", func(b []byte) { binary.LittleEndian.PutUint32(b[28:32], uint32(len(b))) }, ErrEnvelopeCorrupt},
		{"flags", func(b []byte) { b[11] = 1 }, ErrEnvelopeSemantic},
		{"reserved", func(b []byte) { b[287] = 1 }, ErrEnvelopeSemantic},
		{"zero_identity", func(b []byte) { clear(b[32:48]) }, ErrEnvelopeSemantic},
		{"zero_generation", func(b []byte) { clear(b[144:152]) }, ErrEnvelopeSemantic},
		{"zero_format", func(b []byte) { clear(b[14:16]) }, ErrEnvelopeSemantic},
		{"unknown_storage", func(b []byte) { b[10] = 99 }, ErrEnvelopeSemantic},
		{"zero_tenant_length", func(b []byte) { clear(b[272:274]) }, ErrEnvelopeSemantic},
		{"long_distribution_length", func(b []byte) { binary.LittleEndian.PutUint16(b[274:276], MaxIdentityBytes+1) }, ErrEnvelopeSemantic},
		{"result_length_mismatch", func(b []byte) { binary.LittleEndian.PutUint64(b[264:272], 99) }, ErrEnvelopeSemantic},
		{"result_length_too_large", func(b []byte) { binary.LittleEndian.PutUint64(b[264:272], MaxCompletionResultBytes+1) }, ErrEnvelopeSemantic},
		{"result_digest_mismatch", func(b []byte) { b[224] ^= 1 }, ErrEnvelopeSemantic},
		{"result_bytes_mismatch", func(b []byte) { b[inlineStart] ^= 1 }, ErrEnvelopeSemantic},
	}
	for _, tc := range structuredCases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := append([]byte(nil), valid...)
			tc.mutate(candidate)
			sealEnvelope(candidate)
			if _, err := OpenCompletion(candidate); !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func completionInlineOffset(frame []byte) int {
	return completionHeaderBytes +
		int(binary.LittleEndian.Uint16(frame[272:274])) +
		int(binary.LittleEndian.Uint16(frame[274:276])) +
		int(binary.LittleEndian.Uint16(frame[276:278]))
}

func TestCompletionResultDigestBindsMetadataAndBytes(t *testing.T) {
	base := CompletionResultDigest(0, 1, []byte("result"))
	for _, other := range []Digest{
		CompletionResultDigest(1, 1, []byte("result")),
		CompletionResultDigest(0, 2, []byte("result")),
		CompletionResultDigest(0, 1, []byte("Result")),
	} {
		if other == base {
			t.Fatal("completion digest did not bind all result metadata")
		}
	}
}

func TestAppendCommandRejectsWritableRegionAliases(t *testing.T) {
	frameBytes := len(encodeCommand(t, testCommand()))
	const prefix = "prefix"
	tests := []struct {
		name string
		bind func(*Command, []byte)
	}{
		{"tenant", func(command *Command, region []byte) {
			source := append([]byte(nil), command.Tenant...)
			copy(region, source)
			command.Tenant = region[:len(source)]
		}},
		{"distribution", func(command *Command, region []byte) {
			source := []byte(command.Distribution)
			copy(region, source)
			command.Distribution = unsafe.String(unsafe.SliceData(region), len(source))
		}},
		{"key", func(command *Command, region []byte) {
			source := append([]byte(nil), command.Mutations[0].Key...)
			copy(region, source)
			command.Mutations[0].Key = region[:len(source)]
		}},
		{"value", func(command *Command, region []byte) {
			source := append([]byte(nil), command.Mutations[0].Value...)
			copy(region, source)
			command.Mutations[0].Value = region[:len(source)]
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backing := make([]byte, len(prefix)+frameBytes)
			copy(backing, prefix)
			dst := backing[:len(prefix)]
			command := testCommand()
			tc.bind(&command, backing[len(prefix):])
			before := append([]byte(nil), backing...)
			got, err := AppendCommand(dst, command)
			if !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
			if !bytes.Equal(got, dst) || !bytes.Equal(backing, before) {
				t.Fatal("alias rejection modified destination backing")
			}
		})
	}
}

func TestAppendCompletionRejectsWritableRegionAliases(t *testing.T) {
	frameBytes := len(encodeCompletion(t, testInlineCompletion()))
	const prefix = "prefix"
	tests := []struct {
		name string
		bind func(*Completion, []byte)
	}{
		{"tenant", func(completion *Completion, region []byte) {
			source := append([]byte(nil), completion.Tenant...)
			copy(region, source)
			completion.Tenant = region[:len(source)]
		}},
		{"distribution", func(completion *Completion, region []byte) {
			source := []byte(completion.Distribution)
			copy(region, source)
			completion.Distribution = unsafe.String(unsafe.SliceData(region), len(source))
		}},
		{"shard", func(completion *Completion, region []byte) {
			source := []byte(completion.Shard)
			copy(region, source)
			completion.Shard = unsafe.String(unsafe.SliceData(region), len(source))
		}},
		{"inline_result", func(completion *Completion, region []byte) {
			source := append([]byte(nil), completion.InlineResult...)
			copy(region, source)
			completion.InlineResult = region[:len(source)]
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backing := make([]byte, len(prefix)+frameBytes)
			copy(backing, prefix)
			dst := backing[:len(prefix)]
			completion := testInlineCompletion()
			tc.bind(&completion, backing[len(prefix):])
			before := append([]byte(nil), backing...)
			got, err := AppendCompletion(dst, completion)
			if !errors.Is(err, ErrEnvelopeSemantic) {
				t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
			}
			if !bytes.Equal(got, dst) || !bytes.Equal(backing, before) {
				t.Fatal("alias rejection modified destination backing")
			}
		})
	}
}

func TestAppendCompletionBytesRejectsWritableRegionAliases(t *testing.T) {
	completion := testCompletionBytes(testInlineCompletion())
	frameBytes := len(encodeCompletion(t, testInlineCompletion()))
	const prefix = "prefix"
	backing := make([]byte, len(prefix)+frameBytes)
	copy(backing, prefix)
	region := backing[len(prefix):]
	copy(region, completion.Distribution)
	completion.Distribution = region[:len(completion.Distribution)]
	dst := backing[:len(prefix)]
	before := append([]byte(nil), backing...)

	got, err := AppendCompletionBytes(dst, completion)
	if !errors.Is(err, ErrEnvelopeSemantic) {
		t.Fatalf("error = %v, want %v", err, ErrEnvelopeSemantic)
	}
	if !bytes.Equal(got, dst) || !bytes.Equal(backing, before) {
		t.Fatal("alias rejection modified destination backing")
	}
}

func TestAppendCommandRelocatesBeforeReadingOldWritableRegion(t *testing.T) {
	command := testCommand()
	backing := make([]byte, 1+len(command.Tenant))
	backing[0] = 0x7f
	copy(backing[1:], command.Tenant)
	command.Tenant = backing[1:]

	encoded, err := AppendCommand(backing[:1], command)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != 0x7f {
		t.Fatal("append changed destination prefix")
	}
	view, err := OpenCommand(encoded[1:])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(view.Tenant, command.Tenant) {
		t.Fatal("growth did not preserve input from old writable region")
	}
}

func TestViewsBorrowExactInputWithClampedCapacity(t *testing.T) {
	commandBytes := encodeCommand(t, testCommand())
	command, err := OpenCommand(commandBytes)
	if err != nil {
		t.Fatal(err)
	}
	if &command.Bytes()[0] != &commandBytes[0] || &command.Tenant[0] != &commandBytes[commandHeaderBytes] {
		t.Fatal("command view copied its input")
	}
	for _, field := range []struct {
		name  string
		bytes []byte
	}{
		{"command bytes", command.Bytes()},
		{"tenant", command.Tenant},
		{"distribution", command.Distribution},
		{"shard", command.Shard},
		{"collection", command.Collection},
	} {
		assertBorrowedSliceClamped(t, field.name, field.bytes, commandBytes)
	}
	iterator := command.Mutations()
	assertBorrowedSliceClamped(t, "mutation iterator", iterator.b, commandBytes)
	for ordinal := 0; iterator.Next(); ordinal++ {
		mutation := iterator.Mutation()
		assertBorrowedSliceClamped(t, "mutation key", mutation.Key, commandBytes)
		assertBorrowedSliceClamped(t, "mutation value", mutation.Value, commandBytes)
		assertBorrowedSliceClamped(t, "remaining mutation iterator", iterator.b, commandBytes)
	}

	completionBytes := encodeCompletion(t, testInlineCompletion())
	completion, err := OpenCompletion(completionBytes)
	if err != nil {
		t.Fatal(err)
	}
	if &completion.Bytes()[0] != &completionBytes[0] ||
		&completion.Tenant[0] != &completionBytes[completionHeaderBytes] {
		t.Fatal("completion view copied its input")
	}
	for _, field := range []struct {
		name  string
		bytes []byte
	}{
		{"completion bytes", completion.Bytes()},
		{"completion tenant", completion.Tenant},
		{"completion distribution", completion.Distribution},
		{"completion shard", completion.Shard},
		{"inline result", completion.InlineResult},
	} {
		assertBorrowedSliceClamped(t, field.name, field.bytes, completionBytes)
	}
}

func assertBorrowedSliceClamped(t *testing.T, name string, borrowed, envelope []byte) {
	t.Helper()
	if cap(borrowed) != len(borrowed) {
		t.Fatalf("%s capacity = %d, length = %d", name, cap(borrowed), len(borrowed))
	}
	before := append([]byte(nil), envelope...)
	grown := append(borrowed, 0xa5)
	if len(grown) != len(borrowed)+1 || !bytes.Equal(envelope, before) {
		t.Fatalf("append through %s overwrote validated envelope bytes", name)
	}
}

func TestStructShapesDoNotAcquireHiddenOwnership(t *testing.T) {
	// This is intentionally a shape check rather than a byte-size ABI promise:
	// borrowed variable fields must remain slices and the fixed identities arrays.
	commandType := reflect.TypeFor[CommandView]()
	for _, name := range []string{"Tenant", "Distribution", "Shard", "Collection"} {
		field, ok := commandType.FieldByName(name)
		if !ok || field.Type.Kind() != reflect.Slice {
			t.Fatalf("CommandView.%s is not a borrowed slice", name)
		}
	}
	completionType := reflect.TypeFor[CompletionView]()
	for _, name := range []string{"Tenant", "Distribution", "Shard", "InlineResult"} {
		field, ok := completionType.FieldByName(name)
		if !ok || field.Type.Kind() != reflect.Slice {
			t.Fatalf("CompletionView.%s is not a borrowed slice", name)
		}
	}
}
