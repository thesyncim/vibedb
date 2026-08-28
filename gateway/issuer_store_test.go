package gateway

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestGatewayIssuerStorePersistsImmutableHandshake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "issuer")
	store, err := OpenGatewayIssuerStore(GatewayIssuerStoreOptions{Path: path, Lanes: 4})
	if err != nil {
		t.Fatal(err)
	}
	contract, err := store.Contract()
	if err != nil {
		t.Fatal(err)
	}
	if contract.Installation == (replication.ID128{}) || contract.Epoch != 1 ||
		len(contract.Lanes) != 4 || contract.Digest == (replication.Digest{}) {
		t.Fatalf("contract = %+v", contract)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenGatewayIssuerStore(GatewayIssuerStoreOptions{Path: path, Lanes: 4,
		Installation: contract.Installation, Epoch: contract.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	again, err := reopened.Contract()
	if err != nil {
		t.Fatal(err)
	}
	if again.Installation != contract.Installation || again.Epoch != contract.Epoch ||
		again.Digest != contract.Digest || len(again.Lanes) != len(contract.Lanes) {
		t.Fatalf("reopened contract = %+v, want %+v", again, contract)
	}
	for index := range again.Lanes {
		if again.Lanes[index] != contract.Lanes[index] || again.Lanes[index] == (requestledger.IssuerLane{}) {
			t.Fatalf("lane[%d] = %x, want %x", index, again.Lanes[index], contract.Lanes[index])
		}
	}
}

func TestGatewayIssuerGrantProjectsClientIdentityWithoutAllocating(t *testing.T) {
	store, err := OpenGatewayIssuerStore(GatewayIssuerStoreOptions{
		Path: filepath.Join(t.TempDir(), "issuer"), Lanes: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	tenant := requestledger.Digest{0x31}
	principal := requestledger.PrincipalID{0x41}
	grant, err := store.GrantLane(requestledger.ScopeAuthenticated, tenant, principal, 1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.RequestKey(grant, requestledger.RequestID{0x51}, 70)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.RequestKey(grant, requestledger.RequestID{0x52}, 9)
	if err != nil {
		t.Fatal(err)
	}
	if first.IssuerSequence != 70 || second.IssuerSequence != 9 ||
		first.Request == second.Request || first.IssuerLane != grant.Lane ||
		first.IssuerEpoch != grant.Epoch || first.Principal != principal ||
		first.TenantDigest != tenant || !first.Valid() || !second.Valid() {
		t.Fatalf("keys = %+v %+v", first, second)
	}
}

func TestGatewayIssuerGrantRejectsForgery(t *testing.T) {
	store, err := OpenGatewayIssuerStore(GatewayIssuerStoreOptions{
		Path: filepath.Join(t.TempDir(), "issuer"), Lanes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	grant, err := store.GrantLane(requestledger.ScopeAuthenticated,
		requestledger.Digest{1}, requestledger.PrincipalID{2}, 0)
	if err != nil {
		t.Fatal(err)
	}
	cases := []GatewayIssuerLaneGrant{grant, grant, grant, grant, grant, grant}
	cases[0].Installation[0] ^= 1
	cases[1].Epoch++
	cases[2].Lane[0] ^= 1
	cases[3].TenantDigest[0] ^= 1
	cases[4].Principal[0] ^= 1
	cases[5].Authenticator[0] ^= 1
	for index, candidate := range cases {
		if _, err = store.RequestKey(candidate, requestledger.RequestID{3}, 1); !errors.Is(err, ErrGatewayIssuerGrant) {
			t.Fatalf("forgery %d error = %v", index, err)
		}
	}
}

func TestGatewayIssuerStoreExclusiveFixedAndCorruptFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "issuer")
	store, err := OpenGatewayIssuerStore(GatewayIssuerStoreOptions{Path: path, Lanes: 2})
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := store.Contract()
	if _, err = OpenGatewayIssuerStore(GatewayIssuerStoreOptions{Path: path, Lanes: 2}); !errors.Is(err, ErrGatewayIssuerStoreInUse) {
		t.Fatalf("second open error = %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenGatewayIssuerStore(GatewayIssuerStoreOptions{Path: path, Lanes: 3}); !errors.Is(err, ErrGatewayIssuerStore) {
		t.Fatalf("lane resize error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenGatewayIssuerStore(GatewayIssuerStoreOptions{Path: path, Lanes: 2,
		Installation: contract.Installation, Epoch: 1}); !errors.Is(err, ErrGatewayIssuerStore) {
		t.Fatalf("corrupt open error = %v", err)
	}
}

func TestGatewayIssuerStoreRecordCanonicalAndTamperProof(t *testing.T) {
	installation := replication.ID128{0x71}
	lanes := []requestledger.IssuerLane{{1}, {2}, {3}}
	secret := replication.Digest{0x81}
	raw, err := appendGatewayIssuerStoreRecord(nil, installation, 7, lanes, secret)
	if err != nil {
		t.Fatal(err)
	}
	gotInstallation, epoch, gotLanes, gotSecret, err := openGatewayIssuerStoreRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := appendGatewayIssuerStoreRecord(nil, gotInstallation, epoch, gotLanes, gotSecret)
	if err != nil {
		t.Fatal(err)
	}
	if gotInstallation != installation || epoch != 7 || gotSecret != secret || string(raw) != string(canonical) {
		t.Fatal("issuer store record did not round-trip canonically")
	}
	for _, offset := range []int{0, 8, 24, 32, 40, 104, len(raw) - 1} {
		tampered := append([]byte(nil), raw...)
		tampered[offset] ^= 1
		if _, _, _, _, err = openGatewayIssuerStoreRecord(tampered); !errors.Is(err, ErrGatewayIssuerStore) {
			t.Fatalf("tamper at %d accepted: %v", offset, err)
		}
	}
}
