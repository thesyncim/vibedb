package gateway

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestPostgresSessionRequireCapabilityEnforcesLeastPrivilege(t *testing.T) {
	readOnly := &postgresSession{capabilities: serviceauthz.CapabilityDataRead}
	if err := readOnly.requireCapability(serviceauthz.CapabilityDataRead); err != nil {
		t.Fatalf("readOnly SELECT rejected: %v", err)
	}
	if err := readOnly.requireCapability(serviceauthz.CapabilityDataWrite); !errors.Is(err, ErrReplicatedUnauthorized) {
		t.Fatalf("readOnly WRITE err = %v, want ErrReplicatedUnauthorized", err)
	}
	if err := readOnly.requireCapability(serviceauthz.CapabilitySchema); !errors.Is(err, ErrReplicatedUnauthorized) {
		t.Fatalf("readOnly DDL err = %v, want ErrReplicatedUnauthorized", err)
	}

	readWrite := &postgresSession{capabilities: serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite}
	if err := readWrite.requireCapability(serviceauthz.CapabilityDataRead); err != nil {
		t.Fatalf("readWrite SELECT rejected: %v", err)
	}
	if err := readWrite.requireCapability(serviceauthz.CapabilityDataWrite); err != nil {
		t.Fatalf("readWrite WRITE rejected: %v", err)
	}
	if err := readWrite.requireCapability(serviceauthz.CapabilitySchema); !errors.Is(err, ErrReplicatedUnauthorized) {
		t.Fatalf("readWrite DDL err = %v, want ErrReplicatedUnauthorized", err)
	}

	// Zero capabilities preserves legacy loopback behavior where Authority alone gates access.
	unrestricted := &postgresSession{}
	for _, cap := range []serviceauthz.Capability{
		serviceauthz.CapabilityDataRead,
		serviceauthz.CapabilityDataWrite,
		serviceauthz.CapabilitySchema,
	} {
		if err := unrestricted.requireCapability(cap); err != nil {
			t.Fatalf("unrestricted cap 0x%x rejected: %v", uint64(cap), err)
		}
	}
}
