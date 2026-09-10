package gateway

import (
	"errors"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"testing"
)

func TestBootstrapNodeDirectoryPublishesCompleteCutAndDoesNotOverwrite(t *testing.T) {
	authority, _, snapshot := newCatalogAuthorityFixture(t)
	records := make([]NodeRecord, 0, len(snapshot.replicatedReplicas))
	for i, replica := range snapshot.replicatedReplicas {
		record := NodeRecord{NodeID: replica.Node, Incarnation: replica.NodeIncarnation, ServiceKeyDigest: replication.Digest{byte(i + 1)},
			DataEndpoint: distribution.EndpointID(replica.Endpoint), NativeEndpoint: distribution.EndpointID(replica.NativeEndpoint), ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint),
			DataAddress: replica.DataAddress, NativeAddress: replica.Address, ControlAddress: replica.ControlAddress,
			FailureDomain: replica.Endpoint, Roles: NodeRoleStorage, Lifecycle: NodeActive, Revision: 1, CatalogGeneration: snapshot.Generation()}
		records = append(records, record)
	}
	if len(records) < 2 {
		t.Fatal("fixture requires multiple physical nodes")
	}
	if err := authority.BootstrapNodeDirectory(t.Context(), records[:1]); !errors.Is(err, ErrScalingIdentity) {
		t.Fatalf("partial cut=%v", err)
	}
	if _, err := authority.ReadNodeDirectoryCut(t.Context()); !errors.Is(err, ErrScalingNodeMissing) {
		t.Fatalf("partial bootstrap published directory: %v", err)
	}
	if err := authority.BootstrapNodeDirectory(t.Context(), records); err != nil {
		t.Fatal(err)
	}
	cut, err := authority.ReadNodeDirectoryCut(t.Context())
	if err != nil || len(cut.Nodes) != len(records) {
		t.Fatalf("cut=%+v err=%v", cut, err)
	}
	records[0].ServiceKeyDigest[0] ^= 0xff
	if err := authority.BootstrapNodeDirectory(t.Context(), records); err != nil {
		t.Fatal(err)
	}
	after, err := authority.ReadNodeDirectoryCut(t.Context())
	if err != nil || after.Digest != cut.Digest {
		t.Fatalf("bootstrap changed established directory: %v", err)
	}
}
