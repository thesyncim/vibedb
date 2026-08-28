package shardservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestMembershipStableAuthorityDoesNotExpandTransportCapabilities(t *testing.T) {
	for _, pair := range [][2]replication.CommandAuthorityClass{
		{replication.CommandAuthorityData, replication.CommandAuthorityMembershipStableData},
		{replication.CommandAuthorityRouteSession, replication.CommandAuthorityMembershipStableRouteSession},
	} {
		for _, capability := range []serviceauthz.Capability{0, serviceauthz.CapabilityDataRead,
			serviceauthz.CapabilityDataWrite, serviceauthz.CapabilityTopology, serviceauthz.CapabilityMembership,
			serviceauthz.CapabilityTransactionRecovery, serviceauthz.CapabilityRequestLedger, serviceauthz.CapabilityExecutionPin} {
			for _, kind := range []replication.CommandKind{replication.CommandMutationBatch,
				replication.CommandTransaction, replication.CommandRouteGate, replication.CommandSessionOpen,
				replication.CommandSessionRetire, replication.CommandSessionRelease, replication.CommandRequestLedger,
				replication.CommandExecutionPin, replication.CommandRetainedPrune} {
				if replicatedCommandCapabilityMatches(capability, kind, pair[0]) != replicatedCommandCapabilityMatches(capability, kind, pair[1]) {
					t.Fatalf("stable authority expanded transport permission: capability=%v kind=%v class=%v", capability, kind, pair)
				}
			}
		}
	}
}
