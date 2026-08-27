package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type refusingRestoreCatalog struct{ called bool }

func (catalog *refusingRestoreCatalog) ProposeRestoreActivation(context.Context, []byte) ([]byte, error) {
	catalog.called = true
	return nil, errors.New("unexpected proposal")
}
func (catalog *refusingRestoreCatalog) ObserveRestoreActivation(context.Context, [32]byte) (clusterrestore.CatalogWitness, error) {
	catalog.called = true
	return clusterrestore.CatalogWitness{}, errors.New("unexpected observation")
}

func TestRestoreActivationRequiresIndependentTargetCapabilityBeforeEffects(t *testing.T) {
	operator := serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 7}
	for _, capability := range []serviceauthz.Capability{
		serviceauthz.CapabilityBackup, serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityMembership,
	} {
		policy, err := serviceauthz.NewPolicy(7, []serviceauthz.Entry{{Node: operator.Node, Capabilities: capability}})
		if err != nil {
			t.Fatal(err)
		}
		gate, err := serviceauthz.NewGate(policy)
		if err != nil {
			t.Fatal(err)
		}
		catalog := &refusingRestoreCatalog{}
		_, _, err = ActivateRestore(t.Context(), RestoreActivationOptions{
			Gate: gate, Operator: operator, Catalog: catalog,
		})
		if !errors.Is(err, ErrRestoreActivation) || catalog.called {
			t.Fatalf("capability=%d called=%t err=%v", capability, catalog.called, err)
		}
	}
}
