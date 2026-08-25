package membershipgrant

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

type refreshSource struct {
	grant Grant
	found bool
	err   error
}

func (source *refreshSource) ReadMembershipGrant(
	context.Context, raftmember.GroupKey,
) (Grant, bool, error) {
	return source.grant, source.found, source.err
}

type refreshSink struct {
	grant Grant
	found bool
}

func (sink *refreshSink) CurrentTransitionGrant(
	raftmember.GroupKey,
) (Grant, bool, error) {
	return sink.grant, sink.found, nil
}

func (sink *refreshSink) InstallTransitionGrant(grant Grant) error {
	if sink.found && sink.grant != grant {
		return ErrRefreshConflict
	}
	sink.grant, sink.found = grant, true
	return nil
}

func (sink *refreshSink) RevokeTransitionGrant(expected Grant) error {
	if !sink.found || sink.grant != expected {
		return ErrRefreshConflict
	}
	sink.grant, sink.found = Grant{}, false
	return nil
}

func TestRefreshRestartConflictAndAbsenceProvedRevocation(t *testing.T) {
	grant := refreshTestGrant()
	source := &refreshSource{grant: grant, found: true}
	for restart := 0; restart < 3; restart++ {
		sink := new(refreshSink)
		got, err := Refresh(context.Background(), source, sink, grant.Group)
		if err != nil || got != grant || !sink.found || sink.grant != grant {
			t.Fatalf("restart %d refresh=%+v err=%v sink=%+v", restart, got, err, sink)
		}
		foreign := grant
		foreign.MetadataEpoch++
		sink.grant = foreign
		if _, err = Refresh(context.Background(), source, sink, grant.Group); !errors.Is(err, ErrRefreshConflict) {
			t.Fatalf("foreign runtime grant refresh=%v", err)
		}
	}

	sink := &refreshSink{grant: grant, found: true}
	if err := RefreshRevocation(context.Background(), source, sink, grant); !errors.Is(err, ErrGrantPresent) {
		t.Fatalf("present revocation=%v", err)
	}
	source.found, source.grant = false, Grant{}
	if err := RefreshRevocation(context.Background(), source, sink, grant); err != nil || sink.found {
		t.Fatalf("absent revocation=%v sink=%+v", err, sink)
	}
	if err := RefreshRevocation(context.Background(), source, new(refreshSink), grant); err != nil {
		t.Fatalf("reopened absent revocation=%v", err)
	}
}

func refreshTestGrant() (grant Grant) {
	grant.Group.ClusterID[0] = 1
	grant.Group.ClusterIncarnation[0] = 2
	grant.Group.TopologyRecoveryEpoch = 3
	grant.Group.ShardIncarnation[0] = 4
	grant.Group.GroupID[0] = 5
	grant.TransitionID[0] = 6
	grant.MetadataEpoch = 7
	grant.CatalogGeneration = 8
	grant.SourceMember = 1
	grant.TargetMember = 3
	return grant
}
