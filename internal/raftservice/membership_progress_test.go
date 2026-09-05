package raftservice

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

type progressAuthority struct {
	membershipTestAuthority
	publications      map[raftmember.GroupKey]int
	promoted, cleared []raftmember.GroupKey
}

func (a *progressAuthority) PublishCommittedAuthority(g raftmember.GroupKey, _ uint64, _ *pb.ConfState) error {
	a.publications[g]++
	return nil
}
func (a *progressAuthority) PublishDurablePromotion(g raftmember.GroupKey, _ raftmember.DurablePromotionProof) error {
	a.promoted = append(a.promoted, g)
	return nil
}
func (a *progressAuthority) ClearDurablePromotion(g raftmember.GroupKey) error {
	a.cleared = append(a.cleared, g)
	return nil
}

type membershipProgressHost struct {
	ownerHost
	publicationCalls map[raftmember.GroupKey]int
	promotionFound   bool
	publicationErr   error
	progress         []multiraft.Progress
	cancel           context.CancelFunc
}

func (h *membershipProgressHost) Publication(g raftmember.GroupKey) (raftmodel.Publication, error) {
	h.publicationCalls[g]++
	return raftmodel.Publication{ReplicaSetVersion: 1, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}, h.publicationErr
}
func (h *membershipProgressHost) DurablePromotion(raftmember.GroupKey, uint64) (raftmember.DurablePromotionProof, bool, error) {
	return raftmember.DurablePromotionProof{}, h.promotionFound, nil
}
func (h *membershipProgressHost) RunOne() (multiraft.Progress, bool, error) {
	if len(h.progress) == 0 {
		h.cancel()
		return multiraft.Progress{}, false, nil
	}
	p := h.progress[0]
	h.progress = h.progress[1:]
	return p, true, nil
}
func (*membershipProgressHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}
func (*membershipProgressHost) Close() error { return nil }

func TestOwnerProgressPublishesOnlyChangedGroupAfterStartup(t *testing.T) {
	group := peerServerTestGroup()
	other := group
	other.GroupID[0]++
	authority := &progressAuthority{publications: make(map[raftmember.GroupKey]int)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := &membershipProgressHost{publicationCalls: make(map[raftmember.GroupKey]int), cancel: cancel}
	// Every kind of progress retains the authority synchronization seam, including
	// persistence (unapplied promotion) and snapshots (committed membership).
	for _, kind := range []raftmember.DriveKind{raftmember.DrivePersisted, raftmember.DriveSnapshotFinished, raftmember.DriveEntry, raftmember.DriveReadStatesFinished} {
		host.progress = append(host.progress, multiraft.Progress{Group: group, Kind: multiraft.ProgressReady, ReadyKind: kind})
	}
	owner := &Owner{host: host, authority: authority, groups: []raftmember.GroupKey{group, other},
		ready: make(chan struct{}), done: make(chan struct{})}
	if err := owner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if host.publicationCalls[group] != 5 || host.publicationCalls[other] != 1 || authority.publications[group] != 5 || authority.publications[other] != 1 {
		t.Fatalf("publication calls host=%v authority=%v", host.publicationCalls, authority.publications)
	}
}

func TestOwnerGroupAuthorityRefreshRetainsPromotionAndErrors(t *testing.T) {
	group := peerServerTestGroup()
	authority := &progressAuthority{publications: make(map[raftmember.GroupKey]int)}
	host := &membershipProgressHost{publicationCalls: make(map[raftmember.GroupKey]int)}
	owner := &Owner{host: host, authority: authority}
	if err := owner.syncMembershipAuthority(group); err != nil {
		t.Fatal(err)
	}
	// A newly installed grant is observed on the group's next progress even if
	// the committed replica set is unchanged; no cached version can hide it.
	authority.grant = membershipgrant.Grant{Group: group, TargetMember: 4}
	authority.found = true
	host.promotionFound = true
	if err := owner.syncMembershipAuthority(group); err != nil {
		t.Fatal(err)
	}
	host.promotionFound = false
	if err := owner.syncMembershipAuthority(group); err != nil {
		t.Fatal(err)
	}
	if len(authority.promoted) != 1 || authority.promoted[0] != group || len(authority.cleared) != 1 || authority.cleared[0] != group {
		t.Fatalf("promotion updates: %+v", authority)
	}
	host.publicationErr = errors.New("unavailable publication")
	if err := owner.syncMembershipAuthority(group); !errors.Is(err, host.publicationErr) {
		t.Fatalf("publication error lost: %v", err)
	}
}
