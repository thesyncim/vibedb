package rf3testfixture

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftstore"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// PrepareSplitRuntime retains the same private namespace and static voter cut
// that prepare-rf3 creates. Raw process fixtures must not rely on serve-rf3 to
// invent missing startup artifacts.
func PrepareSplitRuntime(root string, bootstrap raftstore.Bootstrap) error {
	if root == "" || bootstrap.Snapshot == nil {
		return errors.New("rf3 process fixture: missing split bootstrap")
	}
	// A child bootstrap is a fresh index-one voter cut, not the source's
	// applied snapshot (which has a different data grammar and may have learners).
	voters := bootstrap.Snapshot.GetMetadata().GetConfState().GetVoters()
	if len(voters) != 1 && len(voters) != 3 {
		return errors.New("rf3 process fixture: invalid split voter count")
	}
	for i, voter := range voters {
		if voter == 0 || i > 0 && voters[i-1] >= voter {
			return errors.New("rf3 process fixture: noncanonical split voters")
		}
	}
	index, term := uint64(1), uint64(1)
	child := &pb.Snapshot{Data: []byte("vibedb-rf3-split-child-bootstrap"),
		Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: append([]uint64(nil), voters...)}},
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(child)
	if err != nil {
		return err
	}
	for _, name := range []string{"split-runtime", "split-children"} {
		if err = os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(root, "split-children", "static-bootstrap.pb"), raw, 0o600)
}
