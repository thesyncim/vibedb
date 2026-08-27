package rf3testfixture

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"google.golang.org/protobuf/proto"
)

// PrepareSplitRuntime retains the same private namespace and static voter cut
// that prepare-rf3 creates. Raw process fixtures must not rely on serve-rf3 to
// invent missing startup artifacts.
func PrepareSplitRuntime(root string, bootstrap raftstore.Bootstrap) error {
	if root == "" || bootstrap.Snapshot == nil {
		return errors.New("rf3 process fixture: missing split bootstrap")
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(bootstrap.Snapshot)
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
