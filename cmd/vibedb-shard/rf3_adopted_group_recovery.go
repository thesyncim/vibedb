package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3AdoptedGroupRecovery struct {
	bundle        rf3ManifestGroup
	base          sqldriver.ReplicatedShardStoreIdentity
	apply         sqldriver.ReplicatedApplyIdentity
	runtimeDigest [32]byte
}

func (inventory *rf3AdoptedGroupInventory) recoveryGroups(local rafttransport.NodeID) ([]rf3AdoptedGroupRecovery, error) {
	if inventory == nil {
		return nil, nil
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.root == nil || inventory.failed {
		return nil, errRF3Serving
	}
	roots := inventory.manifest.groupBundles()
	result := make([]rf3AdoptedGroupRecovery, 0, inventory.liveCount())
	for _, entry := range inventory.entries {
		if entry.operation == ([32]byte{}) {
			continue
		}
		resources, err := inventory.entryResources(entry)
		if err != nil {
			return nil, err
		}
		registry := resources.Registry
		paths, err := registry.childPaths(entry.operation, uint8(entry.child))
		if err != nil {
			return nil, err
		}
		raw, err := readRF3AdoptedReceipt(registry, entry.operation, uint8(entry.child))
		if err != nil {
			return nil, err
		}
		receipt, err := splitcontroller.OpenChildPrepareReceipt(raw)
		if err != nil || receipt.Operation != splitcontroller.OperationID(entry.operation) || uint64(receipt.Child) != entry.child ||
			receipt.ReceiptDigest != entry.receipt || receipt.Target.CertificateDigest != entry.certificate || receipt.Target.Node != local ||
			entry.group == rf3DynamicTemplateSlot && receipt.RequestDigest != resources.PreparationDigest {
			return nil, errors.Join(errRF3Serving, err)
		}
		target := receipt.Target
		if entry.group == rf3DynamicTemplateSlot {
			if _, found, err := inventory.templates.Resolve(entry.operation, uint8(entry.child), target); err != nil || !found {
				return nil, errors.Join(errRF3Serving, err)
			}
		} else {
			index, _, found := rf3SplitChildRegistryForTarget(inventory.manifest, entry.operation, uint8(entry.child), target)
			if !found || uint64(index) != entry.group {
				return nil, errRF3Serving
			}
		}
		bundle := rf3ManifestGroup{ChildRegistry: registry}
		bundle.WAL = rf3ManifestWAL{Path: paths.WAL, KeyID: registry.WAL.KeyID,
			KeyMaterialPath: registry.WAL.KeyMaterialPath, Options: registry.WAL.Options}
		bundle.SQL = rf3ManifestSQL{Path: paths.Database}
		binding := target.SQL.Binding
		bundle.Route = rf3ManifestGroupRoute{Group: groupFromBinding(binding), Distribution: binding.Distribution, Shard: binding.Shard,
			AllocationGeneration: binding.AllocationGeneration, MemberID: binding.MemberID, StoreID: binding.StoreID,
			MemberRoot: paths.Root, SplitRuntimeRoot: paths.Root, MembershipGrantPath: filepath.Join(paths.Root, "membership-grant")}
		bundle.Members, bundle.MemberCount, bundle.EnrolledTarget = registry.Members, registry.MemberCount, nil
		for _, prior := range result {
			if prior.bundle.Route.Group == bundle.Route.Group || prior.bundle.Route.StoreID == bundle.Route.StoreID {
				return nil, errRF3Serving
			}
		}
		for _, initial := range roots {
			if initial.Route.Group == bundle.Route.Group || initial.Route.StoreID == bundle.Route.StoreID {
				return nil, errRF3Serving
			}
		}
		result = append(result, rf3AdoptedGroupRecovery{bundle: bundle, base: target.SQL, apply: target.Apply, runtimeDigest: entry.certificate})
	}
	return result, nil
}

// Caller holds inventory.mu. A dynamic template carries the immediate source
// group and exact immutable preparation; a static entry keeps its historical
// startup-manifest index semantics.
func (inventory *rf3AdoptedGroupInventory) entryResources(entry rf3AdoptedGroupEntry) (rf3SplitChildResources, error) {
	if inventory == nil || !inventory.validEntry(entry) {
		return rf3SplitChildResources{}, errRF3Serving
	}
	if entry.group == rf3DynamicTemplateSlot {
		resources, found, err := inventory.templates.Read(entry.operation, uint8(entry.child))
		if err != nil || !found {
			return rf3SplitChildResources{}, errors.Join(errRF3Serving, err)
		}
		return resources, nil
	}
	groups := inventory.manifest.groupBundles()
	if entry.group >= uint64(len(groups)) {
		return rf3SplitChildResources{}, errRF3Serving
	}
	group := groups[entry.group]
	return rf3SplitChildResources{Slot: int(entry.group), SourceGroup: group.Route.Group, Registry: group.ChildRegistry}, nil
}

// Resolve every referenced component inside the exact manifest-owned root.
// In particular, a matching receipt hash does not authorize following a
// replaced operation directory, child directory, or receipt symlink.
func readRF3AdoptedReceipt(template rf3ManifestSplitChildRegistry, operation [32]byte, child uint8) ([]byte, error) {
	paths, err := template.childPaths(operation, child)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(template.Root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(errRF3Serving, err)
	}
	root, err := os.OpenRoot(template.Root)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	relative, err := filepath.Rel(template.Root, paths.Root)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{filepath.Dir(relative), relative} {
		info, err := root.Lstat(name)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.Join(errRF3Serving, err)
		}
	}
	for _, name := range []string{filepath.Join(relative, rf3ChildPrepareReceiptName), filepath.Join(relative, filepath.Base(paths.Database)), filepath.Join(relative, filepath.Base(paths.WAL))} {
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.Join(errRF3Serving, err)
		}
	}
	file, err := root.Open(filepath.Join(relative, rf3ChildPrepareReceiptName))
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, splitcontroller.MaxChildPreparationBytes+1))
	if err = errors.Join(readErr, file.Close()); err != nil || len(raw) > splitcontroller.MaxChildPreparationBytes {
		return nil, errors.Join(errRF3Serving, err)
	}
	return raw, nil
}
