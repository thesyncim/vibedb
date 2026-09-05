package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

// The inventory is persisted before materialization so a restart reuses the
// exact allocated identities. Node-log groups share durability ownership;
// every group retains an independent SQL root.
type devTableInventory struct {
	NextPlacement uint64              `json:"next_placement,omitempty"`
	Tables        []devTableProvision `json:"tables"`
}
type devTableProvision struct {
	Table            string    `json:"table"`
	Distribution     string    `json:"distribution,omitempty"`
	PrimaryKey       string    `json:"primary_key"`
	CreateTable      string    `json:"create_table"`
	GroupID          string    `json:"group_id"`
	ShardIncarnation string    `json:"shard_incarnation"`
	Stores           [3]string `json:"stores"`
	// Physical RF3 tables retain the exact node and group roots selected for
	// each voter. This lets restart reconcile the same live node manifests
	// without deriving a replacement placement from mutable directory order.
	PlacementOrdinal uint64    `json:"placement_ordinal,omitempty"`
	PhysicalNodes    [3]string `json:"physical_nodes,omitempty"`
	GroupRoots       [3]string `json:"group_roots,omitempty"`
	ServeManifests   [3]string `json:"serve_manifests,omitempty"`
}

func (table devTableProvision) distribution() string {
	if table.Distribution != "" {
		return table.Distribution
	}
	return "table-" + table.Table
}

func (table devTableProvision) artifactStem() string {
	if table.Distribution == "" || len(table.GroupID) < 12 {
		return "table-" + table.Table
	}
	return "table-" + table.Table + "-" + table.GroupID[:12]
}

func parseDevTableDDL(source string) (string, string, error) {
	tree, err := sqlast.ParseStatement(source)
	if err != nil || tree.CreateTable == nil || tree.CreateTable.IfNotExists || len(tree.CreateTable.PrimaryKey) != 1 {
		return "", "", fmt.Errorf("%w: one CREATE TABLE with one primary key is required", errDevCluster)
	}
	name := tree.CreateTable.Table
	if name == devDataTable || name == devLedgerTable || name == gateway.ReplicatedCatalogTable || len(name) == 0 || len(name) > 63 {
		return "", "", errDevCluster
	}
	for i, c := range name {
		if !(c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
			return "", "", errDevCluster
		}
	}
	primary := string(tree.CreateTable.PrimaryKey[0].AppendPointer(nil))
	if err := sqldriver.ValidateReplicatedChildSchemaDefinition(name, primary, source, nil, nil); err != nil {
		return "", "", err
	}
	return name, primary, nil
}

func devRandomIdentity() (string, error) {
	var id [16]byte
	if _, err := cryptorand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func ensureDevTables(root, shardBinary string, cluster *devClusterManifest, schemaPath string) error {
	cluster.additionalCatalogs, cluster.dataServeManifests = nil, nil
	inventoryPath := filepath.Join(root, "tables.vibejson")
	var inventory devTableInventory
	raw, err := readDevFile(inventoryPath, 4<<20)
	if err == nil {
		err = vibejson.Unmarshal(raw, &inventory)
		if err == nil {
			canonical, canonicalErr := vibejson.Marshal(&inventory)
			if canonicalErr != nil || !bytes.Equal(canonical, raw) {
				err = errors.Join(errDevCluster, canonicalErr)
			}
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if cluster.Nodes != devClusterRF3 {
		if schemaPath != "" || len(inventory.Tables) != 0 {
			return fmt.Errorf("%w: additional groups require RF3", errDevCluster)
		}
		return nil
	}
	if cluster.PhysicalNodes != 0 {
		return ensureDevPhysicalTables(root, shardBinary, cluster, schemaPath, inventoryPath, inventory)
	}
	if schemaPath != "" {
		ddl, err := readDevFile(schemaPath, sqldriver.ReplicatedChildSchemaMaxBytes)
		if err != nil {
			return err
		}
		name, primary, err := parseDevTableDDL(string(ddl))
		if err != nil {
			return err
		}
		found := false
		for _, table := range inventory.Tables {
			if table.Table == name {
				if table.CreateTable != string(ddl) {
					return fmt.Errorf("%w: table %q already has another declaration", errDevCluster, name)
				}
				found = true
			}
		}
		if !found {
			if len(inventory.Tables) >= 63 {
				return fmt.Errorf("%w: data-node group limit", errDevCluster)
			}
			table := devTableProvision{Table: name, PrimaryKey: primary, CreateTable: string(ddl)}
			table.GroupID, err = devRandomIdentity()
			if err != nil {
				return err
			}
			// A retired physical identity is never reused. Keeping the random
			// group suffix in the distribution name also prevents stale route
			// caches from aliasing a later table with the same SQL name.
			table.Distribution = "table-" + name + "-" + table.GroupID[:12]
			table.ShardIncarnation, err = devRandomIdentity()
			if err != nil {
				return err
			}
			for i := range table.Stores {
				table.Stores[i], err = devRandomIdentity()
				if err != nil {
					return err
				}
			}
			inventory.Tables = append(inventory.Tables, table)
			raw, err = vibejson.Marshal(&inventory)
			if err != nil {
				return err
			}
			if err := replaceDevFile(inventoryPath, raw); err != nil {
				return err
			}
		}
	}
	if len(inventory.Tables) == 0 {
		for i, member := range cluster.DataMembers {
			raw, err := readDevFile(member.ServeManifest, 4<<20)
			if err != nil {
				return err
			}
			path := filepath.Join(filepath.Dir(cluster.DataMembers[i].ServeManifest), "serve-multigroup.vibejson")
			if err := replaceDevFile(path, raw); err != nil {
				return err
			}
			cluster.dataServeManifests = append(cluster.dataServeManifests, path)
		}
		return nil
	}
	if len(inventory.Tables) > 63 {
		return errDevCluster
	}
	groups := make([][]string, 3)
	for i, member := range cluster.DataMembers {
		groups[i] = []string{member.ServeManifest}
	}
	seen := make(map[string]bool, len(inventory.Tables))
	for _, table := range inventory.Tables {
		name, primary, err := parseDevTableDDL(table.CreateTable)
		if err != nil || name != table.Table || primary != table.PrimaryKey || seen[name] ||
			table.Distribution != "" && table.Distribution != "table-"+table.Table+"-"+table.GroupID[:min(12, len(table.GroupID))] {
			return errors.Join(errDevCluster, err)
		}
		seen[name] = true
		members, group, err := prepareDevTable(root, shardBinary, *cluster, table)
		if err != nil {
			return err
		}
		for i, member := range members {
			groups[i] = append(groups[i], member.ServeManifest)
		}
		path := filepath.Join(root, table.artifactStem()+"-catalog.vibejson")
		if raw, err := readDevFile(path, 4<<20); err == nil {
			addition, err := gateway.OpenReplicatedTableProvision(raw)
			if err != nil {
				return err
			}
			declarations, descriptors := addition.ReplicatedTableDeclarations(), addition.ReplicatedShardDescriptors()
			if len(declarations) != 1 || declarations[0].CreateTable != table.CreateTable || len(descriptors) != 1 || descriptors[0].Group != group {
				return errDevCluster
			}
			if err := replaceDevFile(filepath.Join(root, "table-"+name+"-catalog.vibejson"), raw); err != nil {
				return err
			}
			cluster.additionalCatalogs = append(cluster.additionalCatalogs, path)
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		endpoints := make(map[distribution.EndpointID]string)
		dist := distribution.DistributionName(table.distribution())
		// Endpoint names identify physical nodes, not tables. Reuse the data
		// nodes' provisioned capacity and topology identity for every group.
		route, err := inspectDevPreparedRoute(endpoints, "data", dist, "all", name, primary, group, replication.Digest{}, true, members)
		if err != nil {
			return err
		}
		manifest, err := distribution.NewManifest(dist, 1, []distribution.Shard{{ID: "all", AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: route.leaders, Epoch: 1}})
		if err != nil {
			return err
		}
		rangeID, lineage, forwarding := deriveDevLogicalRangeAuthority(group, dist, "all", route.digest)
		addition, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{Name: dist, Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
			Placements:    []distribution.TablePlacement{{Table: name, Distribution: dist, Columns: []string{primary}}},
			Manifests:     []*distribution.Manifest{manifest},
		}, endpoints, 1, nil, nil, []gateway.ReplicatedShardDescriptor{{Distribution: dist, Shard: "all", Group: group, AllocationGeneration: 1,
			Command:             raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: route.schemaGeneration, RelationManifestDigest: route.digest, RoutingVersion: 1, RouteGeneration: 1},
			LogicalSchemaDigest: route.table.LogicalSchemaDigest, RangeIdentity: rangeID, LineageDigest: lineage, ForwardingRuleDigest: forwarding, Replicas: route.replicas,
		}}, []gateway.ReplicatedTableProfile{route.table}, []gateway.ReplicatedTableDeclaration{{Table: name, CreateTable: table.CreateTable}})
		if err != nil {
			return err
		}
		provision, err := gateway.AppendReplicatedTableProvision(nil, addition)
		if err != nil {
			return err
		}
		if err := writeDevFileOnce(path, provision); err != nil {
			return err
		}
		if err := replaceDevFile(filepath.Join(root, "table-"+name+"-catalog.vibejson"), provision); err != nil {
			return err
		}
		cluster.additionalCatalogs = append(cluster.additionalCatalogs, path)
	}
	for i, paths := range groups {
		raw, err := composeDevGroupManifest(paths)
		if err != nil {
			return err
		}
		path := filepath.Join(filepath.Dir(cluster.DataMembers[i].ServeManifest), "serve-multigroup.vibejson")
		if err := retainDevGroupInventoryManifest(filepath.Dir(path), paths); err != nil {
			return err
		}
		if err := replaceDevFile(path, raw); err != nil {
			return err
		}
		cluster.dataServeManifests = append(cluster.dataServeManifests, path)
	}
	return nil
}

// retireDevTable removes only the supervisor's active inventory reference.
// WAL and SQL directories remain as orphaned recovery evidence; a separate
// bounded GC may reclaim them after operators no longer need rollback data.
func retireDevTable(root, shardBinary string, cluster *devClusterManifest, table string) (bool, error) {
	path := filepath.Join(root, "tables.vibejson")
	raw, err := readDevFile(path, 4<<20)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var inventory devTableInventory
	if err = vibejson.Unmarshal(raw, &inventory); err != nil {
		return false, err
	}
	removed := false
	next := inventory.Tables[:0]
	for _, candidate := range inventory.Tables {
		if candidate.Table == table {
			removed = true
			continue
		}
		next = append(next, candidate)
	}
	if !removed {
		return false, nil
	}
	inventory.Tables = next
	raw, err = vibejson.Marshal(&inventory)
	if err != nil {
		return false, err
	}
	if err = replaceDevFile(path, raw); err != nil {
		return false, err
	}
	if err = ensureDevTables(root, shardBinary, cluster, ""); err != nil {
		return false, err
	}
	return true, nil
}

// The certified-child inventory is bound to the exact manifest bytes at its
// last open. Retain that predecessor before publishing an append-only config,
// allowing recovery to prove the old group ordinals rather than reset them.
func retainDevGroupInventoryManifest(root string, paths []string) error {
	state, err := readDevFile(filepath.Join(root, "adopted-groups.state"), 64<<10)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(state) < 64 || string(state[:8]) != "VDBLIVEG" {
		return errDevCluster
	}
	var expected [32]byte
	copy(expected[:], state[8:40])
	directory := filepath.Join(root, "prepared-manifests")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	target := filepath.Join(directory, hex.EncodeToString(expected[:])+".vibejson")
	if raw, err := readDevFile(target, 4<<20); err == nil {
		if sha256.Sum256(raw) != expected {
			return errDevCluster
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// The currently published multigroup manifest is the exact predecessor
	// for both append and retirement transitions. Prefer it over reconstructing
	// prefixes, which cannot represent removal of a middle group.
	if current, currentErr := readDevFile(filepath.Join(root, "serve-multigroup.vibejson"), 4<<20); currentErr == nil {
		if sha256.Sum256(current) == expected {
			return writeDevFileOnce(target, current)
		}
	} else if !errors.Is(currentErr, os.ErrNotExist) {
		return currentErr
	}
	for count := 1; count <= len(paths); count++ {
		var raw []byte
		if count == 1 {
			raw, err = readDevFile(paths[0], 4<<20)
		} else {
			raw, err = composeDevGroupManifest(paths[:count])
		}
		if err != nil {
			return err
		}
		if sha256.Sum256(raw) == expected {
			return writeDevFileOnce(target, raw)
		}
	}
	return fmt.Errorf("%w: cannot prove retained group inventory manifest", errDevCluster)
}

func prepareDevTable(root, binary string, cluster devClusterManifest, table devTableProvision) ([]devClusterMember, raftmember.GroupKey, error) {
	var group raftmember.GroupKey
	groupID, err := decodeDev16(table.GroupID)
	if err != nil {
		return nil, group, err
	}
	shardID, err := decodeDev16(table.ShardIncarnation)
	if err != nil {
		return nil, group, err
	}
	members := make([]devClusterMember, 3)
	for i, member := range cluster.DataMembers {
		raw, err := readDevFile(filepath.Join(root, fmt.Sprintf("prepare-data-member-%d.vibejson", i+1)), 1<<20)
		if err != nil {
			return nil, group, err
		}
		var prepare devPrepareManifest
		if err := vibejson.Unmarshal(raw, &prepare); err != nil {
			return nil, group, err
		}
		clusterID, err := decodeDev16(prepare.ClusterID)
		if err != nil {
			return nil, group, err
		}
		incarnation, err := decodeDev16(prepare.ClusterIncarnation)
		if err != nil {
			return nil, group, err
		}
		group = raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: incarnation, TopologyRecoveryEpoch: prepare.TopologyRecoveryEpoch, GroupID: groupID, ShardIncarnation: shardID}
		prepare.Root = filepath.Join(root, fmt.Sprintf("%s-member-%d", table.artifactStem(), i+1))
		prepare.Table, prepare.CreateTable, prepare.Apply.ShardKey = table.Table, table.CreateTable, table.PrimaryKey
		prepare.Distribution, prepare.Shard = table.distribution(), "all"
		prepare.GroupID, prepare.ShardIncarnation, prepare.StoreID = table.GroupID, table.ShardIncarnation, table.Stores[i]
		if _, err := decodeDev16(prepare.StoreID); err != nil {
			return nil, group, err
		}
		path := filepath.Join(root, fmt.Sprintf("prepare-%s-member-%d.vibejson", table.artifactStem(), i+1))
		raw, err = vibejson.Marshal(&prepare)
		if err != nil {
			return nil, group, err
		}
		if err := writeDevFileOnce(path, raw); err != nil {
			return nil, group, err
		}
		member.Store, member.ServeManifest = table.Stores[i], filepath.Join(prepare.Root, "serve-rf3.vibejson")
		if _, err := os.Stat(member.ServeManifest); errors.Is(err, os.ErrNotExist) {
			command := "prepare-rf3"
			if cluster.NodeLog {
				command = "prepare-node-group-rf3"
			}
			if err := runDevCommand(binary, command, "-manifest", path); err != nil {
				return nil, group, err
			}
		} else if err != nil {
			return nil, group, err
		}
		identityRaw, err := readDevFile(filepath.Join(prepare.Root, "sql-identity.vibejson"), 1<<20)
		if err != nil {
			return nil, group, err
		}
		var identity sqldriver.ReplicatedShardStoreIdentity
		if err := identity.UnmarshalJSON(identityRaw); err != nil {
			return nil, group, err
		}
		if err := sqldriver.ValidateReplicatedChildSchema(identity, table.CreateTable, nil, nil); err != nil {
			return nil, group, err
		}
		members[i] = member
	}
	return members, group, nil
}

// Compose the already-validated, group-local bundles while preserving the
// original node's shared listeners, credentials and control journals.
func composeDevGroupManifest(paths []string) ([]byte, error) {
	if len(paths) < 2 || len(paths) > 64 {
		return nil, errDevCluster
	}
	var process map[string]json.RawMessage
	var bundles []json.RawMessage
	for i, path := range paths {
		raw, err := readDevFile(path, 4<<20)
		if err != nil {
			return nil, err
		}
		var source map[string]json.RawMessage
		if err := json.Unmarshal(raw, &source); err != nil {
			return nil, err
		}
		var split map[string]json.RawMessage
		if err := json.Unmarshal(source["split_control"], &split); err != nil {
			return nil, err
		}
		bundle := make(map[string]json.RawMessage)
		for _, key := range []string{"wal", "sql", "route", "members"} {
			if len(source[key]) == 0 {
				return nil, errDevCluster
			}
			bundle[key] = source[key]
			delete(source, key)
		}
		bundle["child_registry"] = split["child_registry"]
		if len(bundle["child_registry"]) == 0 {
			return nil, errDevCluster
		}
		bundleRaw, err := orderedDevManifestObject(bundle, []string{"wal", "sql", "route", "child_registry", "members"})
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, bundleRaw)
		if i == 0 {
			var registry map[string]json.RawMessage
			if err := json.Unmarshal(split["child_registry"], &registry); err != nil {
				return nil, err
			}
			split["max_operations"] = registry["max_operations"]
			delete(split, "child_registry")
			source["split_control"], err = orderedDevManifestObject(split, []string{"journal_path", "max_records", "max_file_bytes", "grants", "max_operations"})
			if err != nil {
				return nil, err
			}
			process = source
		} else {
			if len(source["node_log"]) != 0 && !bytes.Equal(process["node_log"], source["node_log"]) {
				return nil, fmt.Errorf("%w: group changes shared node log", errDevCluster)
			}
			for _, key := range []string{"listeners", "tls", "authorization_policy"} {
				if !bytes.Equal(process[key], source[key]) {
					return nil, fmt.Errorf("%w: group changes shared %s", errDevCluster, key)
				}
			}
		}
	}
	raw, err := json.Marshal(bundles)
	if err != nil {
		return nil, err
	}
	process["groups"] = raw
	order := []string{"listeners", "tls", "authorization_policy", "replica_control", "split_control", "groups"}
	if len(process["node_log"]) != 0 {
		order = append([]string{"node_log"}, order...)
	}
	return orderedDevManifestObject(process, order)
}

func orderedDevManifestObject(fields map[string]json.RawMessage, order []string) ([]byte, error) {
	if len(fields) != len(order) {
		return nil, errDevCluster
	}
	result := []byte{'{'}
	for i, key := range order {
		value, ok := fields[key]
		if !ok || len(value) == 0 {
			return nil, errDevCluster
		}
		if i > 0 {
			result = append(result, ',')
		}
		result = append(result, '"')
		result = append(result, key...)
		result = append(result, '"', ':')
		result = append(result, value...)
	}
	return append(result, '}'), nil
}

func writeDevFileOnce(path string, raw []byte) error {
	previous, err := readDevFile(path, 4<<20)
	if err == nil {
		if !bytes.Equal(previous, raw) {
			return fmt.Errorf("%w: retained provisioning file differs: %s", errDevCluster, path)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeDevExclusive(path, raw, 0o600)
}

func replaceDevFile(path string, raw []byte) error {
	if previous, err := readDevFile(path, 4<<20); err == nil && bytes.Equal(previous, raw) {
		return nil
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".table-provision-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(raw)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncDevDir(filepath.Dir(path))
}
