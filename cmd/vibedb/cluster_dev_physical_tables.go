package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

const devPhysicalMaxGroups = 64

func ensureDevPhysicalTables(root, binary string, cluster *devClusterManifest, schemaPath, inventoryPath string, inventory devTableInventory) error {
	if cluster == nil || !validDevManifest(*cluster, root) || cluster.PhysicalNodes == 0 ||
		len(inventory.Tables) > int(cluster.PhysicalNodes)*devPhysicalMaxGroups/devClusterRF3 {
		return errDevCluster
	}
	// A retained plan is the only authority for a new root. Reconcile plans
	// before allocating another table, including a crash before cluster.json
	// recorded the reserved ordinals. No directory enumeration chooses IDs.
	if err := reserveDevPhysicalTablePlans(cluster, inventory); err != nil {
		return err
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
			table, err := planDevPhysicalTable(*cluster, name, primary, string(ddl), inventory.NextPlacement)
			if err != nil {
				return err
			}
			inventory.Tables = append(inventory.Tables, table)
			inventory.NextPlacement++
			raw, err := vibejson.Marshal(&inventory)
			if err != nil {
				return err
			}
			// Persist group/store IDs, exact voters and roots before invoking
			// even the first preparer. Every failure resumes this same plan.
			if err := replaceDevFile(inventoryPath, raw); err != nil {
				return err
			}
			if err := reserveDevPhysicalTablePlans(cluster, inventory); err != nil {
				return err
			}
		}
	}
	if err := persistDevPhysicalClusterManifest(root, *cluster); err != nil {
		return err
	}
	for _, table := range inventory.Tables {
		path := filepath.Join(root, table.artifactStem()+"-catalog.vibejson")
		_, fragmentErr := readDevFile(path, 4<<20)
		if fragmentErr != nil && !errors.Is(fragmentErr, os.ErrNotExist) {
			return fragmentErr
		}
		// The immutable fragment is published only after all three stores pass
		// cold validation. A completed table may already have live writers or a
		// newer schema generation; validate its original proof without reopening
		// or freezing those live SQL/apply identities.
		completed := fragmentErr == nil
		members, group, err := prepareDevPhysicalTable(root, binary, *cluster, table, completed)
		if err != nil {
			return err
		}
		provision, err := buildDevPhysicalTableProvision(table, members, group, completed)
		if err != nil {
			return err
		}
		// The bytes include the complete endpoint/store/route/schema witness.
		// A stale or substituted fragment must not be accepted on recovery.
		if err := writeDevFileOnce(path, provision); err != nil {
			return err
		}
		cluster.additionalCatalogs = append(cluster.additionalCatalogs, path)
	}
	return updateDevPhysicalGatewayCatalogs(*cluster, cluster.additionalCatalogs)
}

func planDevPhysicalTable(cluster devClusterManifest, name, primary, ddl string, ordinal uint64) (devTableProvision, error) {
	table := devTableProvision{Table: name, PrimaryKey: primary, CreateTable: ddl, PlacementOrdinal: ordinal}
	if ordinal >= uint64(cluster.PhysicalNodes)*devPhysicalMaxGroups/devClusterRF3 {
		return table, fmt.Errorf("%w: physical group limit", errDevCluster)
	}
	placement := devPhysicalPlacement(int(cluster.PhysicalNodes), int(ordinal)+3)
	for index, nodeIndex := range placement {
		node := cluster.NodeManifests[nodeIndex]
		if len(node.Groups) >= devPhysicalMaxGroups {
			return table, fmt.Errorf("%w: physical node %d group limit", errDevCluster, nodeIndex+1)
		}
		table.PhysicalNodes[index] = node.Node
		table.ServeManifests[index] = node.ServeManifest
		table.GroupRoots[index] = filepath.Join(filepath.Dir(node.ServeManifest), fmt.Sprintf("group-%d", len(node.Groups)))
	}
	var err error
	table.GroupID, err = devRandomIdentity()
	if err != nil {
		return table, err
	}
	table.ShardIncarnation, err = devRandomIdentity()
	if err != nil {
		return table, err
	}
	table.Distribution = "table-" + name + "-" + table.GroupID[:12]
	for index := range table.Stores {
		table.Stores[index], err = devRandomIdentity()
		if err != nil {
			return table, err
		}
	}
	return table, nil
}

func reserveDevPhysicalTablePlans(cluster *devClusterManifest, inventory devTableInventory) error {
	if inventory.NextPlacement > uint64(cluster.PhysicalNodes)*devPhysicalMaxGroups/devClusterRF3 {
		return errDevCluster
	}
	next := append([]devPhysicalNode(nil), cluster.NodeManifests...)
	for index := range next {
		next[index].Groups = append([]string(nil), next[index].Groups...)
	}
	seenNames, seenIDs, seenRoots := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, members := range [][]devClusterMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers} {
		for _, member := range members {
			seenRoots[member.GroupRoot], seenIDs[member.Store] = true, true
		}
	}
	var priorOrdinal uint64
	for tableIndex, table := range inventory.Tables {
		name, primary, err := parseDevTableDDL(table.CreateTable)
		if err != nil || name != table.Table || primary != table.PrimaryKey || seenNames[name] ||
			table.PlacementOrdinal >= inventory.NextPlacement || tableIndex > 0 && table.PlacementOrdinal <= priorOrdinal ||
			len(table.GroupID) != 32 || table.Distribution != "table-"+name+"-"+table.GroupID[:12] {
			return errors.Join(errDevCluster, err)
		}
		priorOrdinal, seenNames[name] = table.PlacementOrdinal, true
		for _, encoded := range append([]string{table.GroupID, table.ShardIncarnation}, table.Stores[:]...) {
			if _, err := decodeDev16(encoded); err != nil || seenIDs[encoded] {
				return errors.Join(errDevCluster, err)
			}
			seenIDs[encoded] = true
		}
		placement := devPhysicalPlacement(int(cluster.PhysicalNodes), int(table.PlacementOrdinal)+3)
		for index, nodeIndex := range placement {
			node := &next[nodeIndex]
			groupRoot := table.GroupRoots[index]
			ordinal, err := devPhysicalGroupOrdinal(filepath.Dir(node.ServeManifest), groupRoot)
			if err != nil || table.PhysicalNodes[index] != node.Node || table.ServeManifests[index] != node.ServeManifest || seenRoots[groupRoot] || ordinal >= devPhysicalMaxGroups {
				return errors.Join(errDevCluster, err)
			}
			seenRoots[groupRoot] = true
			if ordinal == len(node.Groups) {
				node.Groups = append(node.Groups, groupRoot)
			} else if ordinal >= len(node.Groups) || node.Groups[ordinal] != groupRoot {
				return errDevCluster
			}
		}
	}
	cluster.NodeManifests = next
	return nil
}

func devPhysicalGroupOrdinal(root, group string) (int, error) {
	if filepath.Dir(group) != root || !filepath.IsAbs(group) || filepath.Clean(group) != group {
		return 0, errDevCluster
	}
	base := filepath.Base(group)
	if len(base) <= len("group-") || base[:len("group-")] != "group-" {
		return 0, errDevCluster
	}
	ordinal, err := strconv.Atoi(base[len("group-"):])
	if err != nil || ordinal < 0 || base != fmt.Sprintf("group-%d", ordinal) {
		return 0, errDevCluster
	}
	return ordinal, nil
}

func devPhysicalNodeMember(cluster devClusterManifest, node string) (devClusterMember, error) {
	for _, members := range [][]devClusterMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers} {
		for _, member := range members {
			if member.Node == node {
				return member, nil
			}
		}
	}
	return devClusterMember{}, errDevCluster
}

func plannedDevPhysicalMembers(cluster devClusterManifest, table devTableProvision) ([]devClusterMember, error) {
	if cluster.PhysicalNodes != devClusterPhysicalNodes3 && cluster.PhysicalNodes != devClusterPhysicalNodes6 ||
		len(cluster.NodeManifests) != int(cluster.PhysicalNodes) || table.PlacementOrdinal >= uint64(cluster.PhysicalNodes)*devPhysicalMaxGroups/devClusterRF3 {
		return nil, errDevCluster
	}
	members := make([]devClusterMember, devClusterRF3)
	placement := devPhysicalPlacement(int(cluster.PhysicalNodes), int(table.PlacementOrdinal)+3)
	for index, nodeIndex := range placement {
		node := cluster.NodeManifests[nodeIndex]
		ordinal, err := devPhysicalGroupOrdinal(filepath.Dir(node.ServeManifest), table.GroupRoots[index])
		if err != nil || table.PhysicalNodes[index] != node.Node || table.ServeManifests[index] != node.ServeManifest ||
			ordinal >= len(node.Groups) || node.Groups[ordinal] != table.GroupRoots[index] {
			return nil, errors.Join(errDevCluster, err)
		}
		member, err := devPhysicalNodeMember(cluster, node.Node)
		if err != nil {
			return nil, err
		}
		member.Member, member.Store = uint64(index+1), table.Stores[index]
		member.GroupRoot, member.ServeManifest = table.GroupRoots[index], table.ServeManifests[index]
		members[index] = member
	}
	return members, nil
}

func prepareDevPhysicalTable(root, binary string, cluster devClusterManifest, table devTableProvision, completed bool) ([]devClusterMember, raftmember.GroupKey, error) {
	members, err := plannedDevPhysicalMembers(cluster, table)
	if err != nil {
		return nil, raftmember.GroupKey{}, err
	}
	for index, member := range members {
		base, err := devPhysicalNodeMember(cluster, member.Node)
		if err != nil {
			return nil, raftmember.GroupKey{}, err
		}
		raw, err := readDevFile(filepath.Dir(base.GroupRoot)+"."+filepath.Base(base.GroupRoot)+".prepare.vibejson", 1<<20)
		if err != nil {
			return nil, raftmember.GroupKey{}, err
		}
		var prepare devPrepareManifest
		if err := vibejson.Unmarshal(raw, &prepare); err != nil || prepare.Root != base.GroupRoot || prepare.StoreID != base.Store || prepare.MemberID != base.Member {
			return nil, raftmember.GroupKey{}, errors.Join(errDevCluster, err)
		}
		if !devReadAuthorityEqual(prepare.ReadAuthority, cluster.ReadAuthority) ||
			prepare.ReadAuthority != nil && !validDevReadAuthority(*prepare.ReadAuthority) {
			return nil, raftmember.GroupKey{}, fmt.Errorf("%w: retained group read authority differs from cluster policy", errDevCluster)
		}
		prepare.Root, prepare.MemberID, prepare.StoreID = member.GroupRoot, member.Member, member.Store
		prepare.Table, prepare.CreateTable = table.Table, table.CreateTable
		prepare.Distribution, prepare.Shard = table.Distribution, "all"
		prepare.GroupID, prepare.ShardIncarnation = table.GroupID, table.ShardIncarnation
		// The template may be a ledger-only node. Never inherit its ledger
		// home identity or key profile into an ordinary user table.
		prepare.Apply = devPrepareApplyProfile(table.PrimaryKey, replication.Digest{})
		prepare.Members = make([]devPrepareMember, len(members))
		prepare.ReadAuthority = cloneDevReadAuthority(cluster.ReadAuthority)
		for peerIndex, peer := range members {
			prepare.Members[peerIndex] = devPrepareMember{MemberID: peer.Member, NodeID: peer.Node, PeerAddress: peer.Peer}
			if cluster.ReadAuthority != nil {
				prepare.Members[peerIndex].StoreID = peer.Store
				prepare.Members[peerIndex].NativeAddress = peer.Native
			}
		}
		raw, err = vibejson.Marshal(&prepare)
		if err != nil {
			return nil, raftmember.GroupKey{}, err
		}
		path := filepath.Join(root, fmt.Sprintf("prepare-%s-member-%d.vibejson", table.artifactStem(), index+1))
		if completed {
			retained, err := readDevFile(path, 1<<20)
			if err != nil || !bytes.Equal(retained, raw) {
				return nil, raftmember.GroupKey{}, errors.Join(errDevCluster, err)
			}
		} else if err := writeDevFileOnce(path, raw); err != nil {
			return nil, raftmember.GroupKey{}, err
		}
		if _, err := os.Stat(filepath.Join(member.GroupRoot, "serve-rf3.vibejson")); errors.Is(err, os.ErrNotExist) {
			if completed {
				return nil, raftmember.GroupKey{}, errors.Join(errDevCluster, err)
			}
			if err := runDevCommand(binary, "prepare-node-group-rf3", "-manifest", path); err != nil {
				return nil, raftmember.GroupKey{}, err
			}
		} else if err != nil {
			return nil, raftmember.GroupKey{}, err
		}
	}
	members, group, err := devPhysicalTableMembersAt(cluster, table, !completed)
	if err != nil {
		return nil, group, err
	}
	// Every SQL root is durable before any live manifest advertises the new
	// group. A retry can find any prefix of these manifest publications.
	for _, member := range members {
		if err := reconcileDevPhysicalNodeGroup(member, !completed); err != nil {
			return nil, group, err
		}
	}
	return members, group, nil
}

func devPhysicalTableMembers(cluster devClusterManifest, table devTableProvision) ([]devClusterMember, raftmember.GroupKey, error) {
	return devPhysicalTableMembersAt(cluster, table, true)
}

func devPhysicalTableMembersAt(cluster devClusterManifest, table devTableProvision, initialSchema bool) ([]devClusterMember, raftmember.GroupKey, error) {
	members, err := plannedDevPhysicalMembers(cluster, table)
	group := mustDevGroup(cluster.Members)
	if err != nil || group == (raftmember.GroupKey{}) {
		return nil, group, errors.Join(errDevCluster, err)
	}
	group.GroupID, err = decodeDev16(table.GroupID)
	if err != nil {
		return nil, group, err
	}
	group.ShardIncarnation, err = decodeDev16(table.ShardIncarnation)
	if err != nil {
		return nil, group, err
	}
	for _, member := range members {
		raw, err := readDevFile(filepath.Join(member.GroupRoot, "sql-identity.vibejson"), 1<<20)
		if err != nil {
			return nil, group, err
		}
		var identity sqldriver.ReplicatedShardStoreIdentity
		if err := identity.UnmarshalJSON(raw); err != nil {
			return nil, group, err
		}
		storeID, err := decodeDev16(member.Store)
		binding := identity.Binding
		if err != nil || binding.ClusterID != group.ClusterID || binding.ClusterIncarnation != group.ClusterIncarnation ||
			binding.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch || binding.GroupID != group.GroupID || binding.ShardIncarnation != group.ShardIncarnation ||
			binding.Distribution != table.Distribution || binding.Shard != "all" || binding.AllocationGeneration != 1 ||
			binding.MemberID != member.Member || binding.StoreID != storeID || identity.UserTable != table.Table || identity.UserPrimaryKey != table.PrimaryKey {
			return nil, group, errors.Join(errDevCluster, err)
		}
		if initialSchema {
			if err := sqldriver.ValidateReplicatedChildSchema(identity, table.CreateTable, nil, nil); err != nil {
				return nil, group, err
			}
		} else {
			// An evolving apply identity must still exist and obey its strict
			// retained grammar. Its live generation is owned by serve-node.
			raw, err := readDevFile(filepath.Join(member.GroupRoot, "apply-identity.vibejson"), 1<<20)
			if err != nil {
				return nil, group, err
			}
			var apply sqldriver.ReplicatedApplyIdentity
			if err := apply.UnmarshalJSON(raw); err != nil {
				return nil, group, err
			}
		}
	}
	return members, group, nil
}

func buildDevPhysicalTableProvision(table devTableProvision, members []devClusterMember, group raftmember.GroupKey, completed bool) ([]byte, error) {
	endpoints := make(map[distribution.EndpointID]string)
	name := distribution.DistributionName(table.Distribution)
	var route devPreparedRoute
	var err error
	if completed {
		route, err = plannedDevPhysicalTableRoute(endpoints, table, members, group)
	} else {
		route, err = inspectDevPreparedRoute(endpoints, table.artifactStem(), name, "all", table.Table, table.PrimaryKey, group, replication.Digest{}, true, members)
	}
	if err != nil {
		return nil, err
	}
	manifest, err := distribution.NewManifest(name, 1, []distribution.Shard{{ID: "all", AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: route.leaders, Epoch: 1}})
	if err != nil {
		return nil, err
	}
	rangeID, lineage, forwarding := deriveDevLogicalRangeAuthority(group, name, "all", route.digest)
	addition, err := gateway.NewSnapshotWithReplicatedTableMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: name, Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
		Placements:    []distribution.TablePlacement{{Table: table.Table, Distribution: name, Columns: []string{table.PrimaryKey}}}, Manifests: []*distribution.Manifest{manifest},
	}, endpoints, 1, nil, nil, []gateway.ReplicatedShardDescriptor{{Distribution: name, Shard: "all", Group: group, AllocationGeneration: 1,
		Command:             raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: route.schemaGeneration, RelationManifestDigest: route.digest, RoutingVersion: 1, RouteGeneration: 1},
		LogicalSchemaDigest: route.table.LogicalSchemaDigest, RangeIdentity: rangeID, LineageDigest: lineage, ForwardingRuleDigest: forwarding, Replicas: route.replicas,
	}}, []gateway.ReplicatedTableProfile{route.table}, []gateway.ReplicatedTableDeclaration{{Table: table.Table, CreateTable: table.CreateTable}})
	if err != nil {
		return nil, err
	}
	return gateway.AppendReplicatedTableProvision(nil, addition)
}

// A completed fragment preserves the initial schema proof even after an
// authorized ALTER changes the live catalog. The pure driver schema builders
// share the cold store's exact digest grammar without acquiring writer locks.
func plannedDevPhysicalTableRoute(endpoints map[distribution.EndpointID]string, table devTableProvision, members []devClusterMember, group raftmember.GroupKey) (devPreparedRoute, error) {
	statement, err := query.PrepareDML(table.CreateTable)
	if err != nil {
		return devPreparedRoute{}, err
	}
	defer statement.Release()
	definition, err := statement.LowerTable()
	if err != nil {
		return devPreparedRoute{}, err
	}
	result := devPreparedRoute{
		leaders: make([]distribution.EndpointID, len(members)), replicas: make([]gateway.ReplicatedReplicaDescriptor, len(members)),
		schemaGeneration: 1,
	}
	placement := sqldriver.ReplicatedPlacementProfile{
		Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: table.PrimaryKey,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
	}
	schema := sqldriver.InitialReplicatedRelationSchema{Table: table.Table, PrimaryKey: table.PrimaryKey, Schema: definition.Schema}
	for index, member := range members {
		node, nodeErr := decodeDev16(member.Node)
		store, storeErr := decodeDev16(member.Store)
		if nodeErr != nil || storeErr != nil {
			return devPreparedRoute{}, errDevCluster
		}
		binding := sqldriver.ReplicatedShardStoreBinding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation, TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			Distribution: table.Distribution, Shard: "all", AllocationGeneration: 1,
			ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID, MemberID: member.Member, StoreID: store,
			Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1},
		}
		digest, limits, err := sqldriver.InitialReplicatedRelationManifest(binding, placement, schema)
		if err != nil {
			return devPreparedRoute{}, err
		}
		logical, err := sqldriver.InitialReplicatedLogicalSchemaDigest(binding, placement, schema)
		if err != nil {
			return devPreparedRoute{}, err
		}
		profile := gateway.ReplicatedTableProfile{
			Table: table.Table, Relation: 1, PrimaryKey: table.PrimaryKey, SchemaGeneration: 1,
			LogicalSchemaDigest: logical, MaxKeyBytes: uint16(limits.MaxKeyBytes), MaxDocumentBytes: uint32(limits.MaxDocumentBytes),
		}
		if index == 0 {
			result.digest, result.table = digest, profile
		} else if result.digest != digest || result.table != profile {
			return devPreparedRoute{}, errDevCluster
		}
		prefix := fmt.Sprintf("%s-member-%d", table.artifactStem(), index+1)
		result.leaders[index] = distribution.EndpointID(prefix)
		result.replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: member.Member, Node: rafttransport.NodeID(node), StoreID: store, NodeIncarnation: 1,
			Endpoint: result.leaders[index], NativeEndpoint: distribution.EndpointID(prefix + "-native"), ControlEndpoint: distribution.EndpointID(prefix + "-control"),
		}
		endpoints[result.leaders[index]] = member.Peer
		endpoints[result.replicas[index].NativeEndpoint] = member.Native
		endpoints[result.replicas[index].ControlEndpoint] = member.Control
	}
	return result, nil
}

func reconcileDevPhysicalNodeGroup(member devClusterMember, appendMissing bool) error {
	groupRaw, err := readDevFile(filepath.Join(member.GroupRoot, "serve-rf3.vibejson"), 4<<20)
	if err != nil {
		return err
	}
	nodeRaw, err := readDevFile(member.ServeManifest, 4<<20)
	if err != nil {
		return err
	}
	var source, groupSource map[string]json.RawMessage
	if err := json.Unmarshal(nodeRaw, &source); err != nil {
		return err
	}
	if err := json.Unmarshal(groupRaw, &groupSource); err != nil {
		return err
	}
	var split map[string]json.RawMessage
	if err := json.Unmarshal(groupSource["split_control"], &split); err != nil || len(split["child_registry"]) == 0 {
		return errDevCluster
	}
	bundle := map[string]json.RawMessage{"child_registry": split["child_registry"]}
	for _, key := range []string{"wal", "sql", "route", "members"} {
		bundle[key] = groupSource[key]
	}
	bundleRaw, err := orderedDevManifestObject(bundle, []string{"wal", "sql", "route", "child_registry", "members"})
	if err != nil {
		return err
	}
	var groups []json.RawMessage
	if err := json.Unmarshal(source["groups"], &groups); err != nil || len(groups) == 0 {
		return errDevCluster
	}
	ordinal, err := devPhysicalGroupOrdinal(filepath.Dir(member.ServeManifest), member.GroupRoot)
	if err != nil {
		return err
	}
	for index, existing := range groups {
		if bytes.Equal(existing, bundleRaw) {
			if index != ordinal {
				return errDevCluster
			}
			return nil
		}
	}
	if !appendMissing || ordinal != len(groups) || len(groups) >= devPhysicalMaxGroups {
		return errDevCluster
	}
	if err := retainDevPhysicalManifest(member.ServeManifest, nodeRaw); err != nil {
		return err
	}
	groups = append(groups, bundleRaw)
	source["groups"], err = json.Marshal(groups)
	if err != nil {
		return err
	}
	order := []string{"node_log", "listeners", "tls", "authorization_policy", "replica_control", "split_control"}
	if len(source["read_authority"]) != 0 {
		order = append(order, "read_authority")
	}
	order = append(order, "gateway", "groups")
	raw, err := orderedDevManifestObject(source, order)
	if err != nil {
		return err
	}
	return replaceDevFile(member.ServeManifest, raw)
}

func retainDevPhysicalManifest(path string, raw []byte) error {
	directory := filepath.Join(filepath.Dir(path), "prepared-manifests")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	if err := writeDevFileOnce(filepath.Join(directory, hex.EncodeToString(digest[:])+".vibejson"), raw); err != nil {
		return err
	}
	return errors.Join(syncDevDir(directory), syncDevDir(filepath.Dir(path)))
}

// This file is a startup registration inventory, separate from immutable
// process configuration. Live CREATE registers its returned fragment through
// the gateway's authenticated catalog authority. Restart completes any prefix
// interrupted after preparation but before that registration.
func updateDevPhysicalGatewayCatalogs(cluster devClusterManifest, catalogs []string) error {
	if catalogs == nil {
		catalogs = []string{}
	}
	raw, err := vibejson.Marshal(&catalogs)
	if err != nil {
		return err
	}
	return replaceDevFile(filepath.Join(filepath.Dir(cluster.CatalogPath), "table-catalogs.vibejson"), raw)
}

func persistDevPhysicalClusterManifest(root string, cluster devClusterManifest) error {
	raw, err := vibejson.Marshal(&cluster)
	if err != nil {
		return err
	}
	return replaceDevFile(filepath.Join(root, "cluster.vibejson"), raw)
}
