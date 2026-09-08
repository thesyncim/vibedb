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
	"sort"
	"strconv"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3qualification"
	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const devPhysicalMaxGroups = 64

func ensureDevPhysicalTables(root, binary string, cluster *devClusterManifest, schemaPath, inventoryPath string, inventory devTableInventory) error {
	if cluster == nil || !validDevManifest(*cluster, root) || cluster.PhysicalNodes == 0 ||
		len(inventory.Tables) > int(cluster.PhysicalNodes)*devPhysicalMaxGroups/devClusterRF3 {
		return errDevCluster
	}
	if cluster.ReadAuthority != nil && !rf3qualification.ReadAuthorityEnabled {
		return fmt.Errorf("%w: enabled read authority requires the explicitly tagged laboratory build %q", errDevCluster, rf3qualification.ReadAuthorityLabBuildTag)
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
	for tableIndex, table := range inventory.Tables {
		if !validDevProvisionBundleFormat(table.ProvisionBundleFormat) {
			return errDevCluster
		}
		path := filepath.Join(root, table.artifactStem()+"-catalog.vibejson")
		bundlePath := filepath.Join(root, table.artifactStem()+devTableProvisionBundleSuffix)
		fragment, fragmentErr := readDevFile(path, 4<<20)
		if fragmentErr != nil && !errors.Is(fragmentErr, os.ErrNotExist) {
			return fragmentErr
		}
		bundle, bundleErr := readDevFile(bundlePath, 4<<20)
		if bundleErr != nil && !errors.Is(bundleErr, os.ErrNotExist) {
			return bundleErr
		}
		var bundleCatalog []byte
		if bundleErr == nil {
			catalogRaw, _, openErr := gateway.OpenReplicatedTableProvisionBundle(bundle)
			if openErr != nil {
				return openErr
			}
			bundleCatalog = catalogRaw
			if fragmentErr == nil && !bytes.Equal(fragment, catalogRaw) {
				return fmt.Errorf("%w: table %q bundle does not match its immutable catalog fragment", errDevCluster, table.Table)
			}
		}
		// The immutable fragment is published only after all three stores pass
		// cold validation. A completed table may already have live writers or a
		// newer schema generation; validate its original proof without reopening
		// or freezing those live SQL/apply identities.
		completed := fragmentErr == nil || bundleErr == nil
		members, group, err := prepareDevPhysicalTable(root, binary, *cluster, table, completed)
		if err != nil {
			return err
		}
		if bundleErr == nil {
			expected, expectedErr := buildDevPhysicalTableProvision(table, members, group, true)
			if expectedErr != nil || !bytes.Equal(expected, bundleCatalog) {
				return errors.Join(errDevCluster, expectedErr)
			}
			if err := validateDevPhysicalTableSplitSource(root, bundle, table, members, group); err != nil {
				return err
			}
			if fragmentErr != nil {
				if err := writeDevFileOnce(path, expected); err != nil {
					return err
				}
				fragment, fragmentErr = expected, nil
			}
			if table.ProvisionBundleFormat != gateway.ReplicatedTableProvisionBundleFormat {
				table.ProvisionBundleFormat = gateway.ReplicatedTableProvisionBundleFormat
				inventory.Tables[tableIndex] = table
				raw, marshalErr := vibejson.Marshal(&inventory)
				if marshalErr != nil {
					return marshalErr
				}
				if err := replaceDevFile(inventoryPath, raw); err != nil {
					return err
				}
			}
		} else {
			provision, err := buildDevPhysicalTableProvision(table, members, group, completed)
			if err != nil {
				return err
			}
			if fragmentErr == nil && !bytes.Equal(fragment, provision) {
				return fmt.Errorf("%w: retained table fragment differs from prepared identity", errDevCluster)
			}
			if err := writeDevFileOnce(path, provision); err != nil {
				return err
			}
			fragment = provision
			// New plans require a complete bundle. Legacy plans may be upgraded
			// only when the exact prepared identities produce a valid proof.
			candidate := table
			candidate.ProvisionBundleFormat = gateway.ReplicatedTableProvisionBundleFormat
			sourceRaw, sourceErr := buildDevPhysicalTableSplitSource(root, candidate, members, group, completed)
			if sourceErr == nil {
				bundle, sourceErr = gateway.AppendReplicatedTableProvisionBundle(nil, fragment, sourceRaw)
				if sourceErr != nil {
					return sourceErr
				}
				if err := writeDevFileOnce(bundlePath, bundle); err != nil {
					return err
				}
				if table.ProvisionBundleFormat != gateway.ReplicatedTableProvisionBundleFormat {
					table.ProvisionBundleFormat = gateway.ReplicatedTableProvisionBundleFormat
					inventory.Tables[tableIndex] = table
					raw, marshalErr := vibejson.Marshal(&inventory)
					if marshalErr != nil {
						return marshalErr
					}
					if err := replaceDevFile(inventoryPath, raw); err != nil {
						return err
					}
				}
				bundleErr = nil
			}
			if sourceErr != nil && table.ProvisionBundleFormat == gateway.ReplicatedTableProvisionBundleFormat {
				return sourceErr
			}
		}
		if table.ProvisionBundleFormat == gateway.ReplicatedTableProvisionBundleFormat || bundleErr == nil {
			cluster.additionalCatalogs = append(cluster.additionalCatalogs, bundlePath)
		} else {
			cluster.additionalCatalogs = append(cluster.additionalCatalogs, path)
		}
	}
	return updateDevPhysicalGatewayCatalogs(*cluster, cluster.additionalCatalogs)
}

func planDevPhysicalTable(cluster devClusterManifest, name, primary, ddl string, ordinal uint64) (devTableProvision, error) {
	table := devTableProvision{Table: name, PrimaryKey: primary, CreateTable: ddl, PlacementOrdinal: ordinal,
		ProvisionBundleFormat: gateway.ReplicatedTableProvisionBundleFormat}
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

// buildDevPhysicalTableSplitSource retains the exact prepared SQL identity of
// one independently provisioned table. It deliberately reads the durable
// preparation records and every member-local identity instead of rebuilding a
// source from the mutable live route.
func buildDevPhysicalTableSplitSource(root string, table devTableProvision, members []devClusterMember, group raftmember.GroupKey, completed bool) ([]byte, error) {
	if len(members) != devClusterRF3 || group == (raftmember.GroupKey{}) || table.ProvisionBundleFormat != gateway.ReplicatedTableProvisionBundleFormat {
		return nil, errDevCluster
	}
	endpoints := make(map[distribution.EndpointID]string)
	var route devPreparedRoute
	var err error
	physical := members[0].GroupRoot != ""
	if completed && physical {
		route, err = plannedDevPhysicalTableRoute(endpoints, table, members, group)
	} else {
		role := "data"
		if physical {
			role = table.artifactStem()
		}
		route, err = inspectDevPreparedRoute(endpoints, role, distribution.DistributionName(table.distribution()), "all", table.Table, table.PrimaryKey, group, replication.Digest{}, true, members)
	}
	if err != nil {
		return nil, err
	}
	placement := sqldriver.ReplicatedPlacementProfile{
		Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: table.PrimaryKey,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
	}
	var source devReplicaSplitSource
	var logical replication.Digest
	var machine [sha256.Size]byte
	var apply devPrepareApply
	for index, member := range members {
		preparePath := filepath.Join(root, fmt.Sprintf("prepare-%s-member-%d.vibejson", table.artifactStem(), index+1))
		raw, readErr := readDevFile(preparePath, 1<<20)
		if readErr != nil {
			return nil, readErr
		}
		var prepare devPrepareManifest
		if err := vibejson.Unmarshal(raw, &prepare); err != nil ||
			prepare.Root != devMemberRoot(member) || prepare.MemberID != member.Member || prepare.StoreID != member.Store ||
			prepare.Table != table.Table || prepare.CreateTable != table.CreateTable ||
			prepare.Distribution != table.distribution() || prepare.Shard != "all" || prepare.AllocationGeneration != 1 ||
			prepare.GroupID != table.GroupID || prepare.ShardIncarnation != table.ShardIncarnation ||
			prepare.Apply.ShardKey != table.PrimaryKey || len(prepare.Members) != devClusterRF3 {
			return nil, errors.Join(errDevCluster, err)
		}
		clusterID, clusterErr := decodeDev16(prepare.ClusterID)
		clusterIncarnation, incarnationErr := decodeDev16(prepare.ClusterIncarnation)
		shardIncarnation, shardErr := decodeDev16(prepare.ShardIncarnation)
		groupID, groupErr := decodeDev16(prepare.GroupID)
		storeID, storeErr := decodeDev16(prepare.StoreID)
		if clusterErr != nil || incarnationErr != nil || shardErr != nil || groupErr != nil || storeErr != nil ||
			clusterID != group.ClusterID || clusterIncarnation != group.ClusterIncarnation ||
			shardIncarnation != group.ShardIncarnation || groupID != group.GroupID ||
			prepare.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch {
			return nil, errors.Join(errDevCluster, clusterErr, incarnationErr, shardErr, groupErr, storeErr)
		}
		if index != 0 && prepare.Apply != apply {
			return nil, errDevCluster
		}
		if index == 0 {
			apply = prepare.Apply
		}
		for peerIndex, peer := range members {
			preparedPeer := prepare.Members[peerIndex]
			if preparedPeer.MemberID != peer.Member || preparedPeer.NodeID != peer.Node || preparedPeer.PeerAddress != peer.Peer {
				return nil, errDevCluster
			}
		}
		identityRaw, err := readDevFile(filepath.Join(devMemberRoot(member), "sql-identity.vibejson"), 1<<20)
		if err != nil {
			return nil, err
		}
		var identity sqldriver.ReplicatedShardStoreIdentity
		if err := identity.UnmarshalJSON(identityRaw); err != nil || identity.Binding != (sqldriver.ReplicatedShardStoreBinding{
			ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			Distribution: table.distribution(), Shard: "all", AllocationGeneration: 1,
			ShardIncarnation: shardIncarnation, GroupID: groupID, MemberID: member.Member, StoreID: storeID,
			Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: prepare.Authority.ActivePolicyGeneration,
				ProtectionEpoch: prepare.Authority.ProtectionEpoch, OwnershipEpoch: prepare.Authority.OwnershipEpoch,
				SchemaGeneration: prepare.Authority.SchemaGeneration, RoutingVersion: prepare.Authority.RoutingVersion,
				RouteGeneration: prepare.Authority.RouteGeneration},
		}) || identity.UserTable != table.Table || identity.UserPrimaryKey != table.PrimaryKey || identity.RelationCount != 1 {
			return nil, errors.Join(errDevCluster, err)
		}
		if err := sqldriver.ValidateReplicatedChildSchema(identity, table.CreateTable, nil, nil); err != nil {
			return nil, err
		}
		portable, err := sqldriver.ReplicatedRelationManifestDigest(identity)
		if err != nil {
			return nil, err
		}
		if identity.Relations[0].LocalIndexDigest != ([sha256.Size]byte{}) {
			// The initial dev-table grammar has no retained index statement list.
			// Refuse to manufacture an index proof from a digest alone.
			return nil, errDevCluster
		}
		actualMachine, err := sqldriver.ReplicatedSchemaManifest(identity, placement, nil)
		if err != nil || actualMachine != route.digest {
			return nil, errors.Join(errDevCluster, err)
		}
		if index == 0 {
			logical, machine = replication.Digest(portable), actualMachine
			source.ClusterID, source.ClusterIncarnation, source.TopologyRecoveryEpoch = group.ClusterID, group.ClusterIncarnation, group.TopologyRecoveryEpoch
			source.ShardIncarnation, source.GroupID = group.ShardIncarnation, group.GroupID
			source.SchemaGeneration, source.Table, source.SQL = identity.Binding.Authority.SchemaGeneration, table.Table, identity.Clone()
		} else if replication.Digest(portable) != logical || actualMachine != machine || identity.UserLimits != source.SQL.UserLimits {
			return nil, errDevCluster
		}
	}
	if logical != route.table.LogicalSchemaDigest || machine != route.digest || source.SQL.Binding.Authority.SchemaGeneration == 0 {
		return nil, errDevCluster
	}
	source.RelationManifestDigest = machine
	source.Placement = devReplicaSplitPlacement{Format: placement.Format, ShardKey: placement.ShardKey,
		TupleVersion: uint16(placement.TupleVersion), MapperVersion: uint16(placement.MapperVersion),
		RangeStart: placement.Range.Start, RangeEnd: placement.Range.End.Point, RangeEndMax: placement.Range.End.Max}
	source.Template = devReplicaSplitTemplate{MaxSessions: apply.MaxSessions, RetryWindow: apply.RetryWindow,
		TxnLimits: durable.TxnLimits{MaxCollections: apply.MaxCollections, MaxDocuments: apply.MaxDocuments, MaxBytes: apply.MaxBytes},
		Format:    placement.Format, ShardKey: placement.ShardKey, TupleVersion: uint16(placement.TupleVersion), MapperVersion: uint16(placement.MapperVersion),
		MaxBatchDocuments: source.SQL.UserLimits.MaxBatchDocuments, MaxBatchBytes: source.SQL.UserLimits.MaxBatchBytes}
	for _, member := range members {
		source.Replicas = append(source.Replicas, devReplicaSplitSourceReplica{Node: member.Node,
			ChildRoot: filepath.Join(devMemberRoot(member), "split-children")})
	}
	sort.Slice(source.Replicas, func(left, right int) bool { return source.Replicas[left].Node < source.Replicas[right].Node })
	return vibejson.Marshal(&source)
}

func validateDevPhysicalTableSplitSource(root string, bundleRaw []byte, table devTableProvision, members []devClusterMember, group raftmember.GroupKey) error {
	catalogRaw, sourceRaw, err := gateway.OpenReplicatedTableProvisionBundle(bundleRaw)
	if err != nil {
		return err
	}
	addition, err := gateway.OpenReplicatedTableProvision(catalogRaw)
	if err != nil {
		return err
	}
	var source devReplicaSplitSource
	if err := vibejson.Unmarshal(sourceRaw, &source); err != nil {
		return errors.Join(errDevCluster, err)
	}
	canonical, err := vibejson.Marshal(&source)
	if err != nil || !bytes.Equal(canonical, sourceRaw) || len(source.Replicas) != devClusterRF3 ||
		source.ClusterID != group.ClusterID || source.ClusterIncarnation != group.ClusterIncarnation ||
		source.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch || source.ShardIncarnation != group.ShardIncarnation ||
		source.GroupID != group.GroupID || source.SchemaGeneration == 0 || source.Table != table.Table ||
		source.SQL.Binding.ClusterID != group.ClusterID || source.SQL.Binding.ClusterIncarnation != group.ClusterIncarnation ||
		source.SQL.Binding.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch || source.SQL.Binding.Distribution != table.distribution() ||
		source.SQL.Binding.Shard != "all" || source.SQL.Binding.AllocationGeneration != 1 || source.SQL.Binding.ShardIncarnation != group.ShardIncarnation ||
		source.SQL.Binding.GroupID != group.GroupID || source.SQL.UserTable != table.Table || source.SQL.UserPrimaryKey != table.PrimaryKey ||
		source.SQL.RelationCount != 1 || source.SQL.RelationSchemaGeneration != source.SchemaGeneration ||
		source.SQL.Binding.Authority.SchemaGeneration != source.SchemaGeneration || len(source.LocalIndexes) != 0 {
		return errDevCluster
	}
	placement := sqldriver.ReplicatedPlacementProfile{Format: source.Placement.Format, ShardKey: source.Placement.ShardKey,
		TupleVersion: distribution.TupleVersion(source.Placement.TupleVersion), MapperVersion: distribution.MapperVersion(source.Placement.MapperVersion),
		Range: distribution.KeyRange{Start: source.Placement.RangeStart, End: distribution.KeyspaceEnd{Point: source.Placement.RangeEnd, Max: source.Placement.RangeEndMax}}}
	if placement.Format != sqldriver.ReplicatedPlacementProfileFormat ||
		placement.TupleVersion != distribution.CurrentTupleVersion || placement.MapperVersion != distribution.NativeMapperVersion ||
		placement.ShardKey != table.PrimaryKey || placement.Range.Start != ([8]byte{}) || !placement.Range.End.Max {
		return errDevCluster
	}
	var apply devPrepareApply
	var preparedAuthority devPrepareAuthority
	for index, member := range members {
		preparePath := filepath.Join(root, fmt.Sprintf("prepare-%s-member-%d.vibejson", table.artifactStem(), index+1))
		prepareRaw, readErr := readDevFile(preparePath, 1<<20)
		if readErr != nil {
			return readErr
		}
		var prepare devPrepareManifest
		if err := vibejson.Unmarshal(prepareRaw, &prepare); err != nil {
			return errors.Join(errDevCluster, err)
		}
		canonicalPrepare, marshalErr := vibejson.Marshal(&prepare)
		if marshalErr != nil || !bytes.Equal(canonicalPrepare, prepareRaw) ||
			prepare.Root != devMemberRoot(member) || prepare.MemberID != member.Member || prepare.StoreID != member.Store ||
			prepare.Table != table.Table || prepare.CreateTable != table.CreateTable || prepare.Distribution != table.distribution() ||
			prepare.Shard != "all" || prepare.AllocationGeneration != 1 || prepare.GroupID != table.GroupID ||
			prepare.ShardIncarnation != table.ShardIncarnation || prepare.Apply.ShardKey != table.PrimaryKey ||
			len(prepare.Members) != devClusterRF3 {
			return errors.Join(errDevCluster, marshalErr)
		}
		if index == 0 {
			apply = prepare.Apply
			preparedAuthority = prepare.Authority
		} else if prepare.Apply != apply {
			return errDevCluster
		}
	}
	if source.Template.MaxSessions != apply.MaxSessions || source.Template.RetryWindow != apply.RetryWindow ||
		source.Template.TxnLimits.MaxCollections != apply.MaxCollections || source.Template.TxnLimits.MaxDocuments != apply.MaxDocuments ||
		source.Template.TxnLimits.MaxBytes != apply.MaxBytes || source.Template.ShardKey != apply.ShardKey ||
		source.Template.Format != source.Placement.Format || source.Template.TupleVersion != source.Placement.TupleVersion ||
		source.Template.MapperVersion != source.Placement.MapperVersion ||
		source.Template.MaxBatchDocuments != source.SQL.UserLimits.MaxBatchDocuments ||
		source.Template.MaxBatchBytes != source.SQL.UserLimits.MaxBatchBytes ||
		source.SQL.Binding.Authority != (sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: preparedAuthority.ActivePolicyGeneration,
			ProtectionEpoch:        preparedAuthority.ProtectionEpoch,
			OwnershipEpoch:         preparedAuthority.OwnershipEpoch,
			SchemaGeneration:       preparedAuthority.SchemaGeneration,
			RoutingVersion:         preparedAuthority.RoutingVersion,
			RouteGeneration:        preparedAuthority.RouteGeneration,
		}) {
		return errDevCluster
	}
	logical, err := sqldriver.ReplicatedRelationManifestDigest(source.SQL)
	if err != nil {
		return err
	}
	machine, err := sqldriver.ReplicatedSchemaManifest(source.SQL, placement, nil)
	if err != nil || machine != source.RelationManifestDigest || logical == ([sha256.Size]byte{}) {
		return errors.Join(errDevCluster, err)
	}
	descriptors, profiles := addition.ReplicatedShardDescriptors(), addition.ReplicatedTableProfiles()
	if len(descriptors) != 1 || len(profiles) != 1 || descriptors[0].Group != group ||
		descriptors[0].Distribution != distribution.DistributionName(table.distribution()) || descriptors[0].Shard != "all" ||
		descriptors[0].AllocationGeneration != 1 ||
		descriptors[0].Command.RelationManifestDigest != machine || descriptors[0].LogicalSchemaDigest != replication.Digest(logical) ||
		descriptors[0].Command.ActivePolicyGeneration != source.SQL.Binding.Authority.ActivePolicyGeneration ||
		descriptors[0].Command.ProtectionEpoch != source.SQL.Binding.Authority.ProtectionEpoch ||
		descriptors[0].Command.OwnershipEpoch != source.SQL.Binding.Authority.OwnershipEpoch ||
		descriptors[0].Command.SchemaGeneration != source.SQL.Binding.Authority.SchemaGeneration ||
		descriptors[0].Command.RoutingVersion != source.SQL.Binding.Authority.RoutingVersion ||
		descriptors[0].Command.RouteGeneration != source.SQL.Binding.Authority.RouteGeneration ||
		profiles[0].Table != table.Table || profiles[0].PrimaryKey != table.PrimaryKey || profiles[0].SchemaGeneration != source.SchemaGeneration ||
		profiles[0].LogicalSchemaDigest != replication.Digest(logical) {
		return errDevCluster
	}
	byNode := make(map[string]string, len(members))
	boundMember := false
	for _, member := range members {
		byNode[member.Node] = filepath.Join(devMemberRoot(member), "split-children")
		storeID, decodeErr := decodeDev16(member.Store)
		if decodeErr != nil {
			return decodeErr
		}
		if source.SQL.Binding.MemberID == member.Member && source.SQL.Binding.StoreID == storeID {
			boundMember = true
		}
	}
	if !boundMember {
		return errDevCluster
	}
	for _, replica := range source.Replicas {
		root, found := byNode[replica.Node]
		if !found || replica.ChildRoot != root {
			return errDevCluster
		}
		delete(byNode, replica.Node)
	}
	if len(byNode) != 0 {
		return errDevCluster
	}
	return nil
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
	readAuthority := bytes.TrimSpace(source["read_authority"])
	if len(readAuthority) != 0 && !bytes.Equal(readAuthority, []byte("null")) && !rf3qualification.ReadAuthorityEnabled {
		return fmt.Errorf("%w: enabled read authority requires the explicitly tagged laboratory build %q", errDevCluster, rf3qualification.ReadAuthorityLabBuildTag)
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
