package main

import (
	"bytes"
	"encoding/hex"
	"math"
	"path/filepath"
	"strings"

	"github.com/thesyncim/vibejson"
)

// rf3ManifestGateway is the persisted, serializable part of one embedded
// frontend. Runtime interfaces, listeners and transports are deliberately
// excluded: serve-node constructs those only after the node owners have
// started. The gateway has its own TLS principal and durable identity paths;
// it never reuses the node log or storage TLS identity.
type rf3ManifestGateway struct {
	CatalogPath                 string                   `json:"catalog_path"`
	CatalogRouteSeedPath        string                   `json:"catalog_route_seed_path"`
	CatalogBootstrapIfMissing   bool                     `json:"catalog_bootstrap_if_missing"`
	CatalogRelation             uint64                   `json:"catalog_relation"`
	CatalogAttempts             uint64                   `json:"catalog_attempts"`
	CatalogAttemptTimeoutMillis uint64                   `json:"catalog_attempt_timeout_millis"`
	CatalogSessionLeaseMillis   uint64                   `json:"catalog_session_lease_millis"`
	CatalogSessionJournal       string                   `json:"catalog_session_journal"`
	CatalogClientID             string                   `json:"catalog_client_id"`
	CatalogRetryHome            string                   `json:"catalog_retry_home"`
	DurableAckKeyPath           string                   `json:"durable_ack_key"`
	ListenAddress               string                   `json:"listen"`
	PGListenAddress             string                   `json:"pg_listen"`
	PGDDLSocket                 string                   `json:"pg_ddl_socket"`
	TLS                         rf3ManifestTLS           `json:"tls"`
	TLSHandshakeTimeoutMillis   uint64                   `json:"tls_handshake_timeout_millis"`
	AuthorizationPolicy         string                   `json:"authorization_policy"`
	ShardPeers                  []rf3ManifestGatewayPeer `json:"shard_peers"`
	MaxConnections              uint64                   `json:"max_connections"`
	MaxHandshakes               uint64                   `json:"max_handshakes"`
	MaxShardConnections         uint64                   `json:"max_shard_connections"`
	MaxShardHandshakes          uint64                   `json:"max_shard_handshakes"`
	MaxNativeReadConcurrency    uint64                   `json:"max_native_read_concurrency"`
	MaxNativeReadBytes          uint64                   `json:"max_native_read_bytes"`
	MaxNativeScatterConcurrency uint64                   `json:"max_native_scatter_concurrency"`
	TableCatalogs               []string                 `json:"table_catalogs"`
	TableCatalogsPath           string                   `json:"table_catalogs_path"`
	HotShardCapacityPath        string                   `json:"hot_shard_capacity"`
	HotShardIntervalMillis      uint64                   `json:"hot_shard_interval_millis"`
	ReplicaControlManifestPath  string                   `json:"replica_control_manifest"`
	ControlParticipantOnly      bool                     `json:"control_participant_only"`
	DDLOwnerAddress             string                   `json:"ddl_owner_address"`
	DDLOwnerNode                string                   `json:"ddl_owner_node"`
	BackupRepositoryPath        string                   `json:"backup_repository"`
	BackupMaxBackups            uint64                   `json:"backup_max_backups"`
	BackupMaxArtifacts          uint64                   `json:"backup_max_artifacts"`
	BackupMaxArtifactBytes      uint64                   `json:"backup_max_artifact_bytes"`
	BackupMaxDiskBytes          uint64                   `json:"backup_max_disk_bytes"`
	ControllerIntervalMillis    uint64                   `json:"controller_interval_millis"`
	SchemaRolloutPlan           string                   `json:"schema_rollout_plan"`
	SchemaRolloutOnce           bool                     `json:"schema_rollout_once"`
}

type rf3ManifestGatewayPeer struct {
	Address string `json:"address"`
	NodeID  string `json:"node_id"`
}

func parseRF3ManifestGateway(node vibejson.Node) (*rf3ManifestGateway, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return nil, errInvalidRF3Manifest
	}
	result := new(rf3ManifestGateway)
	value, err := nextRF3Field(&fields, `"catalog_path"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogPath, err = rf3ManifestAbsolutePath(value, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"catalog_route_seed_path"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogRouteSeedPath, err = rf3ManifestAbsolutePath(value, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"catalog_bootstrap_if_missing"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogBootstrapIfMissing, ok = value.Bool(); !ok {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"catalog_relation"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogRelation, err = rf3ManifestPositiveUint64(value); err != nil || result.CatalogRelation > math.MaxUint16 {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"catalog_attempts"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogAttempts, err = rf3ManifestPositiveUint64(value); err != nil || result.CatalogAttempts > math.MaxInt {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"catalog_attempt_timeout_millis"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogAttemptTimeoutMillis, err = rf3ManifestPositiveUint64(value); err != nil || result.CatalogAttemptTimeoutMillis > uint64(math.MaxInt64/1_000_000) {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"catalog_session_lease_millis"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogSessionLeaseMillis, err = rf3ManifestPositiveUint64(value); err != nil || result.CatalogSessionLeaseMillis > uint64(math.MaxInt64/1_000_000) {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"catalog_session_journal"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogSessionJournal, err = rf3ManifestAbsolutePath(value, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"catalog_client_id"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogClientID, err = rf3ManifestHexString(value, 16, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"catalog_retry_home"`)
	if err != nil {
		return nil, err
	}
	if result.CatalogRetryHome, err = rf3ManifestHexString(value, 8, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"durable_ack_key"`)
	if err != nil {
		return nil, err
	}
	if result.DurableAckKeyPath, err = rf3ManifestAbsolutePath(value, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"listen"`)
	if err != nil {
		return nil, err
	}
	if result.ListenAddress, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"pg_listen"`)
	if err != nil {
		return nil, err
	}
	if result.PGListenAddress, err = rf3ManifestOptionalString(value, maxRF3ManifestStringBytes); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"pg_ddl_socket"`)
	if err != nil {
		return nil, err
	}
	if result.PGDDLSocket, err = rf3ManifestOptionalAbsolutePath(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"tls"`)
	if err != nil {
		return nil, err
	}
	if result.TLS, err = parseRF3ManifestTLS(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"tls_handshake_timeout_millis"`)
	if err != nil {
		return nil, err
	}
	if result.TLSHandshakeTimeoutMillis, err = rf3ManifestOptionalUint64(value); err != nil || result.TLSHandshakeTimeoutMillis > uint64(math.MaxInt64/1_000_000) {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"authorization_policy"`)
	if err != nil {
		return nil, err
	}
	if result.AuthorizationPolicy, err = rf3ManifestAbsolutePath(value, false); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"shard_peers"`)
	if err != nil {
		return nil, err
	}
	if result.ShardPeers, err = parseRF3ManifestGatewayPeers(value); err != nil {
		return nil, err
	}
	unsigned := []struct {
		name string
		dest *uint64
	}{
		{`"max_connections"`, &result.MaxConnections},
		{`"max_handshakes"`, &result.MaxHandshakes},
		{`"max_shard_connections"`, &result.MaxShardConnections},
		{`"max_shard_handshakes"`, &result.MaxShardHandshakes},
		{`"max_native_read_concurrency"`, &result.MaxNativeReadConcurrency},
	}
	maxInt := uint64(^uint(0) >> 1)
	for _, item := range unsigned {
		value, err = nextRF3Field(&fields, item.name)
		if err != nil {
			return nil, err
		}
		if *item.dest, err = rf3ManifestOptionalUint64(value); err != nil || *item.dest > maxInt {
			return nil, errInvalidRF3Manifest
		}
	}
	value, err = nextRF3Field(&fields, `"max_native_read_bytes"`)
	if err != nil {
		return nil, err
	}
	if result.MaxNativeReadBytes, err = rf3ManifestOptionalUint64(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"max_native_scatter_concurrency"`)
	if err != nil {
		return nil, err
	}
	if result.MaxNativeScatterConcurrency, err = rf3ManifestOptionalUint64(value); err != nil || result.MaxNativeScatterConcurrency > maxInt {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"table_catalogs"`)
	if err != nil {
		return nil, err
	}
	if result.TableCatalogs, err = parseRF3ManifestGatewayPaths(value, maxRF3ManifestGroups); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"table_catalogs_path"`)
	if err != nil {
		return nil, err
	}
	if result.TableCatalogsPath, err = rf3ManifestOptionalAbsolutePath(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"hot_shard_capacity"`)
	if err != nil {
		return nil, err
	}
	if result.HotShardCapacityPath, err = rf3ManifestOptionalAbsolutePath(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"hot_shard_interval_millis"`)
	if err != nil {
		return nil, err
	}
	if result.HotShardIntervalMillis, err = rf3ManifestOptionalUint64(value); err != nil {
		return nil, err
	}
	if result.HotShardIntervalMillis > uint64(math.MaxInt64/1_000_000) {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"replica_control_manifest"`)
	if err != nil {
		return nil, err
	}
	if result.ReplicaControlManifestPath, err = rf3ManifestOptionalAbsolutePath(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"control_participant_only"`)
	if err != nil {
		return nil, err
	}
	if result.ControlParticipantOnly, ok = value.Bool(); !ok {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"ddl_owner_address"`)
	if err != nil {
		return nil, err
	}
	if result.DDLOwnerAddress, err = rf3ManifestOptionalString(value, maxRF3ManifestStringBytes); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"ddl_owner_node"`)
	if err != nil {
		return nil, err
	}
	if result.DDLOwnerNode, err = rf3ManifestOptionalString(value, 32); err != nil {
		return nil, err
	}
	if result.DDLOwnerAddress != "" || result.DDLOwnerNode != "" {
		if !result.ControlParticipantOnly || result.PGListenAddress == "" ||
			validateRF3Address(result.DDLOwnerAddress, false) != nil {
			return nil, errInvalidRF3Manifest
		}
		if _, valid := rf3GatewayNodeID(result.DDLOwnerNode); !valid {
			return nil, errInvalidRF3Manifest
		}
	} else if result.ControlParticipantOnly && result.PGListenAddress != "" {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"backup_repository"`)
	if err != nil {
		return nil, err
	}
	if result.BackupRepositoryPath, err = rf3ManifestOptionalAbsolutePath(value); err != nil {
		return nil, err
	}
	unsigned = []struct {
		name string
		dest *uint64
	}{
		{`"backup_max_backups"`, &result.BackupMaxBackups},
		{`"backup_max_artifacts"`, &result.BackupMaxArtifacts},
		{`"backup_max_artifact_bytes"`, &result.BackupMaxArtifactBytes},
		{`"backup_max_disk_bytes"`, &result.BackupMaxDiskBytes},
		{`"controller_interval_millis"`, &result.ControllerIntervalMillis},
	}
	for _, item := range unsigned {
		value, err = nextRF3Field(&fields, item.name)
		if err != nil {
			return nil, err
		}
		if *item.dest, err = rf3ManifestOptionalUint64(value); err != nil || *item.dest > maxInt {
			return nil, errInvalidRF3Manifest
		}
	}
	if result.ControllerIntervalMillis > uint64(math.MaxInt64/1_000_000) {
		return nil, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"schema_rollout_plan"`)
	if err != nil {
		return nil, err
	}
	if result.SchemaRolloutPlan, err = rf3ManifestOptionalAbsolutePath(value); err != nil {
		return nil, err
	}
	value, err = nextRF3Field(&fields, `"schema_rollout_once"`)
	if err != nil {
		return nil, err
	}
	if result.SchemaRolloutOnce, ok = value.Bool(); !ok {
		return nil, errInvalidRF3Manifest
	}
	if _, _, extra := fields.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	if result.CatalogPath == result.CatalogRouteSeedPath ||
		result.CatalogPath == result.CatalogSessionJournal ||
		result.CatalogPath == result.DurableAckKeyPath ||
		result.CatalogRouteSeedPath == result.CatalogSessionJournal ||
		result.CatalogRouteSeedPath == result.DurableAckKeyPath ||
		result.CatalogSessionJournal == result.DurableAckKeyPath {
		return nil, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestGatewayPeers(node vibejson.Node) ([]rf3ManifestGatewayPeer, error) {
	count, ok := node.ArrayLen()
	if !ok || count > maxRF3ManifestGroups*rf3ManifestMembers {
		return nil, errInvalidRF3Manifest
	}
	iter, _ := node.ArrayIter()
	result := make([]rf3ManifestGatewayPeer, 0, count)
	nodes := make(map[string]struct{}, count)
	addresses := make(map[string]struct{}, count)
	for range count {
		value, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		fields, ok := value.ObjectIter()
		if !ok {
			return nil, errInvalidRF3Manifest
		}
		addressNode, err := nextRF3Field(&fields, `"address"`)
		if err != nil {
			return nil, err
		}
		address, err := rf3ManifestString(addressNode, maxRF3ManifestStringBytes)
		if err != nil {
			return nil, err
		}
		if validateRF3Address(address, false) != nil {
			return nil, errInvalidRF3Manifest
		}
		nodeNode, err := nextRF3Field(&fields, `"node_id"`)
		if err != nil {
			return nil, err
		}
		nodeID, err := rf3ManifestNodeID(nodeNode)
		if err != nil {
			return nil, err
		}
		nodeText := hex.EncodeToString(nodeID[:])
		if _, duplicate := nodes[nodeText]; duplicate {
			return nil, errInvalidRF3Manifest
		}
		if _, duplicate := addresses[address]; duplicate {
			return nil, errInvalidRF3Manifest
		}
		nodes[nodeText], addresses[address] = struct{}{}, struct{}{}
		if _, _, extra := fields.Next(); extra {
			return nil, errInvalidRF3Manifest
		}
		result = append(result, rf3ManifestGatewayPeer{Address: address, NodeID: nodeText})
	}
	if _, extra := iter.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestGatewayPaths(node vibejson.Node, maximum int) ([]string, error) {
	count, ok := node.ArrayLen()
	if !ok || count > maximum {
		return nil, errInvalidRF3Manifest
	}
	iter, _ := node.ArrayIter()
	result := make([]string, 0, count)
	for range count {
		value, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		path, err := rf3ManifestAbsolutePath(value, false)
		if err != nil {
			return nil, err
		}
		result = append(result, path)
	}
	if _, extra := iter.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	return result, nil
}

func rf3ManifestAbsolutePath(node vibejson.Node, optional bool) (string, error) {
	value, err := rf3ManifestString(node, maxRF3ManifestStringBytes)
	if err != nil {
		if optional {
			if raw, ok := node.StringBytes(); ok && len(raw) == 0 {
				return "", nil
			}
		}
		return "", errInvalidRF3Manifest
	}
	if !filepath.IsAbs(value) || filepath.Clean(value) != value || value == string(filepath.Separator) {
		return "", errInvalidRF3Manifest
	}
	return value, nil
}

func rf3ManifestOptionalAbsolutePath(node vibejson.Node) (string, error) {
	raw, ok := node.StringBytes()
	if !ok || len(raw) == 0 {
		if ok && len(raw) == 0 {
			return "", nil
		}
		return "", errInvalidRF3Manifest
	}
	return rf3ManifestAbsolutePath(node, false)
}

func rf3ManifestOptionalUint64(node vibejson.Node) (uint64, error) {
	value, ok := node.Uint64()
	if !ok {
		return 0, errInvalidRF3Manifest
	}
	return value, nil
}

func rf3ManifestHexString(node vibejson.Node, bytesCount int, allowZero bool) (string, error) {
	value, err := rf3ManifestString(node, bytesCount*2)
	if err != nil || len(value) != bytesCount*2 {
		return "", errInvalidRF3Manifest
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || (!allowZero && bytes.Equal(decoded, make([]byte, bytesCount))) {
		return "", errInvalidRF3Manifest
	}
	if strings.ToLower(value) != value {
		return "", errInvalidRF3Manifest
	}
	return value, nil
}
