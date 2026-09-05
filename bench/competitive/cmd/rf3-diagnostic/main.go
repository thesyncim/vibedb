// Command rf3-diagnostic collects a bounded, read-only per-group Raft cut.
//
// It is a benchmark qualification helper. It speaks only the authenticated
// shard-control observation and metrics protocols and never submits SQL,
// proposals, membership changes, or fault signals to a running cluster.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"go.etcd.io/raft/v3"
)

const (
	defaultInterval       = 500 * time.Millisecond
	defaultRequestTimeout = 350 * time.Millisecond
	defaultMaxBytes       = 8 << 20
	maxManifestBytes      = 8 << 20
	maxGroups             = 64
	maxNodes              = 6
	maxCaptureConcurrency = 8
	preflightCycles       = 3
)

type manifest struct {
	NodeLog struct {
		Path string `json:"path"`
	} `json:"node_log"`
	Listeners struct {
		Control string `json:"control"`
	} `json:"listeners"`
	TLS struct {
		Certificate string `json:"certificate"`
		Key         string `json:"key"`
		Roots       string `json:"roots"`
		IdentityOID string `json:"identity_oid"`
	} `json:"tls"`
	Groups []manifestGroup `json:"groups"`
}

// clusterManifest carries the controller credential that is authorized for
// shard-control observations. A storage node credential can establish the
// same mutually authenticated transport, but the RF3 policy deliberately
// withholds membership capability from storage principals.
type clusterManifest struct {
	GatewayCertificate string `json:"gateway_certificate"`
	GatewayKey         string `json:"gateway_key"`
	Roots              string `json:"roots"`
}

type manifestGroup struct {
	Route struct {
		ClusterID             string `json:"cluster_id"`
		ClusterIncarnation    string `json:"cluster_incarnation"`
		TopologyRecoveryEpoch uint64 `json:"topology_recovery_epoch"`
		ShardIncarnation      string `json:"shard_incarnation"`
		GroupID               string `json:"group_id"`
		MemberID              uint64 `json:"member_id"`
		Distribution          string `json:"distribution"`
		Shard                 string `json:"shard"`
	} `json:"route"`
	Members []manifestMember `json:"members"`
}

type manifestMember struct {
	MemberID uint64 `json:"member_id"`
	NodeID   string `json:"node_id"`
}

type nodeConfig struct {
	ID      rafttransport.NodeID
	Address string
}

type groupConfig struct {
	Key          raftmember.GroupKey
	ID           string
	Distribution string
	Shard        string
	Members      []manifestMember
}

type config struct {
	Root           string
	Output         string
	Interval       time.Duration
	RequestTimeout time.Duration
	MaxBytes       int64
	Nodes          []nodeConfig
	Groups         []groupConfig
	Profile        *rafttransport.PeerTLS
}

type controlOpener struct {
	profile   *rafttransport.PeerTLS
	addresses map[rafttransport.NodeID]string
	deadline  time.Duration
}

func (opener controlOpener) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	if ctx == nil || opener.profile == nil || node == (rafttransport.NodeID{}) {
		return nil, errors.New("rf3-diagnostic: invalid control opener")
	}
	address, found := opener.addresses[node]
	if !found || address == "" {
		return nil, fmt.Errorf("rf3-diagnostic: no control address for node %x", node)
	}
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(opener.deadline)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	connection, err := opener.profile.Client(
		ctx, raw, node, rafttransport.TrafficShardControl,
		func() time.Time { return deadline },
	)
	if err != nil {
		return nil, err
	}
	// servicemetrics.Client deliberately leaves deadlines to its opener. The
	// same bound also makes a failed metrics exchange unable to hold a cycle.
	if err = connection.SetDeadline(deadline); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

type cycle struct {
	Schema          string          `json:"schema"`
	Sequence        uint64          `json:"sequence"`
	UTC             string          `json:"utc"`
	ElapsedNS       int64           `json:"elapsed_ns"`
	Groups          []groupSnapshot `json:"groups"`
	ExpectedCuts    uint64          `json:"expected_cuts"`
	ValidCuts       uint64          `json:"valid_cuts"`
	PreflightReady  bool            `json:"preflight_ready"`
	PreflightReason string          `json:"preflight_reason,omitempty"`
	SamplingErrs    uint64          `json:"sampling_errors"`
}

type groupSnapshot struct {
	GroupID      string           `json:"group_id"`
	Distribution string           `json:"distribution"`
	Shard        string           `json:"shard"`
	Members      []memberSnapshot `json:"members"`
}

type memberSnapshot struct {
	MemberID     uint64            `json:"member_id"`
	NodeID       string            `json:"node_id"`
	Status       *statusSnapshot   `json:"status,omitempty"`
	Progress     *progressSnapshot `json:"progress,omitempty"`
	Metrics      *metricsSnapshot  `json:"metrics,omitempty"`
	ObserveError string            `json:"observe_error,omitempty"`
	MetricsError string            `json:"metrics_error,omitempty"`
	Error        string            `json:"error,omitempty"`
	ElapsedNS    int64             `json:"elapsed_ns"`
}

type statusSnapshot struct {
	MemberID          uint64 `json:"member_id"`
	LeaderID          uint64 `json:"leader_id"`
	Term              uint64 `json:"term"`
	Commit            uint64 `json:"commit"`
	Applied           uint64 `json:"applied"`
	CheckpointApplied uint64 `json:"checkpoint_applied"`
	LeadTransferee    uint64 `json:"lead_transferee"`
	RaftState         uint8  `json:"raft_state"`
	RaftStateName     string `json:"raft_state_name"`
	StateApplied      uint64 `json:"state_applied"`
	ReplicaSetVersion uint64 `json:"replica_set_version"`
}

type progressSnapshot struct {
	Found           bool   `json:"found"`
	Learner         bool   `json:"learner"`
	RecentActive    bool   `json:"recent_active"`
	FlowPaused      bool   `json:"flow_paused"`
	Match           uint64 `json:"match"`
	Next            uint64 `json:"next"`
	PendingSnapshot uint64 `json:"pending_snapshot"`
}

type metricsSnapshot struct {
	ProposalCommands   uint64 `json:"proposal_commands"`
	ProposalBytes      uint64 `json:"proposal_bytes"`
	AppliedEntries     uint64 `json:"applied_entries"`
	ReadyPersisted     uint64 `json:"ready_persisted"`
	CommitAdvancements uint64 `json:"commit_advancements"`
	CommittedEntries   uint64 `json:"committed_entries"`
}

type boundedWriter struct {
	writer io.Writer
	left   int64
}

func (writer *boundedWriter) Write(value []byte) (int, error) {
	if writer == nil || writer.writer == nil || writer.left < int64(len(value)) {
		return 0, io.ErrShortBuffer
	}
	written, err := writer.writer.Write(value)
	writer.left -= int64(written)
	return written, err
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("rf3-diagnostic", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	root := flags.String("root", "/data/vibe", "candidate RF3 durable root")
	output := flags.String("output", "/evidence/per-group-snapshots.jsonl", "bounded JSONL output")
	readyFile := flags.String("ready-file", "", "write after every expected status and metrics cut is valid")
	interval := flags.Duration("interval", defaultInterval, "interval between observation cycles")
	timeout := flags.Duration("request-timeout", defaultRequestTimeout, "per-member observation timeout")
	maxBytes := flags.Int64("max-bytes", defaultMaxBytes, "maximum output bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *root == "" || *output == "" || *interval <= 0 || *interval > 10*time.Second ||
		*timeout <= 0 || *timeout > 5*time.Second || *maxBytes <= 0 || *maxBytes > 64<<20 {
		return errors.New("rf3-diagnostic: invalid configuration")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := loadConfig(*root, *interval, *timeout, *maxBytes)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := &boundedWriter{writer: file, left: cfg.MaxBytes}
	expectedCuts := expectedValidCuts(cfg)
	if expectedCuts == 0 {
		return errors.New("rf3-diagnostic: no group/member bindings")
	}
	if *readyFile != "" {
		if err = os.Remove(*readyFile); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	var sequence uint64
	for {
		sequence++
		started := time.Now()
		record := capture(ctx, cfg, sequence)
		record.ElapsedNS = time.Since(started).Nanoseconds()
		record.ExpectedCuts = expectedCuts
		record.PreflightReady = record.ValidCuts == expectedCuts
		if !record.PreflightReady {
			record.PreflightReason = fmt.Sprintf("valid status and metrics cuts %d/%d", record.ValidCuts, expectedCuts)
		}
		line, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return marshalErr
		}
		line = append(line, '\n')
		if _, err = writer.Write(line); err != nil {
			if errors.Is(err, io.ErrShortBuffer) {
				return errors.New("rf3-diagnostic: bounded output exhausted")
			}
			return err
		}
		if err = file.Sync(); err != nil {
			return err
		}
		if record.PreflightReady && *readyFile != "" {
			if err = writeReadyFile(*readyFile); err != nil {
				return err
			}
		}
		if sequence >= preflightCycles && !record.PreflightReady {
			return fmt.Errorf("rf3-diagnostic: preflight incomplete: %s", record.PreflightReason)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func loadConfig(root string, interval, timeout time.Duration, maxBytes int64) (config, error) {
	if root == "" || interval <= 0 || timeout <= 0 || maxBytes <= 0 {
		return config{}, errors.New("rf3-diagnostic: invalid root configuration")
	}
	var manifests []manifest
	for node := 1; node <= maxNodes; node++ {
		path := filepath.Join(root, fmt.Sprintf("node-%d", node), "serve-rf3.vibejson")
		raw, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && len(manifests) != 0 {
				break
			}
			return config{}, fmt.Errorf("rf3-diagnostic: read node manifest %d: %w", node, err)
		}
		if len(raw) == 0 || len(raw) > maxManifestBytes {
			return config{}, errors.New("rf3-diagnostic: node manifest exceeds bound")
		}
		var value manifest
		if err = json.Unmarshal(raw, &value); err != nil {
			return config{}, fmt.Errorf("rf3-diagnostic: decode node manifest %d: %w", node, err)
		}
		if value.Listeners.Control == "" || value.TLS.Certificate == "" || value.TLS.Key == "" ||
			value.TLS.Roots == "" || value.TLS.IdentityOID == "" || len(value.Groups) == 0 {
			return config{}, fmt.Errorf("rf3-diagnostic: incomplete node manifest %d", node)
		}
		manifests = append(manifests, value)
	}
	if len(manifests) < 3 || len(manifests) > maxNodes {
		return config{}, fmt.Errorf("rf3-diagnostic: expected 3..%d node manifests", maxNodes)
	}
	if len(manifests[0].Groups) > maxGroups {
		return config{}, fmt.Errorf("rf3-diagnostic: too many groups: %d", len(manifests[0].Groups))
	}
	clusterRaw, err := os.ReadFile(filepath.Join(root, "cluster.vibejson"))
	if err != nil {
		return config{}, fmt.Errorf("rf3-diagnostic: read cluster manifest: %w", err)
	}
	if len(clusterRaw) == 0 || len(clusterRaw) > maxManifestBytes {
		return config{}, errors.New("rf3-diagnostic: cluster manifest exceeds bound")
	}
	var cluster clusterManifest
	if err = json.Unmarshal(clusterRaw, &cluster); err != nil {
		return config{}, fmt.Errorf("rf3-diagnostic: decode cluster manifest: %w", err)
	}
	if cluster.GatewayCertificate == "" || cluster.GatewayKey == "" || cluster.Roots == "" {
		return config{}, errors.New("rf3-diagnostic: incomplete cluster TLS manifest")
	}
	profile, err := servicetls.LoadProfile(cluster.GatewayCertificate, cluster.GatewayKey,
		cluster.Roots, manifests[0].TLS.IdentityOID, time.Now)
	if err != nil {
		return config{}, fmt.Errorf("rf3-diagnostic: load TLS profile: %w", err)
	}

	nodes := make([]nodeConfig, 0, len(manifests))
	addresses := make(map[rafttransport.NodeID]string, len(manifests))
	for index, value := range manifests {
		node, err := nodeIDFromGroup(value.Groups[0], profile.LocalIdentity().TrustDomain)
		if err != nil {
			return config{}, fmt.Errorf("rf3-diagnostic: node %d identity: %w", index+1, err)
		}
		if _, exists := addresses[node]; exists {
			return config{}, errors.New("rf3-diagnostic: duplicate node identity")
		}
		nodes = append(nodes, nodeConfig{ID: node, Address: value.Listeners.Control})
		addresses[node] = value.Listeners.Control
	}

	groups := make([]groupConfig, 0, len(manifests[0].Groups))
	seen := make(map[raftmember.GroupKey]struct{}, len(manifests[0].Groups))
	for index, value := range manifests[0].Groups {
		group, err := groupFromManifest(value, profile.LocalIdentity().TrustDomain)
		if err != nil {
			return config{}, fmt.Errorf("rf3-diagnostic: group %d: %w", index, err)
		}
		if _, exists := seen[group.Key]; exists {
			return config{}, errors.New("rf3-diagnostic: duplicate group identity")
		}
		seen[group.Key] = struct{}{}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(left, right int) bool { return groups[left].ID < groups[right].ID })
	return config{Root: root, Output: "", Interval: interval, RequestTimeout: timeout, MaxBytes: maxBytes,
		Nodes: nodes, Groups: groups, Profile: profile}, nil
}

func nodeIDFromGroup(group manifestGroup, domain rafttransport.TrustDomain) (rafttransport.NodeID, error) {
	if group.Route.MemberID == 0 {
		return rafttransport.NodeID{}, errors.New("group has no local member id")
	}
	for _, member := range group.Members {
		if member.MemberID != group.Route.MemberID {
			continue
		}
		node, err := parseNodeID(member.NodeID)
		if err != nil {
			continue
		}
		key, keyErr := groupFromManifest(group, domain)
		if keyErr == nil && key.Key.ClusterID == domain.ClusterID && key.Key.ClusterIncarnation == domain.ClusterIncarnation {
			return node, nil
		}
	}
	return rafttransport.NodeID{}, errors.New("no valid group member identity")
}

func groupFromManifest(value manifestGroup, domain rafttransport.TrustDomain) (groupConfig, error) {
	route := value.Route
	clusterID, err := decode16(route.ClusterID)
	if err != nil {
		return groupConfig{}, fmt.Errorf("cluster_id: %w", err)
	}
	clusterIncarnation, err := decode16(route.ClusterIncarnation)
	if err != nil {
		return groupConfig{}, fmt.Errorf("cluster_incarnation: %w", err)
	}
	if clusterID != domain.ClusterID || clusterIncarnation != domain.ClusterIncarnation {
		return groupConfig{}, errors.New("group trust domain differs from TLS profile")
	}
	shardIncarnation, err := decode16(route.ShardIncarnation)
	if err != nil {
		return groupConfig{}, fmt.Errorf("shard_incarnation: %w", err)
	}
	groupID, err := decode16(route.GroupID)
	if err != nil {
		return groupConfig{}, fmt.Errorf("group_id: %w", err)
	}
	if route.TopologyRecoveryEpoch == 0 || route.Distribution == "" || route.Shard == "" || len(value.Members) != 3 {
		return groupConfig{}, errors.New("invalid group route or RF3 roster")
	}
	for _, member := range value.Members {
		if member.MemberID == 0 {
			return groupConfig{}, errors.New("invalid RF3 member id")
		}
		if _, err = parseNodeID(member.NodeID); err != nil {
			return groupConfig{}, err
		}
	}
	return groupConfig{Key: raftmember.GroupKey{ClusterID: clusterID,
		ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: route.TopologyRecoveryEpoch,
		ShardIncarnation: shardIncarnation, GroupID: groupID}, ID: route.GroupID,
		Distribution: route.Distribution, Shard: route.Shard, Members: append([]manifestMember(nil), value.Members...)}, nil
}

func decode16(value string) ([16]byte, error) {
	var result [16]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, errors.New("expected 16-byte lowercase hex")
	}
	copy(result[:], decoded)
	if result == ([16]byte{}) {
		return result, errors.New("zero identity")
	}
	return result, nil
}

func parseNodeID(value string) (rafttransport.NodeID, error) {
	var result rafttransport.NodeID
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, errors.New("expected 16-byte node hex")
	}
	copy(result[:], decoded)
	if result == (rafttransport.NodeID{}) {
		return result, errors.New("zero node identity")
	}
	return result, nil
}

func capture(ctx context.Context, cfg config, sequence uint64) cycle {
	record := cycle{Schema: "vibedb.rf3-diagnostic/1", Sequence: sequence,
		UTC: time.Now().UTC().Format(time.RFC3339Nano), Groups: make([]groupSnapshot, len(cfg.Groups))}
	concurrency := make(chan struct{}, maxCaptureConcurrency)
	var wg sync.WaitGroup
	for index, group := range cfg.Groups {
		record.Groups[index] = groupSnapshot{GroupID: group.ID, Distribution: group.Distribution,
			Shard: group.Shard, Members: make([]memberSnapshot, len(group.Members))}
		for memberIndex, member := range group.Members {
			record.Groups[index].Members[memberIndex] = memberSnapshot{MemberID: member.MemberID, NodeID: member.NodeID}
			wg.Add(1)
			go func(groupIndex, memberIndex int, group groupConfig, member manifestMember) {
				defer wg.Done()
				select {
				case concurrency <- struct{}{}:
					defer func() { <-concurrency }()
				case <-ctx.Done():
					record.Groups[groupIndex].Members[memberIndex].Error = ctx.Err().Error()
					return
				}
				record.Groups[groupIndex].Members[memberIndex] = captureMember(ctx, cfg, group, member)
			}(index, memberIndex, group, member)
		}
	}
	wg.Wait()
	for _, group := range record.Groups {
		for _, member := range group.Members {
			if member.Status != nil && member.Metrics != nil {
				record.ValidCuts++
			}
			if member.Error != "" {
				record.SamplingErrs++
			}
		}
	}
	return record
}

func expectedValidCuts(cfg config) uint64 {
	var total uint64
	for _, group := range cfg.Groups {
		total += uint64(len(group.Members))
	}
	return total
}

func writeReadyFile(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = io.WriteString(file, "rf3-diagnostic preflight ready\n"); err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func captureMember(ctx context.Context, cfg config, group groupConfig, member manifestMember) (result memberSnapshot) {
	started := time.Now()
	result = memberSnapshot{MemberID: member.MemberID, NodeID: member.NodeID}
	defer func() { result.ElapsedNS = time.Since(started).Nanoseconds() }()
	node, err := parseNodeID(member.NodeID)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	observeCtx, cancelObserve := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancelObserve()
	metricsCtx, cancelMetrics := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancelMetrics()
	operation := [32]byte(sha256.Sum256([]byte("vibedb/rf3-diagnostic/operation")))
	step := [32]byte(sha256.Sum256([]byte("vibedb/rf3-diagnostic/step")))
	targetMember := group.Members[0].MemberID
	request := replicacontrol.Request{Operation: operation, Step: step, Group: group.Key, TargetMember: targetMember}
	opener := controlOpener{profile: cfg.Profile, addresses: nodeAddresses(cfg.Nodes), deadline: cfg.RequestTimeout}
	client, err := replicacontrol.NewClient(replicacontrol.ClientOptions{Opener: opener,
		ReadDeadline:  func() time.Time { return time.Now().Add(cfg.RequestTimeout) },
		WriteDeadline: func() time.Time { return time.Now().Add(cfg.RequestTimeout) }})
	if err != nil {
		result.Error = err.Error()
		return result
	}
	var (
		observation replicacontrol.Observation
		observeErr  error
		metrics     servicemetrics.Snapshot
		metricsErr  error
	)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		observation, observeErr = client.Observe(observeCtx, node, request)
	}()
	go func() {
		defer wg.Done()
		metrics, metricsErr = readGroupMetrics(metricsCtx, cfg, node, group.Key)
	}()
	wg.Wait()
	if observeErr != nil || metricsErr != nil {
		if observeErr != nil {
			result.ObserveError = observeErr.Error()
		}
		if metricsErr != nil {
			result.MetricsError = metricsErr.Error()
		}
		result.Error = joinError(observeErr, metricsErr)
		// Keep whichever cut arrived. This makes a transient protocol failure
		// distinguishable from a node that was unreachable for the whole cycle.
	}
	if observeErr == nil {
		stateName := raft.StateType(observation.Status.RaftState).String()
		result.Status = &statusSnapshot{MemberID: observation.Status.MemberID, LeaderID: observation.Status.LeaderID,
			Term: observation.Status.Term, Commit: observation.Status.Commit, Applied: observation.Status.Applied,
			CheckpointApplied: observation.Status.CheckpointApplied, LeadTransferee: observation.Status.LeadTransferee,
			RaftState: uint8(observation.Status.RaftState), RaftStateName: stateName,
			StateApplied: observation.State.Applied, ReplicaSetVersion: observation.Publication.ReplicaSetVersion}
		result.Progress = &progressSnapshot{Found: observation.ProgressFound, Learner: observation.Progress.Learner,
			RecentActive: observation.Progress.RecentActive, FlowPaused: observation.Progress.FlowPaused,
			Match: observation.Progress.Match, Next: observation.Progress.Next,
			PendingSnapshot: observation.Progress.PendingSnapshot}
	}
	if metricsErr == nil {
		result.Metrics = &metricsSnapshot{ProposalCommands: metrics.Metrics.ProposalCommands,
			ProposalBytes: metrics.Metrics.ProposalBytes, AppliedEntries: metrics.Metrics.AppliedEntries,
			ReadyPersisted: metrics.Metrics.ReadyPersisted, CommitAdvancements: metrics.Metrics.CommitAdvancements,
			CommittedEntries: metrics.Metrics.CommittedEntries}
	}
	return result
}

func nodeAddresses(nodes []nodeConfig) map[rafttransport.NodeID]string {
	result := make(map[rafttransport.NodeID]string, len(nodes))
	for _, node := range nodes {
		result[node.ID] = node.Address
	}
	return result
}

func readGroupMetrics(ctx context.Context, cfg config, node rafttransport.NodeID, group raftmember.GroupKey) (servicemetrics.Snapshot, error) {
	opener := controlOpener{profile: cfg.Profile, addresses: nodeAddresses(cfg.Nodes), deadline: cfg.RequestTimeout}
	client := servicemetrics.Client{Open: func(ctx context.Context) (rafttransport.PeerConnection, error) {
		return opener.OpenShardControl(ctx, node)
	}}
	return client.ReadGroup(ctx, group)
}

func joinError(left, right error) string {
	return errors.Join(left, right).Error()
}
