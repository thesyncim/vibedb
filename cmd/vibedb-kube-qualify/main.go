// Command vibedb-kube-qualify is a bounded client and process inspector for the
// development-only Kubernetes qualification lane. It is not a general client.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibejson"
)

const (
	qualificationMaxResponseBytes = 1 << 20
	qualificationIdentityOID      = "1.3.6.1.4.1.32473.1.1"
)

var errQualification = errors.New("vibedb-kube-qualify: qualification failed")

type clientOptions struct {
	address, certificate, key, roots, gatewayNode, bootstrapState, state string
	samples                                                              int
	maximumP99, maximumLatency                                           time.Duration
}

type bootstrapIdentity struct {
	GatewayNodeID string `json:"gateway_node"`
}

type issuerOpenRequest struct {
	Op             string `json:"op"`
	InstallationID string `json:"installation_id"`
	IssuerEpoch    uint64 `json:"issuer_epoch"`
	LaneOrdinal    uint16 `json:"lane_ordinal"`
}

type issuerOpenResponse struct {
	OK             bool   `json:"ok"`
	InstallationID string `json:"installation_id"`
	IssuerEpoch    uint64 `json:"issuer_epoch"`
	LaneOrdinal    uint16 `json:"lane_ordinal"`
	GrantDigest    string `json:"grant_digest"`
	Error          string `json:"error"`
}

type qualifyParam struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

type qualifyStatement struct {
	SQL    string         `json:"sql"`
	Params []qualifyParam `json:"params,omitempty"`
}

type execBatchRequest struct {
	Op             string             `json:"op"`
	RequestID      string             `json:"request_id"`
	InstallationID string             `json:"installation_id"`
	IssuerEpoch    uint64             `json:"issuer_epoch"`
	LaneOrdinal    uint16             `json:"lane_ordinal"`
	GrantDigest    string             `json:"grant_digest"`
	IssuerSequence uint64             `json:"issuer_sequence"`
	Class          string             `json:"class"`
	Statements     []qualifyStatement `json:"statements"`
}

type queryRequest struct {
	Op     string         `json:"op"`
	SQL    string         `json:"sql"`
	Class  string         `json:"class"`
	Params []qualifyParam `json:"params"`
}

type qualificationEvidence struct {
	Format        uint16 `json:"format"`
	Samples       int    `json:"samples"`
	P50Micros     int64  `json:"p50_micros"`
	P99Micros     int64  `json:"p99_micros"`
	MaximumMicros int64  `json:"maximum_micros"`
	Recovered     bool   `json:"recovered"`
}

type processEvidence struct {
	Format       uint16 `json:"format"`
	RSSBytes     uint64 `json:"rss_bytes"`
	StorageBytes uint64 `json:"storage_bytes"`
	WALBytes     uint64 `json:"wal_bytes"`
	Files        uint64 `json:"files"`
}

type dnsEvidence struct {
	Format    uint16 `json:"format"`
	Namespace string `json:"namespace"`
	Resolved  int    `json:"resolved"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: vibedb-kube-qualify write|verify|measure|dns")
		os.Exit(2)
	}
	if os.Args[1] == "measure" {
		if err := runMeasure(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if os.Args[1] == "dns" {
		if err := runDNS(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	options, err := parseClientOptions(os.Args[1], os.Args[2:])
	if err == nil {
		err = runClient(os.Args[1], options)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runMeasure(arguments []string) error {
	flags := flag.NewFlagSet("measure", flag.ContinueOnError)
	root := flags.String("root", "/var/lib/vibedb", "pod durable root")
	maximumRSS := flags.Uint64("max-rss-bytes", 1<<30, "hard RSS ceiling")
	maximumStorage := flags.Uint64("max-storage-bytes", 1<<30, "hard apparent durable-byte ceiling")
	maximumWAL := flags.Uint64("max-wal-bytes", 512<<20, "hard WAL-byte ceiling")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 ||
		!filepath.IsAbs(*root) || filepath.Clean(*root) != *root || *root == string(filepath.Separator) {
		return errQualification
	}
	status, err := os.ReadFile("/proc/1/status")
	if err != nil || len(status) > 1<<20 {
		return errors.Join(errQualification, err)
	}
	rss, ok := parseRSS(status)
	if !ok {
		return errQualification
	}
	evidence := processEvidence{Format: 1, RSSBytes: rss}
	err = filepath.WalkDir(*root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errQualification
		}
		if entry.IsDir() {
			return nil
		}
		evidence.Files++
		if evidence.Files > 100_000 {
			return errQualification
		}
		info, infoErr := entry.Info()
		if infoErr != nil || info.Size() < 0 {
			return errors.Join(errQualification, infoErr)
		}
		size := uint64(info.Size())
		if ^uint64(0)-evidence.StorageBytes < size {
			return errQualification
		}
		evidence.StorageBytes += size
		if strings.HasSuffix(path, ".wal") {
			evidence.WALBytes += size
		}
		return nil
	})
	if err != nil {
		return err
	}
	if evidence.RSSBytes > *maximumRSS || evidence.StorageBytes > *maximumStorage ||
		evidence.WALBytes > *maximumWAL {
		return fmt.Errorf("%w: rss=%d/%d storage=%d/%d wal=%d/%d", errQualification,
			evidence.RSSBytes, *maximumRSS, evidence.StorageBytes, *maximumStorage,
			evidence.WALBytes, *maximumWAL)
	}
	raw, err := vibejson.Marshal(&evidence)
	if err == nil {
		_, err = os.Stdout.Write(append(raw, '\n'))
	}
	return err
}

func runDNS(arguments []string) error {
	flags := flag.NewFlagSet("dns", flag.ContinueOnError)
	namespace := flags.String("namespace", "vibedb-test", "Kubernetes namespace")
	timeout := flags.Duration("timeout", 30*time.Second, "whole DNS qualification deadline")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 ||
		!validDNSLabel(*namespace) || *timeout <= 0 || *timeout > 2*time.Minute {
		return errQualification
	}
	hosts := make([]string, 0, 10)
	for _, role := range []string{"catalog", "ledger", "data"} {
		for ordinal := 0; ordinal < 3; ordinal++ {
			hosts = append(hosts, fmt.Sprintf("vibedb-%s-%d.vibedb-%s-peer.%s.svc", role,
				ordinal, role, *namespace))
		}
	}
	hosts = append(hosts, "vibedb-gateway-0.vibedb-gateway-peer."+*namespace+".svc")
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	for _, host := range hosts {
		addresses, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil || len(addresses) == 0 {
			return fmt.Errorf("%w: resolve %s: %v", errQualification, host, err)
		}
	}
	evidence := dnsEvidence{Format: 1, Namespace: *namespace, Resolved: len(hosts)}
	raw, err := vibejson.Marshal(&evidence)
	if err == nil {
		_, err = os.Stdout.Write(append(raw, '\n'))
	}
	return err
}

func validDNSLabel(value string) bool {
	if value == "" || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

func parseRSS(status []byte) (uint64, bool) {
	for _, line := range bytes.Split(status, []byte{'\n'}) {
		fields := bytes.Fields(line)
		if len(fields) != 3 || !bytes.Equal(fields[0], []byte("VmRSS:")) ||
			!bytes.Equal(fields[2], []byte("kB")) {
			continue
		}
		kilobytes, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil || kilobytes > ^uint64(0)/1024 {
			return 0, false
		}
		return kilobytes * 1024, true
	}
	return 0, false
}

func parseClientOptions(mode string, arguments []string) (clientOptions, error) {
	if mode != "write" && mode != "verify" {
		return clientOptions{}, errQualification
	}
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	var result clientOptions
	flags.StringVar(&result.address, "address", "127.0.0.1:17400", "forwarded gateway address")
	flags.StringVar(&result.certificate, "certificate", "", "qualification client certificate")
	flags.StringVar(&result.key, "key", "", "qualification client key")
	flags.StringVar(&result.roots, "roots", "", "qualification CA roots")
	flags.StringVar(&result.gatewayNode, "gateway-node", "", "expected gateway TLS node identity")
	flags.StringVar(&result.bootstrapState, "bootstrap-state", "", "canonical bootstrap state containing the gateway identity")
	flags.StringVar(&result.state, "state", "", "durable exact request file")
	flags.IntVar(&result.samples, "samples", 128, "verification read samples")
	flags.DurationVar(&result.maximumP99, "max-p99", time.Second, "hard p99 read-latency ceiling")
	flags.DurationVar(&result.maximumLatency, "max-latency", 5*time.Second, "hard maximum read-latency ceiling")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 ||
		result.certificate == "" || result.key == "" || result.roots == "" ||
		result.state == "" || result.samples < 1 || result.samples > 4096 ||
		result.maximumP99 <= 0 || result.maximumLatency < result.maximumP99 ||
		result.maximumLatency > 30*time.Second {
		return clientOptions{}, errQualification
	}
	if result.gatewayNode == "" && result.bootstrapState != "" {
		identity, err := loadBootstrapIdentity(result.bootstrapState)
		if err != nil {
			return clientOptions{}, err
		}
		result.gatewayNode = identity.GatewayNodeID
	}
	node, err := decodeNode(result.gatewayNode)
	if err != nil || node == (rafttransport.NodeID{}) {
		return clientOptions{}, errQualification
	}
	if !filepath.IsAbs(result.state) || filepath.Clean(result.state) != result.state {
		return clientOptions{}, errQualification
	}
	return result, nil
}

func loadBootstrapIdentity(path string) (bootstrapIdentity, error) {
	var result bootstrapIdentity
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return result, errQualification
	}
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(raw) > 1<<20 || vibejson.Unmarshal(raw, &result) != nil {
		return result, errors.Join(errQualification, err)
	}
	return result, nil
}

func runClient(mode string, options clientOptions) error {
	profile, err := servicetls.LoadProfile(options.certificate, options.key, options.roots,
		qualificationIdentityOID, time.Now)
	if err != nil {
		return errors.Join(errQualification, err)
	}
	gatewayNode, _ := decodeNode(options.gatewayNode)
	client, err := servicetls.NewClient(servicetls.ClientOptions{TLS: profile,
		Class:     rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: options.address, Node: gatewayNode}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: func() time.Time { return time.Now().Add(5 * time.Second) },
		MaxConnections: 1, MaxHandshakes: 1})
	if err != nil {
		return errors.Join(errQualification, err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	connection, err := dialQualification(ctx, client, options.address)
	if err != nil {
		return err
	}
	defer connection.Close()
	wire := &qualificationWire{connection: connection,
		reader: bufio.NewReaderSize(connection, qualificationMaxResponseBytes+1)}
	request, err := loadOrCreateRequest(mode, options.state, wire)
	if err != nil {
		return err
	}
	response, _, err := wire.roundTrip(request)
	if err != nil || !committedResponse(response) {
		return errors.Join(errQualification, err)
	}
	latencies := make([]time.Duration, options.samples)
	for index := range latencies {
		response, latency, roundErr := wire.roundTrip(qualificationQuery())
		if roundErr != nil || !qualificationRowVisible(response) {
			return errors.Join(errQualification, roundErr)
		}
		latencies[index] = latency
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	evidence := qualificationEvidence{Format: 1, Samples: len(latencies),
		P50Micros:     latencies[(len(latencies)-1)/2].Microseconds(),
		P99Micros:     latencies[((len(latencies)-1)*99)/100].Microseconds(),
		MaximumMicros: latencies[len(latencies)-1].Microseconds(), Recovered: mode == "verify"}
	if latencies[((len(latencies)-1)*99)/100] > options.maximumP99 ||
		latencies[len(latencies)-1] > options.maximumLatency {
		return fmt.Errorf("%w: p99=%s/%s maximum=%s/%s", errQualification,
			latencies[((len(latencies)-1)*99)/100], options.maximumP99,
			latencies[len(latencies)-1], options.maximumLatency)
	}
	raw, err := vibejson.Marshal(&evidence)
	if err == nil {
		_, err = os.Stdout.Write(append(raw, '\n'))
	}
	return err
}

func decodeNode(value string) (rafttransport.NodeID, error) {
	var node rafttransport.NodeID
	if len(value) != hex.EncodedLen(len(node)) {
		return node, errQualification
	}
	_, err := hex.Decode(node[:], []byte(value))
	return node, err
}

func dialQualification(ctx context.Context, client *servicetls.Client, address string) (net.Conn, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, err := client.Dial(ctx, address)
		if err == nil {
			return connection, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(errQualification, context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

type qualificationWire struct {
	connection net.Conn
	reader     *bufio.Reader
}

func (wire *qualificationWire) roundTrip(request []byte) ([]byte, time.Duration, error) {
	if wire == nil || wire.connection == nil || len(request) == 0 || len(request) > qualificationMaxResponseBytes {
		return nil, 0, errQualification
	}
	if err := wire.connection.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, 0, err
	}
	started := time.Now()
	if _, err := io.Copy(wire.connection, io.MultiReader(bytes.NewReader(request), bytes.NewReader([]byte{'\n'}))); err != nil {
		return nil, 0, err
	}
	response, err := wire.reader.ReadSlice('\n')
	latency := time.Since(started)
	if err != nil || len(response) > qualificationMaxResponseBytes {
		return nil, latency, errors.Join(errQualification, err)
	}
	return bytes.TrimSpace(response), latency, nil
}

func loadOrCreateRequest(mode, path string, wire *qualificationWire) ([]byte, error) {
	if mode == "verify" {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		return io.ReadAll(io.LimitReader(file, qualificationMaxResponseBytes+1))
	}
	if _, err := os.Lstat(path); err == nil || !errors.Is(err, os.ErrNotExist) {
		return nil, errQualification
	}
	const installation = "81000000000000000000000000000000"
	open, err := vibejson.Marshal(&issuerOpenRequest{Op: "issuer_open", InstallationID: installation,
		IssuerEpoch: 1, LaneOrdinal: 0})
	if err != nil {
		return nil, err
	}
	response, _, err := wire.roundTrip(open)
	var grant issuerOpenResponse
	if err != nil || vibejson.Unmarshal(response, &grant) != nil || !grant.OK || grant.Error != "" ||
		grant.InstallationID != installation || grant.IssuerEpoch != 1 || grant.LaneOrdinal != 0 ||
		len(grant.GrantDigest) != 64 {
		return nil, errors.Join(errQualification, err)
	}
	request, err := vibejson.Marshal(&execBatchRequest{Op: "exec_batch",
		RequestID: "91000000000000000000000000000000", InstallationID: installation,
		IssuerEpoch: 1, LaneOrdinal: 0, GrantDigest: grant.GrantDigest, IssuerSequence: 1,
		Class: "interactive", Statements: []qualifyStatement{{SQL: "INSERT INTO documents VALUES (?)",
			Params: []qualifyParam{{Kind: "document", Text: `{"id":"kind-proof"}`}}}}})
	if err != nil {
		return nil, err
	}
	return request, writeExclusive(path, request)
}

func qualificationQuery() []byte {
	raw, _ := vibejson.Marshal(&queryRequest{Op: "query",
		SQL: "SELECT id FROM documents WHERE id = ?", Class: "interactive",
		Params: []qualifyParam{{Kind: "string", Text: "kind-proof"}}})
	return raw
}

func committedResponse(raw []byte) bool {
	document, err := vibejson.Parse(raw)
	if err != nil {
		return false
	}
	committed, ok := document.Get("committed")
	if !ok {
		return false
	}
	value, ok := committed.Bool()
	return ok && value
}

func qualificationRowVisible(raw []byte) bool {
	document, err := vibejson.Parse(raw)
	if err != nil {
		return false
	}
	rowsNode, ok := document.Get("rows")
	if !ok {
		return false
	}
	rows, ok := rowsNode.Array()
	if !ok || len(rows) != 1 {
		return false
	}
	cells, ok := rows[0].Array()
	text, textOK := "", false
	if ok && len(cells) == 1 {
		text, textOK = cells[0].Text()
	}
	return textOK && text == "kind-proof"
}

func writeExclusive(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}
