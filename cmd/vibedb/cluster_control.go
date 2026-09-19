package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thesyncim/vibedb/internal/clustercontrol"
)

const (
	clusterControlDefaultWait = 30 * time.Second
	clusterControlGrace       = 15 * time.Second
)

// runClusterControl is the shipped operator path.  It deliberately builds a
// clustercontrol.Request and sends it through the authenticated gateway-client
// listener; no command-local or test-only transport is available.
func runClusterControl(operation string, arguments []string) int {
	flags := flag.NewFlagSet("cluster "+operation, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var options struct {
		profile, nodeFile, nodeID, operationID, requestID string
		json                                              bool
		wait                                              time.Duration
		incarnation                                       uint64
		desiredNodeCount                                  uint
		maxMoves                                          uint
		maxMigrationBytes                                 uint64
		hysteresisPPM                                     uint64
	}
	flags.StringVar(&options.profile, "profile", "", "canonical authenticated operator client profile")
	flags.StringVar(&options.requestID, "request-id", "", "lowercase 256-bit idempotency request ID (generated when omitted)")
	flags.BoolVar(&options.json, "json", false, "emit the canonical machine-readable response")
	flags.DurationVar(&options.wait, "wait", 0, "server progress wait duration; cancellation never cancels the durable operation")
	flags.StringVar(&options.nodeFile, "node-file", "", "canonical public empty-node descriptor for join")
	flags.StringVar(&options.nodeID, "node", "", "retiring physical node ID for decommission")
	flags.StringVar(&options.operationID, "operation", "", "durable operation ID for status")
	flags.Uint64Var(&options.incarnation, "incarnation", 0, "retiring node incarnation for decommission")
	flags.UintVar(&options.desiredNodeCount, "desired-node-count", 0, "desired physical-node count for rebalance")
	flags.UintVar(&options.maxMoves, "max-moves", 0, "maximum moves admitted in one rebalance intent")
	flags.Uint64Var(&options.maxMigrationBytes, "max-migration-bytes", 0, "maximum migration bytes admitted in one rebalance intent")
	flags.Uint64Var(&options.hysteresisPPM, "hysteresis-ppm", 0, "minimum placement improvement in parts per million")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return 2
	}
	if options.profile == "" || options.wait < 0 {
		clusterControlUsage(operation)
		return 2
	}
	if operation == clustercontrol.OpJoin && options.nodeFile == "" ||
		operation == clustercontrol.OpDecommission && (options.nodeID == "" || options.incarnation == 0) ||
		operation == clustercontrol.OpStatus && options.operationID == "" {
		clusterControlUsage(operation)
		return 2
	}
	if operation != clustercontrol.OpJoin && options.nodeFile != "" ||
		operation != clustercontrol.OpDecommission && (options.nodeID != "" || options.incarnation != 0) ||
		operation != clustercontrol.OpStatus && options.operationID != "" {
		clusterControlUsage(operation)
		return 2
	}

	requestID := options.requestID
	if requestID == "" {
		var err error
		requestID, err = clustercontrol.NewRequestID()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cluster %s: generate request ID: %v\n", operation, err)
			return 1
		}
	}
	profile, err := clustercontrol.LoadProfile(options.profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster %s: load profile: %v\n", operation, err)
		return 1
	}
	client, err := clustercontrol.NewClient(profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster %s: create authenticated client: %v\n", operation, err)
		return 1
	}
	defer client.Close()

	request := clustercontrol.Request{Format: clustercontrol.Format, Op: operation, RequestID: requestID}
	if options.wait > 0 {
		if options.wait > time.Duration(clustercontrol.MaxWaitMillis)*time.Millisecond {
			fmt.Fprintf(os.Stderr, "cluster %s: --wait exceeds the 24-hour bound\n", operation)
			return 2
		}
		request.WaitMillis = uint64(options.wait / time.Millisecond)
	}
	switch operation {
	case clustercontrol.OpJoin:
		descriptor, descriptorErr := clustercontrol.LoadNodeDescriptor(options.nodeFile)
		if descriptorErr != nil {
			fmt.Fprintf(os.Stderr, "cluster join: load node descriptor: %v\n", descriptorErr)
			return 1
		}
		request.NodeDescriptor = &descriptor
	case clustercontrol.OpRebalance:
		if options.maxMoves == 0 {
			options.maxMoves = 4096
		}
		if options.maxMigrationBytes == 0 {
			options.maxMigrationBytes = 1 << 30
		}
		if options.maxMoves > 4096 || options.desiredNodeCount > 4096 {
			clusterControlUsage(operation)
			return 2
		}
		request.MaxMoves = uint16(options.maxMoves)
		request.MaxMigrationBytes = options.maxMigrationBytes
		request.DesiredNodeCount = uint16(options.desiredNodeCount)
		request.HysteresisPPM = options.hysteresisPPM
	case clustercontrol.OpDecommission:
		request.NodeID = options.nodeID
		request.NodeIncarnation = options.incarnation
	case clustercontrol.OpStatus:
		request.OperationID = options.operationID
	}

	waitFor := clusterControlDefaultWait
	if options.wait > 0 {
		waitFor = options.wait + clusterControlGrace
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	response, executeErr := client.Execute(ctx, request)
	if response.RequestID != "" {
		if printErr := printClusterResponse(response, options.json); printErr != nil {
			fmt.Fprintf(os.Stderr, "cluster %s: print response: %v\n", operation, printErr)
			return 1
		}
	}
	if executeErr != nil {
		fmt.Fprintf(os.Stderr, "cluster %s: %v\n", operation, executeErr)
		return 1
	}
	return 0
}

func printClusterResponse(response clustercontrol.Response, asJSON bool) error {
	if asJSON {
		raw, err := clustercontrol.EncodeResponse(response)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(raw)
		return err
	}
	fmt.Fprintf(os.Stdout, "op=%s ok=%t request_id=%s", response.Op, response.OK, response.RequestID)
	if response.OperationID != "" {
		fmt.Fprintf(os.Stdout, " operation_id=%s", response.OperationID)
	}
	if response.State != "" {
		fmt.Fprintf(os.Stdout, " state=%s", response.State)
	}
	fmt.Fprintf(os.Stdout, " catalog_generation=%d directory_revision=%d safe_to_stop=%t\n",
		response.CatalogGeneration, response.DirectoryRevision, response.SafeToStop)
	for _, node := range response.Nodes {
		fmt.Fprintf(os.Stdout, "node=%s incarnation=%d lifecycle=%s revision=%d catalog_generation=%d safe_to_stop=%t\n",
			node.NodeID, node.Incarnation, node.Lifecycle, node.Revision, node.CatalogGeneration, node.SafeToStop)
	}
	for _, blocker := range response.Blockers {
		fmt.Fprintf(os.Stdout, "blocker=%s detail=%s", blocker.Code, blocker.Detail)
		if blocker.NodeID != "" {
			fmt.Fprintf(os.Stdout, " node=%s incarnation=%d revision=%d", blocker.NodeID, blocker.NodeIncarnation, blocker.Revision)
		}
		fmt.Fprintln(os.Stdout)
	}
	if response.Error != "" {
		fmt.Fprintf(os.Stdout, "error=%s\n", response.Error)
	}
	return nil
}

func clusterControlUsage(operation string) {
	fmt.Fprintf(os.Stderr, "usage: vibedb cluster %s --profile <auth-client.vibejson> [--json] [--request-id <hex>] [--wait <duration>]", operation)
	switch operation {
	case clustercontrol.OpJoin:
		fmt.Fprint(os.Stderr, " --node-file <public-node.vibejson>")
	case clustercontrol.OpDecommission:
		fmt.Fprint(os.Stderr, " --node <node-id> --incarnation <n>")
	case clustercontrol.OpStatus:
		fmt.Fprint(os.Stderr, " --operation <operation-id>")
	case clustercontrol.OpRebalance:
		fmt.Fprint(os.Stderr, " [--max-moves <n>] [--max-migration-bytes <n>] [--desired-node-count <n>] [--hysteresis-ppm <n>]")
	}
	fmt.Fprintln(os.Stderr)
}
