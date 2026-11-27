// Command vibedb provides operator-facing workflows composed from the shipped
// gateway and shard commands.
package main

import (
	"fmt"
	"os"
)

func main() { os.Exit(run(os.Args)) }

func run(args []string) int {
	if len(args) < 3 || args[1] != "cluster" {
		usage()
		return 2
	}
	switch args[2] {
	case "dev":
		return runClusterDev(args[3:])
	case "init-operator":
		return runClusterInitOperator(args[3:])
	case "init-node":
		return runClusterInitNode(args[3:])
	case "rotate-cert":
		return runClusterRotateCert(args[3:])
	case "policy":
		return runClusterPolicy(args[3:])
	case "nodes", "join", "rebalance", "decommission", "status":
		return runClusterControl("cluster_"+args[2], args[3:])
	default:
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb cluster dev --replicas 1|3 [--physical-nodes 3|6] [--pg-listen address | --pg-listens address,...] [--table-schema file ...] --root <absolute-path>")
	fmt.Fprintln(os.Stderr, "  vibedb cluster init-operator --ca-cert <pem> --ca-key <pem> (--trust-domain-from <cert.pem> | --cluster-id <hex> --cluster-incarnation <hex>) --address <host:port> --server-node <hex> --out-dir <abs-path> [--roots <pem>] [--policy <path> --update-policy]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster init-node --ca-cert <pem> --ca-key <pem> (--trust-domain-from <cert.pem> | --cluster-id <hex> --cluster-incarnation <hex>) --root <abs-path> --out-dir <abs-path> --policy <abs-path> --failure-domain <name> --peer <host:port> --native <host:port> --snapshot <host:port> --control <host:port> --grant-node <hex>[,<hex>...]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster rotate-cert --ca-cert <pem> --ca-key <pem> --cert <pem> --key <pem> [--valid-for 8760h] [--node-descriptor <path>]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster policy (show|grant|revoke) --policy <path> [--node-id <hex>] [--capabilities <csv> | --all]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster nodes --profile <auth-client.vibejson> [--json] [--wait duration]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster join --profile <auth-client.vibejson> --node-file <public-node.vibejson> [--json] [--request-id hex] [--wait duration]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster rebalance --profile <auth-client.vibejson> [--json] [--request-id hex] [--wait duration]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster decommission --profile <auth-client.vibejson> --node <node-id> --incarnation <n> [--json] [--request-id hex] [--wait duration]")
	fmt.Fprintln(os.Stderr, "  vibedb cluster status --profile <auth-client.vibejson> --operation <operation-id> [--json] [--wait duration]")
}
