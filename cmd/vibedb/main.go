// Command vibedb provides operator-facing workflows composed from the shipped
// gateway and shard commands.
package main

import (
	"fmt"
	"os"
)

func main() { os.Exit(run(os.Args)) }

func run(args []string) int {
	if len(args) < 3 || args[1] != "cluster" || args[2] != "dev" {
		usage()
		return 2
	}
	return runClusterDev(args[3:])
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb cluster dev --replicas 1|3 [--physical-nodes 3|6] [--pg-listen address | --pg-listens address,...] [--table-schema file ...] --root <absolute-path>")
}
