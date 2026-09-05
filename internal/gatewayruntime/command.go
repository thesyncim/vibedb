// Package gatewayruntime contains the complete gateway serving assembly.
//
// The command package is intentionally a very small process wrapper. Keeping
// command dispatch here lets the standalone gateway and an embedded frontend
// share the same catalog, durable request, PostgreSQL, and public serving
// implementation.
package gatewayruntime

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/thesyncim/vibedb/gateway"
)

// Run dispatches one vibedb-gateway command. It is exported for the standalone
// command wrapper; embedded users should construct Runtime with Open instead.
func Run(args []string) int {
	return run(args)
}

func run(args []string) int {
	if len(args) < 2 {
		usage()
		return 2
	}
	switch args[1] {
	case "inspect":
		return runInspect(args[2:])
	case "validate":
		return runValidate(args[2:])
	case "serve":
		return runServe(args[2:])
	case "schema-rollout":
		return runServe(append(args[2:], "-schema-rollout-once"))
	case "restore-activate":
		return runRestoreActivate(args[2:])
	default:
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb-gateway inspect  -catalog <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-gateway validate -catalog <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-gateway serve    -catalog <path> [-listen <addr>]")
	fmt.Fprintln(os.Stderr, "  vibedb-gateway schema-rollout <authenticated serve flags> -schema-rollout-plan <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-gateway restore-activate -manifest <path>")
}

// runValidate loads and re-validates a persisted catalog, reporting the outcome.
func runValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	catalog := fs.String("catalog", "", "path to the persisted catalog generation")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *catalog == "" {
		usage()
		return 2
	}
	snap, err := gateway.LoadSnapshot(*catalog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid catalog %q: %v\n", *catalog, err)
		return 1
	}
	fmt.Fprintf(os.Stdout, "catalog %q is valid: generation %d, %d endpoints\n",
		*catalog, snap.Generation(), snap.EndpointCount())
	return 0
}

// runInspect loads a persisted catalog and prints its routing configuration and
// endpoint membership.
func runInspect(args []string) int {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	catalog := fs.String("catalog", "", "path to the persisted catalog generation")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *catalog == "" {
		usage()
		return 2
	}
	snap, err := gateway.LoadSnapshot(*catalog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid catalog %q: %v\n", *catalog, err)
		return 1
	}

	fmt.Fprintf(os.Stdout, "generation %d\n", snap.Generation())
	for i := 0; i < snap.DistributionCount(); i++ {
		spec, _ := snap.DistributionAt(i)
		fmt.Fprintf(os.Stdout, "distribution %q arity=%d mapper=%d\n",
			spec.Name, spec.Arity, spec.MapperVersion)
		man, ok := snap.Manifest(spec.Name)
		if !ok {
			continue
		}
		fmt.Fprintf(os.Stdout, "  routing version %d, %d shards\n", man.Version(), man.ShardCount())
		for i := 0; i < man.ShardCount(); i++ {
			info, _ := man.ShardInfo(i)
			addr := "<unresolved>"
			if len(info.Leaders) > 0 {
				if a, err := snap.Address(info.Leaders[0]); err == nil {
					addr = a
				}
			}
			fmt.Fprintf(os.Stdout, "    shard %q allocation-generation=%d epoch=%d leader=%s\n",
				info.ID, info.AllocationGeneration, info.Epoch, addr)
		}
	}

	ids := make([]string, 0, snap.EndpointCount())
	for i := 0; i < snap.DistributionCount(); i++ {
		spec, _ := snap.DistributionAt(i)
		man, ok := snap.Manifest(spec.Name)
		if !ok {
			continue
		}
		for i := 0; i < man.ShardCount(); i++ {
			info, _ := man.ShardInfo(i)
			for _, ep := range info.Leaders {
				ids = append(ids, string(ep))
			}
		}
	}
	sort.Strings(ids)
	fmt.Fprintf(os.Stdout, "%d endpoints referenced\n", len(dedup(ids)))
	return 0
}

// dedup returns the sorted input with adjacent duplicates removed.
func dedup(sorted []string) []string {
	out := sorted[:0:0]
	for i, s := range sorted {
		if i == 0 || s != sorted[i-1] {
			out = append(out, s)
		}
	}
	return out
}
