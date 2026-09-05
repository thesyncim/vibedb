// Command vibedb-gateway is the standalone process wrapper around the
// reusable internal/gatewayruntime package.
package main

import (
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/gatewayruntime"
	"github.com/thesyncim/vibedb/internal/processprofile"
)

func main() {
	os.Exit(runProfiled(os.Args))
}

func runProfiled(args []string) int {
	if len(args) > 1 && args[1] == "serve" {
		stop, err := processprofile.StartFromEnv("gateway")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer stop()
	}
	return gatewayruntime.Run(args)
}
