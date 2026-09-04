package main

import (
	"flag"
	"fmt"
	"os"
)

// SQL is prepared without opening the live node log. The fsynced serving
// manifest authorizes the node owner to validate and register its exact
// descriptor/bootstrap before it adopts this group.
func runPrepareNodeGroupRF3(args []string) int {
	flags := flag.NewFlagSet("prepare-node-group-rf3", flag.ContinueOnError)
	path := flags.String("manifest", "", "canonical RF3 group preparation manifest")
	if err := flags.Parse(args); err != nil || *path == "" || flags.NArg() != 0 {
		return 2
	}
	input, err := loadPrepareRF3Manifest(*path)
	if err == nil && input.DevelopmentOnly {
		err = errPrepareRF3
	}
	if err == nil {
		err = provisionRF3MemberInto(input, input.Root, nil, true)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error prepare node-log RF3 group: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "RF3 node-log group prepared root=%q\n", input.Root)
	return 0
}
