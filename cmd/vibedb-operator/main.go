// Command vibedb-operator renders and prepares the deliberately small
// Kubernetes test lane. It does not elect leaders or mutate VibeDB topology;
// Raft and the replicated catalog retain those authorities.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/internal/kubeoperator"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: vibedb-operator render|prepare")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "render":
		err = render(os.Args[2:])
	case "prepare":
		err = prepare(os.Args[2:])
	default:
		err = errors.New("unknown command")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "vibedb-operator: %v\n", err)
		os.Exit(2)
	}
}

func render(arguments []string) error {
	flags := flag.NewFlagSet("render", flag.ContinueOnError)
	namespace := flags.String("namespace", "vibedb", "Kubernetes namespace")
	image := flags.String("image", "", "VibeDB image reference")
	nodes := flags.String("shard-node-ids", "", "three comma-separated TLS node IDs in StatefulSet ordinal order")
	manifestConfig := flags.String("shard-manifests", "vibedb-rf3-manifests", "ConfigMap containing prepare-{0,1,2}.vibejson")
	tlsSecret := flags.String("shard-tls", "vibedb-rf3-tls", "Secret containing shard TLS material and WAL key source")
	gatewayConfig := flags.String("gateway-config", "vibedb-gateway-config", "ConfigMap containing catalog, policy, and replica-control manifests")
	gatewayTLS := flags.String("gateway-tls", "vibedb-gateway-tls", "Secret containing gateway TLS material")
	storageClass := flags.String("storage-class", "", "optional StorageClass name")
	shardStorage := flags.String("shard-storage", "20Gi", "storage requested by each shard")
	gatewayStorage := flags.String("gateway-storage", "1Gi", "storage requested by the gateway journal")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	parts := strings.Split(*nodes, ",")
	if len(parts) != 3 {
		return errors.New("-shard-node-ids requires exactly three values")
	}
	return kubeoperator.Render(os.Stdout, kubeoperator.Config{Namespace: *namespace, Image: *image,
		ShardNodeIDs:      [3]string{parts[0], parts[1], parts[2]},
		ManifestConfigMap: *manifestConfig, TLSSecret: *tlsSecret,
		GatewayConfigMap: *gatewayConfig, GatewayTLSSecret: *gatewayTLS,
		StorageClass: *storageClass, ShardStorage: *shardStorage, GatewayStorage: *gatewayStorage})
}

func prepare(arguments []string) error {
	flags := flag.NewFlagSet("prepare", flag.ContinueOnError)
	hostname := flags.String("hostname", os.Getenv("HOSTNAME"), "StatefulSet pod hostname")
	manifestDirectory := flags.String("manifest-dir", "/bootstrap", "directory containing prepare-<ordinal>.vibejson")
	dataDirectory := flags.String("data-dir", "/var/lib/vibedb/member", "exact member root named by the preparation manifest")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	ordinal, err := podOrdinal(*hostname)
	if err != nil {
		return err
	}
	serve := filepath.Join(*dataDirectory, "serve-rf3.vibejson")
	if info, statErr := os.Stat(serve); statErr == nil && info.Mode().IsRegular() {
		return nil
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	manifest := filepath.Join(*manifestDirectory, "prepare-"+strconv.Itoa(ordinal)+".vibejson")
	command := exec.Command("vibedb-shard", "prepare-rf3", "-manifest", manifest)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err = command.Run(); err != nil {
		return err
	}
	info, err := os.Stat(serve)
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(err, errors.New("prepare-rf3 did not publish the expected member root"))
	}
	return nil
}

func podOrdinal(hostname string) (int, error) {
	cut := strings.LastIndexByte(hostname, '-')
	if cut <= 0 || cut == len(hostname)-1 {
		return 0, errors.New("hostname has no StatefulSet ordinal")
	}
	ordinal, err := strconv.Atoi(hostname[cut+1:])
	if err != nil || ordinal < 0 || ordinal > 2 {
		return 0, errors.New("hostname ordinal is outside the RF3 roster")
	}
	return ordinal, nil
}
