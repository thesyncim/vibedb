// Command vibedb-operator renders and prepares the deliberately small
// Kubernetes test lane. It does not elect leaders or mutate VibeDB topology;
// Raft and the replicated catalog retain those authorities.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/internal/kubeoperator"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: vibedb-operator adopt-restore|bootstrap|render|prepare|prepare-gateway|restore-group|validate")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "adopt-restore":
		err = adoptRestore(os.Args[2:])
	case "bootstrap":
		err = bootstrap(os.Args[2:])
	case "render":
		err = render(os.Args[2:])
	case "prepare":
		err = prepare(os.Args[2:])
	case "prepare-gateway":
		err = prepareGateway(os.Args[2:])
	case "restore-group":
		err = restoreGroup(os.Args[2:])
	case "validate":
		err = validate(os.Args[2:])
	default:
		err = errors.New("unknown command")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "vibedb-operator: %v\n", err)
		os.Exit(2)
	}
}

func prepareGateway(arguments []string) error {
	flags := flag.NewFlagSet("prepare-gateway", flag.ContinueOnError)
	source := flags.String("catalog-source", "", "read-only projected generation-one catalog")
	target := flags.String("catalog-target", "", "regular immutable generation-one catalog on gateway PVC")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("prepare-gateway accepts no positional arguments")
	}
	return kubeoperator.PrepareGatewaySeed(*source, *target)
}

func adoptRestore(arguments []string) error {
	flags := flag.NewFlagSet("adopt-restore", flag.ContinueOnError)
	manifest := flags.String("manifest", "", "canonical per-node target preparation manifest")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || *manifest == "" || !filepath.IsAbs(*manifest) || filepath.Clean(*manifest) != *manifest {
		return errors.New("adopt-restore requires one canonical absolute -manifest")
	}
	command := exec.Command("vibedb-shard", "adopt-restored-rf3", "-manifest", *manifest)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command.Run()
}

func bootstrap(arguments []string) error {
	flags := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	namespace := flags.String("namespace", "vibedb", "Kubernetes namespace")
	stateDirectory := flags.String("state-dir", "", "absolute private directory retaining the generated authority")
	manifestConfig := flags.String("shard-manifests", "vibedb-rf3-manifests", "shard bootstrap ConfigMap")
	tlsSecret := flags.String("shard-tls", "vibedb-rf3-tls", "shard TLS and WAL Secret")
	gatewayConfig := flags.String("gateway-config", "vibedb-gateway-config", "gateway ConfigMap")
	gatewayTLS := flags.String("gateway-tls", "vibedb-gateway-tls", "gateway TLS Secret")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stateDirectory == "" {
		return errors.New("bootstrap requires -state-dir and accepts no positional arguments")
	}
	result, err := kubeoperator.Bootstrap(os.Stdout, kubeoperator.BootstrapConfig{
		Namespace: *namespace, StateDirectory: *stateDirectory,
		ManifestConfigMap: *manifestConfig, TLSSecret: *tlsSecret,
		GatewayConfigMap: *gatewayConfig, GatewayTLSSecret: *gatewayTLS,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "shard-node-ids=%s\ngateway-node=%s\nclient-node=%s\n",
		strings.Join(result.ShardNodeIDs[:], ","), result.GatewayNodeID, result.ClientNodeID)
	return nil
}

func validate(arguments []string) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	path := flags.String("manifest", "", "rendered Kubernetes manifest, or - for stdin")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || *path == "" {
		return errors.New("validate requires -manifest")
	}
	var reader io.Reader = os.Stdin
	var file *os.File
	var err error
	if *path != "-" {
		file, err = os.Open(*path)
		if err != nil {
			return err
		}
		defer file.Close()
		reader = file
	}
	raw, err := io.ReadAll(io.LimitReader(reader, kubeoperator.MaxManifestBytes+1))
	if err != nil {
		return err
	}
	return kubeoperator.ValidateRendered(raw)
}

func render(arguments []string) error {
	flags := flag.NewFlagSet("render", flag.ContinueOnError)
	namespace := flags.String("namespace", "vibedb", "Kubernetes namespace")
	image := flags.String("image", "", "VibeDB image reference")
	nodes := flags.String("shard-node-ids", "", "nine comma-separated TLS node IDs: catalog, ledger, then data ordinal order")
	bootstrapState := flags.String("bootstrap-state-dir", "", "absolute private bootstrap state directory supplying authenticated node IDs")
	manifestConfig := flags.String("shard-manifests", "vibedb-rf3-manifests", "ConfigMap containing catalog-, ledger-, and data-{0,1,2}.vibejson")
	tlsSecret := flags.String("shard-tls", "vibedb-rf3-tls", "Secret containing shard TLS material and WAL key source")
	gatewayConfig := flags.String("gateway-config", "vibedb-gateway-config", "ConfigMap containing catalog, policy, and replica-control manifests")
	gatewayTLS := flags.String("gateway-tls", "vibedb-gateway-tls", "Secret containing gateway TLS material")
	storageClass := flags.String("storage-class", "", "optional StorageClass name")
	shardStorage := flags.String("shard-storage", "20Gi", "storage requested by each shard")
	gatewayStorage := flags.String("gateway-storage", "1Gi", "storage requested by the gateway journal")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("render accepts no positional arguments")
	}
	var shardNodeIDs [9]string
	if *bootstrapState != "" {
		if *nodes != "" {
			return errors.New("-bootstrap-state-dir and -shard-node-ids are mutually exclusive")
		}
		result, err := kubeoperator.LoadBootstrapState(*bootstrapState)
		if err != nil {
			return err
		}
		shardNodeIDs = result.ShardNodeIDs
	} else {
		parts := strings.Split(*nodes, ",")
		if len(parts) == len(shardNodeIDs) {
			shardNodeIDs = [9]string(parts)
		}
	}
	if shardNodeIDs == ([9]string{}) {
		return errors.New("-shard-node-ids requires exactly nine values")
	}
	return kubeoperator.Render(os.Stdout, kubeoperator.Config{Namespace: *namespace, Image: *image,
		ShardNodeIDs:      shardNodeIDs,
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
	if flags.NArg() != 0 || !validOperatorDirectory(*manifestDirectory) ||
		!validOperatorDirectory(*dataDirectory) {
		return errors.New("prepare requires canonical absolute manifest and data directories")
	}
	role, ordinal, err := podIdentity(*hostname)
	if err != nil {
		return err
	}
	serve := filepath.Join(*dataDirectory, "serve-rf3.vibejson")
	if info, statErr := os.Lstat(serve); statErr == nil && info.Mode().IsRegular() {
		return nil
	} else if statErr == nil {
		return errors.New("existing serve manifest is not a regular file")
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	manifest := filepath.Join(*manifestDirectory, role+"-"+strconv.Itoa(ordinal)+".vibejson")
	command := exec.Command("vibedb-shard", "prepare-rf3", "-manifest", manifest)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err = command.Run(); err != nil {
		return err
	}
	info, err := os.Lstat(serve)
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(err, errors.New("prepare-rf3 did not publish the expected member root"))
	}
	return nil
}

func validOperatorDirectory(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator)
}

func podOrdinal(hostname string) (int, error) {
	_, ordinal, err := podIdentity(hostname)
	return ordinal, err
}

func podIdentity(hostname string) (string, int, error) {
	cut := strings.LastIndexByte(hostname, '-')
	if cut <= 0 || cut == len(hostname)-1 {
		return "", 0, errors.New("hostname has no StatefulSet ordinal")
	}
	role := strings.TrimPrefix(hostname[:cut], "vibedb-")
	if role != "catalog" && role != "ledger" && role != "data" {
		return "", 0, errors.New("hostname is not a VibeDB RF3 role StatefulSet pod")
	}
	ordinal, err := strconv.Atoi(hostname[cut+1:])
	if err != nil || ordinal < 0 || ordinal > 2 || strconv.Itoa(ordinal) != hostname[cut+1:] {
		return "", 0, errors.New("hostname ordinal is outside the RF3 roster")
	}
	return role, ordinal, nil
}
