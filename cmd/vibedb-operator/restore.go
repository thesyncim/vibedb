package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/kubeoperator"
	"github.com/thesyncim/vibejson"
)

const restoreOperationMaxBytes = 64 << 20

type restoreGroupOutput struct {
	TargetGroup          string    `json:"target_group"`
	ArtifactManifest     string    `json:"artifact_manifest"`
	SanitizedImageDigest string    `json:"sanitized_image_digest"`
	GenesisProof         string    `json:"genesis_proof"`
	ReplicaRoots         [3]string `json:"replica_roots"`
	SnapshotIndex        uint64    `json:"snapshot_index"`
	SnapshotTerm         uint64    `json:"snapshot_term"`
}

func restoreGroup(arguments []string) error {
	flags := flag.NewFlagSet("restore-group", flag.ContinueOnError)
	root := flags.String("root", "", "absolute private destination restore root")
	templatePath := flags.String("template", "", "canonical target prepare-*.vibejson schema template")
	operationPath := flags.String("operation", "", "authenticated binary restore operation")
	artifactPath := flags.String("artifact", "", "certified source snapshot artifact")
	ordinal := flags.Uint("group-ordinal", 0, "source and target group ordinal")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || !validOperatorDirectory(*root) || *templatePath == "" ||
		*operationPath == "" || *artifactPath == "" || uint64(*ordinal) > uint64(^uint32(0)) {
		return errors.New("restore-group requires -root, -template, -operation, and -artifact")
	}
	if err := os.MkdirAll(*root, 0o700); err != nil {
		return err
	}
	template, err := readBoundedRestoreInput(*templatePath, kubeoperator.RestoreTemplateMaxBytes)
	if err != nil {
		return err
	}
	operationRaw, err := readBoundedRestoreInput(*operationPath, restoreOperationMaxBytes)
	if err != nil {
		return err
	}
	operation, err := clusterrestore.OpenOperation(operationRaw)
	if err != nil {
		return err
	}
	artifact, err := os.Open(*artifactPath)
	if err != nil {
		return err
	}
	result, restoreErr := kubeoperator.RestoreGroup(context.Background(), kubeoperator.RestoreGroupConfig{
		Root: *root, Template: template, Operation: operation, Ordinal: uint32(*ordinal), Artifact: artifact,
	})
	closeErr := artifact.Close()
	if restoreErr != nil || closeErr != nil {
		return errors.Join(restoreErr, closeErr)
	}
	witness := result.Witness
	output := restoreGroupOutput{
		TargetGroup:          hex.EncodeToString(witness.TargetGroup[:]),
		ArtifactManifest:     hex.EncodeToString(witness.ArtifactManifest[:]),
		SanitizedImageDigest: hex.EncodeToString(witness.SanitizedImageDigest[:]),
		GenesisProof:         hex.EncodeToString(witness.GenesisProof[:]),
		SnapshotIndex:        witness.SnapshotIndex, SnapshotTerm: witness.SnapshotTerm,
	}
	for index := range output.ReplicaRoots {
		output.ReplicaRoots[index] = hex.EncodeToString(witness.ReplicaRoots[index][:])
	}
	raw, err := vibejson.Marshal(&output)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(raw))
	return err
}

func readBoundedRestoreInput(path string, maximum int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || maximum <= 0 {
		return nil, errors.New("restore input path must be canonical and absolute")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximum+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || int64(len(raw)) == 0 || int64(len(raw)) > maximum {
		return nil, errors.Join(readErr, closeErr, errors.New("restore input is empty or exceeds its bound"))
	}
	return raw, nil
}
