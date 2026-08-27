package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

const maxGatewaySchemaRolloutManifestBytes = 4 << 20

type gatewaySchemaRolloutManifest struct {
	Operation     string                              `json:"operation"`
	TargetCatalog string                              `json:"target_catalog"`
	Replicas      []gatewaySchemaRolloutReplicaConfig `json:"replicas"`
}

type gatewaySchemaRolloutReplicaConfig struct {
	Node          string `json:"node"`
	Member        uint64 `json:"member"`
	ApplyContract string `json:"apply_contract"`
	Bundle        string `json:"bundle"`
}

func loadGatewaySchemaRolloutManifest(path string) (gatewaySchemaRolloutManifest, error) {
	var result gatewaySchemaRolloutManifest
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxGatewaySchemaRolloutManifestBytes+1))
	err = errors.Join(readErr, file.Close())
	if err != nil || len(raw) == 0 || len(raw) > maxGatewaySchemaRolloutManifestBytes {
		return result, errors.Join(err, gateway.ErrSchemaRollout)
	}
	canonical, err := vibejson.AppendCanonicalize(nil, raw)
	decodeErr := vibejson.Unmarshal(raw, &result)
	if err != nil || decodeErr != nil || !bytes.Equal(canonical, raw) ||
		result.Operation == "" || result.TargetCatalog == "" || len(result.Replicas) == 0 ||
		len(result.Replicas) > 4096 {
		clear(raw)
		clear(canonical)
		return gatewaySchemaRolloutManifest{}, errors.Join(err, decodeErr, gateway.ErrSchemaRollout)
	}
	clear(raw)
	clear(canonical)
	return result, nil
}

func buildGatewaySchemaRolloutPlans(
	manifest gatewaySchemaRolloutManifest, base, target *gateway.Snapshot,
) ([32]byte, []gateway.SchemaRolloutReplicaPlan, error) {
	var operation [32]byte
	if decodeFixedHex(manifest.Operation, operation[:]) != nil {
		return operation, nil, gateway.ErrSchemaRollout
	}
	plans := make([]gateway.SchemaRolloutReplicaPlan, len(manifest.Replicas))
	for index, replica := range manifest.Replicas {
		var node rafttransport.NodeID
		var contract [32]byte
		if decodeFixedHex(replica.Node, node[:]) != nil ||
			decodeFixedHex(replica.ApplyContract, contract[:]) != nil ||
			replica.Member == 0 || replica.Bundle == "" {
			return operation, nil, gateway.ErrSchemaRollout
		}
		file, err := os.Open(replica.Bundle)
		if err != nil {
			return operation, nil, err
		}
		bundle, readErr := io.ReadAll(io.LimitReader(file, schemainstall.AbsoluteMaxBundleBytes+1))
		err = errors.Join(readErr, file.Close())
		if err != nil || len(bundle) == 0 || len(bundle) > schemainstall.AbsoluteMaxBundleBytes {
			clear(bundle)
			return operation, nil, errors.Join(err, gateway.ErrSchemaRollout)
		}
		if _, err = sqldriver.ValidateReplicatedSchemaCatalogImage(bundle); err != nil {
			clear(bundle)
			return operation, nil, err
		}
		plans[index], err = gateway.NewSchemaRolloutReplicaPlan(
			operation, base, target, node, replica.Member, contract, bundle,
		)
		clear(bundle)
		if err != nil {
			return operation, nil, err
		}
	}
	return operation, plans, nil
}

func executeGatewaySchemaRollout(
	ctx context.Context, manifestPath string, authority *gateway.ReplicatedCatalogAuthority,
	opener *gatewayShardControlOpener, readDeadline, writeDeadline rafttransport.DeadlineFunc,
	maxConcurrent int,
) (gateway.SchemaRolloutResult, error) {
	if ctx == nil || manifestPath == "" || authority == nil || opener == nil {
		return gateway.SchemaRolloutResult{}, gateway.ErrSchemaRollout
	}
	manifest, err := loadGatewaySchemaRolloutManifest(manifestPath)
	if err != nil {
		return gateway.SchemaRolloutResult{}, err
	}
	base, err := authority.Read(ctx)
	if err != nil {
		return gateway.SchemaRolloutResult{}, err
	}
	target, err := gateway.LoadSnapshot(manifest.TargetCatalog)
	if err != nil {
		return gateway.SchemaRolloutResult{}, err
	}
	operation, plans, err := buildGatewaySchemaRolloutPlans(manifest, base, target)
	if err != nil {
		return gateway.SchemaRolloutResult{}, err
	}
	client, err := schemainstall.NewClient(schemainstall.ClientOptions{
		Opener: opener, ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
	})
	if err != nil {
		return gateway.SchemaRolloutResult{}, err
	}
	controller, err := gateway.NewSchemaRolloutController(gateway.SchemaRolloutControllerOptions{
		Authority: authority, Client: client, MaxConcurrent: maxConcurrent,
	})
	if err != nil {
		return gateway.SchemaRolloutResult{}, err
	}
	return controller.Execute(ctx, operation, target, plans)
}

func printGatewaySchemaRolloutResult(result gateway.SchemaRolloutResult, elapsed time.Duration) {
	fmt.Fprintf(os.Stdout,
		"schema rollout complete: catalog-generation=%d operation-revision=%d elapsed=%s\n",
		result.Authorization.TargetCatalogGeneration, result.Record.Revision, elapsed)
}
