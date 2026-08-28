package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibejson"
)

const maxGatewayRestoreManifestBytes = 4 << 20

type gatewayRestoreManifest struct {
	Format           uint16                   `json:"format"`
	Operation        string                   `json:"operation"`
	SchemaSet        string                   `json:"schema_set"`
	StagingRoot      string                   `json:"staging_root"`
	ActivationRoot   string                   `json:"activation_root"`
	TargetCatalog    string                   `json:"target_catalog"`
	Policy           string                   `json:"policy"`
	TLS              gatewayRestoreTLS        `json:"tls"`
	Sessions         [2]gatewayRestoreSession `json:"sessions"`
	Groups           []gatewayRestoreGroup    `json:"groups"`
	Repository       gatewayRestoreRepository `json:"repository"`
	TimeoutMS        uint64                   `json:"timeout_ms"`
	AttemptTimeoutMS uint64                   `json:"attempt_timeout_ms"`
	SessionLeaseMS   uint64                   `json:"session_lease_ms"`
	Attempts         int                      `json:"attempts"`
	MaxConnections   int                      `json:"max_connections"`
}

type gatewayRestoreTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type gatewayRestoreSession struct {
	ClientID  string `json:"client_id"`
	RetryHome string `json:"retry_home"`
	Journal   string `json:"journal"`
}

type gatewayRestoreGroup struct {
	Ordinal          uint32    `json:"ordinal"`
	Root             string    `json:"root"`
	ControlAddresses [3]string `json:"control_addresses"`
}

type gatewayRestoreRepository struct {
	MaxArtifacts     int    `json:"max_artifacts"`
	MaxArtifactBytes uint64 `json:"max_artifact_bytes"`
	MaxDiskBytes     uint64 `json:"max_disk_bytes"`
}

func loadGatewayRestoreManifest(path string) (gatewayRestoreManifest, error) {
	raw, err := readGatewayRestoreInput(path, maxGatewayRestoreManifestBytes)
	if err != nil {
		return gatewayRestoreManifest{}, err
	}
	return parseGatewayRestoreManifest(raw)
}

func parseGatewayRestoreManifest(raw []byte) (gatewayRestoreManifest, error) {
	var manifest gatewayRestoreManifest
	if len(raw) == 0 || len(raw) > maxGatewayRestoreManifestBytes {
		return manifest, gateway.ErrRestoreActivation
	}
	if err := vibejson.Unmarshal(raw, &manifest); err != nil {
		return manifest, errors.Join(gateway.ErrRestoreActivation, err)
	}
	canonical, err := vibejson.Marshal(&manifest)
	if err != nil || !bytes.Equal(canonical, raw) || manifest.Format != 1 ||
		len(manifest.Groups) == 0 || len(manifest.Groups) > clusterbackup.AbsoluteMaxGroupCuts ||
		manifest.TimeoutMS == 0 || manifest.TimeoutMS > 24*60*60*1000 ||
		manifest.AttemptTimeoutMS == 0 || manifest.AttemptTimeoutMS > manifest.TimeoutMS ||
		manifest.SessionLeaseMS <= manifest.AttemptTimeoutMS || manifest.SessionLeaseMS > 24*60*60*1000 ||
		manifest.Attempts <= 0 || manifest.Attempts > 64 || manifest.MaxConnections < 2 || manifest.MaxConnections > 4096 ||
		manifest.Repository.MaxArtifacts < len(manifest.Groups) || manifest.Repository.MaxArtifacts > clusterbackup.AbsoluteMaxGroupCuts ||
		manifest.Repository.MaxArtifactBytes == 0 || manifest.Repository.MaxArtifactBytes > uint64(^uint64(0)>>1) ||
		manifest.Repository.MaxDiskBytes < manifest.Repository.MaxArtifactBytes || manifest.Repository.MaxDiskBytes > uint64(^uint64(0)>>1) ||
		manifest.TLS.IdentityOID == "" {
		return gatewayRestoreManifest{}, errors.Join(gateway.ErrRestoreActivation, err)
	}
	for _, path := range []string{manifest.Operation, manifest.SchemaSet, manifest.StagingRoot, manifest.ActivationRoot,
		manifest.TargetCatalog, manifest.Policy, manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots} {
		if !gatewayRestorePath(path) {
			return gatewayRestoreManifest{}, gateway.ErrRestoreActivation
		}
	}
	if manifest.ActivationRoot == manifest.StagingRoot {
		return gatewayRestoreManifest{}, gateway.ErrRestoreActivation
	}
	for index, session := range manifest.Sessions {
		var client [16]byte
		var retry [8]byte
		if decodeFixedHex(session.ClientID, client[:]) != nil || decodeFixedHex(session.RetryHome, retry[:]) != nil ||
			client == ([16]byte{}) || retry == ([8]byte{}) || !gatewayRestorePath(session.Journal) {
			return gatewayRestoreManifest{}, gateway.ErrRestoreActivation
		}
		if index != 0 && (session.ClientID == manifest.Sessions[0].ClientID || session.Journal == manifest.Sessions[0].Journal) {
			return gatewayRestoreManifest{}, gateway.ErrRestoreActivation
		}
	}
	for index, group := range manifest.Groups {
		if group.Ordinal != uint32(index) || !gatewayRestorePath(group.Root) || group.Root == manifest.ActivationRoot || group.Root == manifest.StagingRoot {
			return gatewayRestoreManifest{}, gateway.ErrRestoreActivation
		}
		for _, address := range group.ControlAddresses {
			host, port, splitErr := net.SplitHostPort(address)
			number, portErr := strconv.ParseUint(port, 10, 16)
			if splitErr != nil || portErr != nil || host == "" || number == 0 {
				return gatewayRestoreManifest{}, gateway.ErrRestoreActivation
			}
		}
	}
	return manifest, nil
}

func gatewayRestorePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator)
}

func readGatewayRestoreInput(path string, maximum int64) ([]byte, error) {
	if !gatewayRestorePath(path) || maximum <= 0 {
		return nil, gateway.ErrRestoreActivation
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, gateway.ErrRestoreActivation
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maximum+1))
	err = errors.Join(readErr, file.Close())
	if err != nil || len(raw) == 0 || int64(len(raw)) > maximum {
		return nil, errors.Join(gateway.ErrRestoreActivation, err)
	}
	return raw, nil
}
