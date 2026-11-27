package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/thesyncim/vibedb/internal/clusterversion"
)

type clusterVersionReport struct {
	Current          string   `json:"current"`
	MinSupported     string   `json:"min_supported"`
	MaxSupported     string   `json:"max_supported"`
	Features         []string `json:"features"`
	Compatible       *bool    `json:"compatible,omitempty"`
	NegotiatedMin    string   `json:"negotiated_min,omitempty"`
	NegotiatedMax    string   `json:"negotiated_max,omitempty"`
	NegotiatedBinary string   `json:"negotiated_min_binary,omitempty"`
	Error            string   `json:"error,omitempty"`
}

func runClusterVersion(args []string) int {
	return runClusterVersionTo(args, os.Stdout, os.Stderr)
}

func runClusterVersionTo(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("cluster version", flag.ContinueOnError)
	fs.SetOutput(stderr)
	asJSON := fs.Bool("json", false, "emit machine-readable JSON output")
	checkPeers := fs.String("check-peers", "", "comma-separated peer versions (e.g. 1.0,1.1 or 1.0..1.1) to validate rolling-upgrade compatibility with this binary")
	activeFlag := fs.String("active", "", "optional current cluster active version (e.g. 1.0) to validate against the negotiated window")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	local := clusterversion.LocalNodeVersion()
	report := clusterVersionReport{
		Current:      local.BinaryVersion.String(),
		MinSupported: local.MinSupported.String(),
		MaxSupported: local.MaxSupported.String(),
		Features:     enabledFeatureNames(local.BinaryVersion),
	}

	exitCode := 0
	if strings.TrimSpace(*checkPeers) != "" {
		nodes := []clusterversion.NodeVersion{local}
		for _, tok := range strings.Split(*checkPeers, ",") {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}
			nv, err := parsePeerVersionSpec(tok)
			if err != nil {
				compat := false
				report.Compatible = &compat
				report.Error = err.Error()
				exitCode = 1
				break
			}
			nodes = append(nodes, nv)
		}
		if exitCode == 0 {
			win, err := clusterversion.Negotiate(nodes)
			if err != nil {
				compat := false
				report.Compatible = &compat
				report.Error = err.Error()
				exitCode = 1
			} else {
				if strings.TrimSpace(*activeFlag) != "" {
					active, perr := clusterversion.ParseVersion(strings.TrimSpace(*activeFlag))
					if perr != nil {
						compat := false
						report.Compatible = &compat
						report.Error = perr.Error()
						exitCode = 1
					} else if verr := clusterversion.ValidateAdvance(active, active, nodes); verr != nil {
						compat := false
						report.Compatible = &compat
						report.Error = verr.Error()
						exitCode = 1
					}
				}
				if exitCode == 0 {
					compat := true
					report.Compatible = &compat
					report.NegotiatedMin = win.MinCommon.String()
					report.NegotiatedMax = win.MaxCommon.String()
					report.NegotiatedBinary = win.MinBinary.String()
				}
			}
		}
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
		return exitCode
	}

	fmt.Fprintf(stdout, "current:       %s\n", report.Current)
	fmt.Fprintf(stdout, "min_supported: %s\n", report.MinSupported)
	fmt.Fprintf(stdout, "max_supported: %s\n", report.MaxSupported)
	fmt.Fprintf(stdout, "features:      %s\n", strings.Join(report.Features, ", "))
	if report.Compatible != nil {
		if *report.Compatible {
			fmt.Fprintf(stdout, "compatible:    true (common window %s..%s, min binary %s)\n",
				report.NegotiatedMin, report.NegotiatedMax, report.NegotiatedBinary)
		} else {
			fmt.Fprintf(stderr, "compatible:    false (%s)\n", report.Error)
		}
	}
	return exitCode
}

func parsePeerVersionSpec(spec string) (clusterversion.NodeVersion, error) {
	if minStr, maxStr, ok := strings.Cut(spec, ".."); ok {
		minV, err := clusterversion.ParseVersion(strings.TrimSpace(minStr))
		if err != nil {
			return clusterversion.NodeVersion{}, err
		}
		maxV, err := clusterversion.ParseVersion(strings.TrimSpace(maxStr))
		if err != nil {
			return clusterversion.NodeVersion{}, err
		}
		nv := clusterversion.NodeVersion{
			MinSupported:  minV,
			MaxSupported:  maxV,
			BinaryVersion: maxV,
		}
		if !nv.Valid() {
			return clusterversion.NodeVersion{}, fmt.Errorf("%w: invalid peer window %q", clusterversion.ErrInvalidVersion, spec)
		}
		return nv, nil
	}
	v, err := clusterversion.ParseVersion(spec)
	if err != nil {
		return clusterversion.NodeVersion{}, err
	}
	return clusterversion.NodeVersion{
		MinSupported:  v,
		MaxSupported:  v,
		BinaryVersion: v,
	}, nil
}

func enabledFeatureNames(active clusterversion.Version) []string {
	type namedFeature struct {
		name string
		feat clusterversion.Feature
	}
	all := []namedFeature{
		{"base_rf3", clusterversion.FeatureBaseRF3},
		{"tin_persisted_sidecar", clusterversion.FeatureTinPersistedSidecar},
		{"gateway_latency_histograms", clusterversion.FeatureGatewayLatencyHistograms},
		{"remote_scram_pgwire", clusterversion.FeatureRemoteSCRAMPGWire},
		{"global_index_split_prune", clusterversion.FeatureGlobalIndexSplitPrune},
	}
	out := make([]string, 0, len(all))
	for _, nf := range all {
		if nf.feat.Enabled(active) {
			out = append(out, nf.name)
		}
	}
	return out
}
