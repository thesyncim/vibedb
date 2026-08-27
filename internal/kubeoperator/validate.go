package kubeoperator

import (
	"bytes"
	"errors"
)

// ErrManifest reports that a rendered Kubernetes test topology does not retain
// the bounded RF3, storage, DNS, probe, and disruption contract.
var ErrManifest = errors.New("kubeoperator: invalid rendered manifest")

const MaxManifestBytes = 2 << 20

// ValidateRendered checks the complete renderer output without accepting a
// general YAML surface. Kubernetes remains the schema authority; this gate
// protects the VibeDB-specific invariants that generic YAML validation cannot.
func ValidateRendered(raw []byte) error {
	if len(raw) == 0 || len(raw) > MaxManifestBytes || bytes.Contains(raw, []byte("\r")) {
		return ErrManifest
	}
	documents := bytes.Split(raw, []byte("\n---\n"))
	if len(documents) != 16 || count(raw, "kind: Namespace\n") != 1 ||
		count(raw, "kind: Service\n") != 6 || count(raw, "kind: PodDisruptionBudget\n") != 4 ||
		count(raw, "kind: StatefulSet\n") != 5 {
		return ErrManifest
	}
	for _, role := range rf3Roles {
		for _, required := range []string{
			"name: vibedb-" + role + "-peer", "name: vibedb-" + role + "\n",
			"app.kubernetes.io/component: " + role,
			"vibedb.io/raft-role: " + role,
			"vibedb-" + role + "-0.vibedb-" + role + "-peer:7511=",
			"vibedb-" + role + "-1.vibedb-" + role + "-peer:7511=",
			"vibedb-" + role + "-2.vibedb-" + role + "-peer:7511=",
		} {
			if !bytes.Contains(raw, []byte(required)) {
				return ErrManifest
			}
		}
	}
	checks := []struct {
		text string
		want int
	}{
		{"  replicas: 3\n", 3}, {"  replicas: 1\n", 1}, {"  replicas: 0\n", 1},
		{"  maxUnavailable: 1\n", 3}, {"  minAvailable: 1\n", 1},
		{"persistentVolumeClaimRetentionPolicy: {whenDeleted: Retain, whenScaled: Retain}", 5},
		{"whenUnsatisfiable: DoNotSchedule", 3}, {"topologyKey: kubernetes.io/hostname", 6},
		{"requiredDuringSchedulingIgnoredDuringExecution:", 3},
		{"startupProbe:", 4}, {"readinessProbe:", 4}, {"livenessProbe:", 4},
		{"automountServiceAccountToken: false", 5}, {"enableServiceLinks: false", 5},
		{"runAsNonRoot: true", 5}, {"seccompProfile: {type: RuntimeDefault}", 5},
		{"volumeClaimTemplates:", 5}, {"clusterIP: None", 5},
	}
	for _, check := range checks {
		if count(raw, check.text) != check.want {
			return ErrManifest
		}
	}
	for _, forbidden := range []string{"emptyDir:", "hostPath:", "leader: true", "leader-election", "image: latest"} {
		if bytes.Contains(raw, []byte(forbidden)) {
			return ErrManifest
		}
	}
	return nil
}

func count(raw []byte, text string) int { return bytes.Count(raw, []byte(text)) }
