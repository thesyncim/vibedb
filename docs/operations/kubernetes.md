# Kubernetes RF3 qualification lane

[Documentation](../README.md) / [Operations](README.md) / Kind qualification

`deploy/kubernetes/qualify-kind.sh` creates a disposable 4-node Kind cluster
(1 control plane, 3 workers, pinned K8s `v1.34.8`) and validates a fixed RF3
topology across rolling StatefulSet restarts. `vibedb-operator` is a manifest
renderer and bootstrap helper only—not a reconciling Kubernetes operator.

## Topology and checks

| Workload | Replicas | PVC | Role |
| --- | ---: | ---: | --- |
| `vibedb-catalog`, `vibedb-ledger`, `vibedb-data` | 3 each | `20Gi` | Standalone `serve-rf3` per group replica |
| `vibedb-gateway` | 1 | `1Gi` | Gateway + session journal |
| Learner template | 0 | — | Static scaffold |

The script verifies deterministic manifest render/validate, in-cluster DNS for
all 10 Pods, an authenticated sequenced write + 128 reads (p99 ≤1 s, max ≤5 s),
rolling restart of every StatefulSet, exact write replay, preserved PVC UIDs,
and per-process RSS/storage/WAL bounds. Note: this per-replica `serve-rf3`
layout differs from the multi-group `serve-node` physical-node layout.

## Run locally

Requires Linux, Go 1.27+, Docker, `kind` (`v0.32.0`), `kubectl`, `timeout`, and
`base64`:

```sh
go install sigs.k8s.io/kind@v0.32.0
evidence_dir="$(mktemp -d "$PWD/.artifacts/kubernetes-rf3.XXXXXXXX")"
VIBEDB_KUBE_EVIDENCE_DIR="$evidence_dir" ./deploy/kubernetes/qualify-kind.sh
```

Set `GOEXPERIMENT=nosimd` for portable builds. Delete the temporary work
directory containing disposable 7-day test PKI after inspection.

To render and validate manifests without Docker/Kind:

```sh
state_dir="$(mktemp -d)" && chmod 0700 "$state_dir"
./bin/vibedb-operator bootstrap -namespace vibedb-test -state-dir "$state_dir" > bootstrap.yaml
./bin/vibedb-operator render -namespace vibedb-test -image vibedb:kube-qualification \
  -bootstrap-state-dir "$state_dir" > topology.yaml
./bin/vibedb-operator validate -manifest topology.yaml
```

## Security and limitations

- Distroless non-root image (`UID/GID 65532`), dropped capabilities, seccomp
  `RuntimeDefault`, read-only Secrets/ConfigMaps, PDBs, and shard anti-affinity.
- Does **not** provide a reconciling controller, CPU/memory limits,
  NetworkPolicies, production PKI, mixed-build upgrades, or multi-zone failure
  qualification. Run in CI by the `kubernetes-rf3` job in `.github/workflows/ci.yml`.

## Source map

- [deploy/kubernetes/qualify-kind.sh](../../deploy/kubernetes/qualify-kind.sh) · [Dockerfile](../../deploy/kubernetes/Dockerfile) · [cmd/vibedb-operator/](../../cmd/vibedb-operator) · [internal/kubeoperator/render.go](../../internal/kubeoperator/render.go)
