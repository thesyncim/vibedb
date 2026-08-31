# Kubernetes RF3 qualification lane

> [!CAUTION]
> **Development and qualification only.** VibeDB is under active development.
> The image, generated manifests, credentials, wire and disk formats, and
> qualification procedure may break at any commit. This is not a production
> deployment, and green qualification does not establish production readiness.

This lane creates a disposable four-node Kind cluster—one control plane and three workers—then exercises a fixed RF3 topology across restart. `vibedb-operator` is only a manifest renderer and init helper. It is not a Kubernetes controller or reconciler.

## What the lane checks

- deterministic rendering and validation of the fixed topology
- startup of catalog, request-ledger, and data RF3 groups
- gateway startup and in-cluster DNS for all ten serving Pods
- an authenticated, sequenced write and exact read visibility
- restart of every catalog, ledger, data, and gateway StatefulSet
- replay and visibility of the acknowledged request after restart
- preservation of all ten PVC identities
- per-process RSS, apparent durable bytes, and WAL bounds

It does not test a mixed-build upgrade, multi-zone failure, backup policy, production PKI, long-running load, autoscaling, ingress, or an external managed Kubernetes service.

## Prerequisites

The qualified CI environment is Linux. Run from the repository root; the script records `HEAD` but does not prove that the worktree is clean. It requires:

- Go 1.26
- Git
- Docker with permission to build and load images
- Kind; CI installs `sigs.k8s.io/kind@v0.32.0`
- `kubectl`
- GNU `timeout`
- `base64`
- enough local resources for one control-plane node, three worker nodes, ten Pods, and ten PVCs

The Kind node image is Kubernetes `v1.34.8` pinned by SHA-256 in `deploy/kubernetes/kind-3-worker.yaml`.

## Run it

Choose a new evidence directory for each run. The script refuses to replace an existing Kind cluster named `vibedb-qualification`.

```bash
go install sigs.k8s.io/kind@v0.32.0

mkdir -p "$PWD/.artifacts"
evidence_dir="$(mktemp -d "$PWD/.artifacts/kubernetes-rf3.XXXXXXXX")"
VIBEDB_KUBE_EVIDENCE_DIR="$evidence_dir" \
  ./deploy/kubernetes/qualify-kind.sh
```

Success ends with:

```text
Kubernetes RF3 qualification passed; evidence=<directory>
```

Normal exit and handled failure cleanup delete the Kind cluster that the script created. An uncatchable termination can bypass that cleanup. The script does not delete its private `${RUNNER_TEMP:-/tmp}/vibedb-kube-rf3.XXXXXXXX` work directory, the local `vibedb:kube-qualification` Docker image, or the evidence directory. The temporary directory contains generated Secret YAML, CA and leaf private keys, WAL/ACK material, and extracted client credentials; treat it as sensitive test material and remove it deliberately after inspection.

The evidence path is created with `mkdir -p` rather than required-new or cleared. Reusing a path can mix stale `failed-*` files with a later run, so use a fresh directory when evidence provenance matters.

## Fixed topology

| Workload | Replicas | PVC per replica | Purpose |
| --- | ---: | ---: | --- |
| `vibedb-catalog` StatefulSet | 3 | `20Gi` | replicated catalog group |
| `vibedb-ledger` StatefulSet | 3 | `20Gi` | replicated durable request ledger |
| `vibedb-data` StatefulSet | 3 | `20Gi` | replicated data group |
| `vibedb-gateway` StatefulSet | 1 | `1Gi` | gateway session journal |
| learner template | 0 | none while scaled to zero | fixed qualification scaffold, not autoscaling |

The empty `storageClassName` default uses the cluster default. The application image is built locally from `deploy/kubernetes/Dockerfile`, loaded into Kind, and referenced by the mutable test tag `vibedb:kube-qualification`.

## Exact sequence

1. Refuse an existing qualification cluster and record the source revision and tool versions.
2. Build `vibedb-operator` and `vibedb-kube-qualify` for the host.
3. Generate seven-day disposable P-256 test PKI, policies, WAL keys, and the durable ACK key in a private bootstrap directory.
4. Render and validate bootstrap and topology YAML.
5. Build the non-root container image.
6. Create the one-control-plane/three-worker Kind cluster and load the image.
7. Apply the generated resources and wait up to ten minutes for each StatefulSet rollout.
8. Resolve nine shard Pod FQDNs and the gateway FQDN from inside the gateway Pod.
9. Require exactly ten PVCs and record their names and UIDs.
10. Forward gateway port `7400` to host port `17400`, perform one durable write, and collect 128 read samples with p99 at most one second and maximum latency at most five seconds.
11. Restart catalog, ledger, data, and gateway StatefulSets in turn, then replay the same request and repeat the read samples.
12. Compare PVC identities, enforce process resource ceilings, and record Pod and event state.

The restart loop uses StatefulSet rollout readiness to avoid voluntarily removing a whole RF3 quorum. This is a bounded restart test, not proof against arbitrary correlated failures.

## Evidence

| File | Meaning |
| --- | --- |
| `revision.txt`, `go-version.txt`, `kind-version.txt`, `kubectl-version.txt` | candidate and tool identity |
| `dns.vibejson` | ten successfully resolved service names |
| `before-restart.vibejson` | durable terminal result and latency sample before restart |
| `after-restart.vibejson` | exact replay, recovered visibility, and latency sample after restart |
| `pvc-before.tsv`, `pvc-after.tsv` | PVC name/UID comparison |
| `vibedb-*.vibejson` | per-process RSS, apparent storage, WAL bytes, and file count |
| `pods.txt`, `events.txt` | final Kubernetes state |
| `port-forward.log` | local forwarding diagnostics |
| `failed-*.txt` | bounded Pod, event, termination, and previous-container diagnostics when collection is reached after failure |

Failure collectors cap their diagnostic files, but failures before evidence-directory creation may leave nothing to upload. Success-path `pods.txt` and `events.txt` do not have an explicit byte cap. Describe the bundle as bounded qualification evidence only where the implementation actually enforces a bound.

## Container and Pod security properties

The checked-in Dockerfile builds CGO-disabled, stripped `vibedb`, shard, gateway, operator, and qualifier binaries, then copies them into a distroless non-root image. `vibedb-verify` is not included.

Generated Pods:

- run as UID, GID, and fsGroup `65532`
- disable service-account-token automount and service links
- use runtime-default seccomp
- drop all Linux capabilities and disallow privilege escalation
- mount ConfigMaps and Secrets read-only

The generated topology does **not** set CPU/memory limits, a read-only root filesystem, NetworkPolicies, RBAC, a required image digest, production secret management, ingress, or external observability. It does set resource requests, required shard anti-affinity and topology spread, and PodDisruptionBudgets; those controls do not turn this test topology into a supported deployment.

## CI scope

The `kubernetes-rf3` job in `.github/workflows/ci.yml` runs this script on `ubuntu-latest` with a 45-minute job timeout and uploads evidence for 30 days, including on failure when files exist. The workflow requests only `contents: read` and deploys nowhere outside the disposable Kind cluster.

A green job proves that the exact tested revision completed this bounded lane on that runner. It does not prove that the check is required by branch protection, that another commit is compatible, or that VibeDB is ready for production Kubernetes.

## Source map

| Concern | Source |
| --- | --- |
| complete qualification sequence | `deploy/kubernetes/qualify-kind.sh` |
| Kind node topology and immutable node image | `deploy/kubernetes/kind-3-worker.yaml` |
| application container | `deploy/kubernetes/Dockerfile` |
| manifest renderer and bootstrap CLI | `cmd/vibedb-operator/` |
| generated workloads and security context | `internal/kubeoperator/render.go` |
| disposable authority generation | `internal/kubeoperator/bootstrap.go` |
| qualification probes | `cmd/vibedb-kube-qualify/` |
| CI invocation and artifact retention | `.github/workflows/ci.yml` |
