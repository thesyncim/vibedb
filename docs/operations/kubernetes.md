# Run an RF3 test cluster on Kubernetes

VibeDB includes a small, Helm-free manifest renderer for repeatable Kubernetes
fault and lifecycle tests. The renderer is not a reconciliation watch-loop or
topology controller: Kubernetes manages processes and volumes, while Raft and
the replicated catalog remain the only leader, membership, ownership, and
routing authorities.

## Build the image contract

Build the repository's static, non-root image, or provide an equivalent image
containing `vibedb-shard`, `vibedb-gateway`, and `vibedb-operator` on `PATH`:

```bash
docker build -f deploy/kubernetes/Dockerfile -t registry.example/vibedb:commit-sha .
docker push registry.example/vibedb:commit-sha
```

Use a Kubernetes cluster with at least three
failure-domain nodes. Supply these objects before applying the rendered
workloads:

- `vibedb-rf3-manifests`, with `catalog-{0,1,2}.vibejson`,
  `ledger-{0,1,2}.vibejson`, and `data-{0,1,2}.vibejson`;
- `vibedb-rf3-tls`, with the nine members' keys and certificates, cluster
  roots, and WAL key sources referenced by those manifests;
- `vibedb-gateway-config`, with `cluster.vibejson`,
  `authorization-policy.vibejson`, and `replica-control.vibejson`;
- `vibedb-gateway-tls`, with the gateway key, certificate, and cluster roots.

Each preparation manifest must use `/var/lib/vibedb/member` as its exact root,
bind its listener ports to `0.0.0.0`, and use its role's stable peer addresses.
For example, the catalog group uses:

```text
vibedb-catalog-0.vibedb-catalog-peer:7411
vibedb-catalog-1.vibedb-catalog-peer:7411
vibedb-catalog-2.vibedb-catalog-peer:7411
```

Replace `catalog` with `ledger` and `data` for the other two groups. Every pod
has a distinct TLS node identity. The nine IDs passed below are role-major:
catalog ordinals 0–2, ledger ordinals 0–2, then data ordinals 0–2.

The ConfigMaps are bootstrap inputs, not live authority. Changing one does not
rewrite an existing PVC's sealed identities or durable membership.

Automatic splitting is not enabled by this Kubernetes bootstrap. It emits an
explicit empty `split_sources` inventory because the init containers have not
yet prepared their actual SQL storage identities. Enabling it requires enrolling
the exact prepared source schema and replica identities; the renderer does not
invent them. The Kind gate qualifies RF3 serving and restart, not hot splitting.

## Render and apply

For a disposable development or test cluster, generate the complete bootstrap
authority instead of hand-writing these objects:

```bash
install -d -m 0700 /absolute/private/vibedb-bootstrap
go run ./cmd/vibedb-operator bootstrap \
  -namespace vibedb-test \
  -state-dir /absolute/private/vibedb-bootstrap \
  > /absolute/private/vibedb-bootstrap-resources.yaml \
  2> /absolute/private/vibedb-bootstrap-identities.txt
```

The command creates a short-lived test CA, distinct identities for all nine
members, the gateway, and a qualification client, plus the exact preparation,
catalog, policy, replica-control, WAL, and durable-ACK inputs. It uses operating
system cryptographic entropy and refuses partial or mismatched authority state.
The retained private bundle is mode `0600`. This is deliberately not production
PKI: it has no external root, HSM/KMS integration, certificate rotation,
operator RBAC, or multi-party issuance. Do not reuse it outside disposable
development and CI clusters.

Use the emitted `shard-node-ids=` value as the renderer's role-major node list,
create the namespace, and apply the bootstrap objects before the topology.

Pass the exact TLS node IDs in StatefulSet ordinal order:

```bash
go run ./cmd/vibedb-operator render \
  -image registry.example/vibedb:commit-sha \
  -namespace vibedb-test \
  -shard-node-ids 11000000000000000000000000000000,12000000000000000000000000000000,13000000000000000000000000000000,21000000000000000000000000000000,22000000000000000000000000000000,23000000000000000000000000000000,31000000000000000000000000000000,32000000000000000000000000000000,33000000000000000000000000000000 \
  > ./vibedb-kubernetes.yaml

go run ./cmd/vibedb-operator validate -manifest ./vibedb-kubernetes.yaml
kubectl apply --dry-run=server -f ./vibedb-kubernetes.yaml
kubectl apply -f ./vibedb-kubernetes.yaml
```

`validate` is bounded to 2 MiB and checks the VibeDB-specific topology contract:
the exact object set, three independent RF3 groups, unique role endpoints,
retained PVCs, one-at-a-time voluntary disruption, failure-domain spread,
probes, and container hardening. Server-side dry-run additionally checks the
Kubernetes version's resource schemas and admission policy.

The rendered lane contains:

- independent three-replica catalog, request-ledger, and data StatefulSets,
  each with stable ordinals and one retained PVC per member;
- one headless peer Service and one `maxUnavailable: 1` PodDisruptionBudget per
  Raft group;
- hard hostname spreading, parallel initial Raft process creation, and
  five-second minimum readiness before Kubernetes advances a rolling update;
- non-root processes, the runtime-default seccomp profile, no service-account
  token, and no CPU limit on latency-sensitive database processes;
- a durable single-gateway StatefulSet, a headless governing Service, and a
  separate client-facing ClusterIP Service;
- a scale-zero replacement StatefulSet template that runs the shipped
  `bootstrap-rf3` command against an empty target PVC.

The single gateway is test tooling, not gateway HA. Running multiple gateways
requires distinct durable session/issuer identities and journals per ordinal;
the renderer intentionally does not manufacture those authorities.
The gateway headless Service also publishes its authenticated catalog-drain
control endpoint. Generation-one catalog bootstrap is enabled, but it still
requires the exact immutable seed and proof generated by the test bootstrap
command; a missing head beside an existing proof fails closed.

The startup, readiness, and liveness probes check TCP reachability only. They
do not claim leadership, linearizability, catch-up, or catalog authority. The
gateway still performs authenticated leader discovery and exact fence
validation.

## Stop, restart, and disrupt

Kubernetes sends `SIGTERM`; both shipped commands stop accepting work and
drain within the configured 120-second termination window. The PDB protects a
voluntary single-member disruption, but it cannot make simultaneous node loss,
forced deletion, or an unavailable storage backend safe.

For tests, delete one follower Pod from one role and verify that its StatefulSet
ordinal reopens the same PVC and catches up. Do not delete its PVC unless
testing permanent replica loss and the replacement protocol.

## Bootstrap a replacement

The rendered `vibedb-learner-bootstrap-template` is deliberately scaled to
zero. Before scaling it up:

1. Create the target ConfigMap and TLS Secret named in the template.
2. Ensure its manifest binds a new member, node, store, and empty target root.
3. Add the exact enrolled target to the serving manifests and replicated
   catalog workflow.
4. Let the gateway persist and distribute the membership grant.
5. Patch the placeholder object names and scale the target StatefulSet to one.

The target runs `bootstrap-rf3`, installs an authenticated snapshot, catches
up as a learner, and hands off to ordinary RF3 serving. Do not patch the
serving StatefulSet from three to four replicas: an extra Pod is not a Raft
member. The gateway's durable replacement controller performs promotion,
catalog G+1 and drain, safe removal, catalog G+2 and drain, and retirement.

Kubernetes DNS supplies stable endpoint discovery only. It never decides the
leader or authorizes failover, promotion, removal, or routing.

See [Operate replica lifecycle](replica-lifecycle.md) for the exact grant,
snapshot, promotion, removal, and finalization contract.

## Keep restore separate from replacement

Learner bootstrap is a membership operation in the existing cluster. Restore
creates fresh cluster and replica identities from a certified complete backup.
Do not point the learner template at a restore staging root or run ordinary
preparation over that root.

The `restore-group` and `adopt-restore` commands construct and adopt exact
restore replicas, but retain a closed-serving fence. Target catalog activation
and transient per-process grants remain database authority, not ConfigMap,
Pod, PVC, or DNS authority. The renderer does not provision the post-seal
certificates or drive the complete restore activation lifecycle. See
[Back up and restore distributed data](backup-restore.md) for the command
inputs and current qualification boundary.

## Mandatory three-worker Kind qualification

Linux CI runs the same checked-in command developers can run locally:

```bash
go install sigs.k8s.io/kind@v0.32.0
deploy/kubernetes/qualify-kind.sh
```

The gate creates one control-plane and three worker containers from a pinned
Kubernetes node image, builds and loads the non-root VibeDB image, generates
fresh test authority, and starts all three RF3 groups plus the gateway. It then
requires stable Pod DNS, ten retained PVC identities, an acknowledged durable
write and repeated reads, a rolling restart of every catalog, ledger, data, and
gateway ordinal, exact-request recovery, and post-restart row visibility. Each
serving process must remain below hard one-second read p99 and five-second
terminal/read maximum latency, 1 GiB RSS, 1 GiB apparent durable storage, and 512 MiB WAL. The
gate has no skip mode and uploads bounded evidence even on failure.

This qualification proves a disposable single-data-shard RF3 serving and
restart path. It does not prove multi-data-shard scale, involuntary partitions,
cloud-volume behavior, gateway HA, production certificate lifecycle, or the
complete automated replacement workflow.
