# Run an RF3 test cluster on Kubernetes

VibeDB includes a small, Helm-free manifest renderer for repeatable Kubernetes
fault and lifecycle tests. The distributed runtime is unreleased and
experimental. The renderer is not a topology controller: Kubernetes manages
processes and volumes, while Raft and the replicated catalog remain the only
leader, membership, ownership, and routing authorities.

## Build the image contract

The image must contain `vibedb-shard`, `vibedb-gateway`, and
`vibedb-operator` on `PATH`. Supply these objects before applying the rendered
workloads:

- `vibedb-rf3-manifests`, with `prepare-0.vibejson`,
  `prepare-1.vibejson`, and `prepare-2.vibejson`;
- `vibedb-rf3-tls`, with the three members' keys and certificates, cluster
  roots, and WAL key source referenced by those manifests;
- `vibedb-gateway-config`, with `cluster.vibejson`,
  `authorization-policy.vibejson`, and `replica-control.vibejson`;
- `vibedb-gateway-tls`, with the gateway key, certificate, and cluster roots.

Each preparation manifest must use `/var/lib/vibedb/member` as its exact root,
bind its listener ports to `0.0.0.0`, and use these stable peer addresses:

```text
vibedb-shard-0.vibedb-shard-peer:7411
vibedb-shard-1.vibedb-shard-peer:7411
vibedb-shard-2.vibedb-shard-peer:7411
```

The ConfigMaps are bootstrap inputs, not live authority. Changing one does not
rewrite an existing PVC's sealed identities or durable membership.

## Render and apply

Pass the exact TLS node IDs in StatefulSet ordinal order:

```bash
go run ./cmd/vibedb-operator render \
  -image registry.example/vibedb:commit-sha \
  -namespace vibedb-test \
  -shard-node-ids 11000000000000000000000000000000,12000000000000000000000000000000,13000000000000000000000000000000 \
  > ./vibedb-kubernetes.yaml

kubectl apply -f ./vibedb-kubernetes.yaml
```

The rendered lane contains:

- one three-replica shard StatefulSet with stable ordinals and one PVC per
  member;
- a headless Service publishing peer, native, snapshot, and control DNS;
- a shard PodDisruptionBudget with `maxUnavailable: 1`;
- a durable single-gateway StatefulSet, a headless governing Service, and a
  separate client-facing ClusterIP Service;
- a scale-zero replacement StatefulSet template that runs the shipped
  `bootstrap-rf3` command against an empty target PVC.

The startup and readiness probes check TCP reachability only. They do not
claim leadership, linearizability, catch-up, or catalog authority. The gateway
still performs authenticated leader discovery and exact fence validation.

## Stop, restart, and disrupt

Kubernetes sends `SIGTERM`; both shipped commands stop accepting work and
drain within the configured 120-second termination window. The PDB protects a
voluntary single-member disruption, but it cannot make simultaneous node loss,
forced deletion, or an unavailable storage backend safe.

For tests, delete one follower Pod and verify that its StatefulSet ordinal
reopens the same PVC and catches up. Do not delete its PVC unless testing
permanent replica loss and the replacement protocol.

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
