package kubeoperator

import (
	"errors"
	"fmt"
	"io"
	"regexp"
)

var ErrConfig = errors.New("kubeoperator: invalid configuration")

var (
	dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)
	imageRef = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/@:-]*$`)
	quantity = regexp.MustCompile(`^[1-9][0-9]*(?:[EPTGMK]i?|m)?$`)
)

// Config is the deliberately small deployment surface for an unreleased RF3
// test cluster. Security identities and VibeDB manifests remain caller-owned.
type Config struct {
	Namespace string
	Image     string
	// ShardNodeIDs are role-major: catalog 0..2, ledger 0..2, data 0..2.
	ShardNodeIDs      [9]string
	ManifestConfigMap string
	TLSSecret         string
	GatewayConfigMap  string
	GatewayTLSSecret  string
	StorageClass      string
	ShardStorage      string
	GatewayStorage    string
}

func (config Config) validate() error {
	for _, value := range []string{config.Namespace, config.ManifestConfigMap,
		config.TLSSecret, config.GatewayConfigMap, config.GatewayTLSSecret} {
		if len(value) > 63 || !dnsLabel.MatchString(value) {
			return ErrConfig
		}
	}
	if !imageRef.MatchString(config.Image) || !quantity.MatchString(config.ShardStorage) ||
		!quantity.MatchString(config.GatewayStorage) ||
		(config.StorageClass != "" && (len(config.StorageClass) > 63 || !dnsLabel.MatchString(config.StorageClass))) {
		return ErrConfig
	}
	seenNodes := make(map[string]struct{}, len(config.ShardNodeIDs))
	for _, id := range config.ShardNodeIDs {
		if len(id) != 32 {
			return ErrConfig
		}
		nonzero := false
		for _, c := range id {
			if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
				return ErrConfig
			}
			nonzero = nonzero || c != '0'
		}
		if !nonzero {
			return ErrConfig
		}
		if _, exists := seenNodes[id]; exists {
			return ErrConfig
		}
		seenNodes[id] = struct{}{}
	}
	return nil
}

var rf3Roles = [...]string{"catalog", "ledger", "data"}

// Render writes one deterministic, Helm-free multi-document manifest. Every
// role is an independent RF3 group. DNS is endpoint discovery only; no
// Kubernetes object asserts leadership, membership, or catalog authority.
func Render(writer io.Writer, config Config) error {
	if writer == nil || config.validate() != nil {
		return ErrConfig
	}
	storageClass := ""
	if config.StorageClass != "" {
		storageClass = fmt.Sprintf("\n      storageClassName: %s", config.StorageClass)
	}
	if _, err := fmt.Fprintf(writer, kubeNamespaceTemplate, config.Namespace); err != nil {
		return err
	}
	for _, role := range rf3Roles {
		if _, err := fmt.Fprintf(writer, kubeRoleTemplate,
			role, config.Namespace, role, role, config.Namespace, role,
			role, config.Namespace, role, role, role, role, role, role,
			config.Image, config.Image, config.ManifestConfigMap, config.TLSSecret,
			config.ShardStorage, storageClass,
		); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(writer, kubeGatewayTemplate,
		config.Namespace, config.Namespace, config.Namespace, config.Namespace, config.Image, config.Image,
		config.ShardNodeIDs[0], config.ShardNodeIDs[1], config.ShardNodeIDs[2],
		config.ShardNodeIDs[3], config.ShardNodeIDs[4], config.ShardNodeIDs[5],
		config.ShardNodeIDs[6], config.ShardNodeIDs[7], config.ShardNodeIDs[8],
		config.GatewayConfigMap, config.GatewayTLSSecret,
		config.GatewayStorage, storageClass,
		config.Namespace, config.Namespace, config.Image, config.ShardStorage, storageClass,
	)
	return err
}

const kubeNamespaceTemplate = `apiVersion: v1
kind: Namespace
metadata:
  name: %s
  labels: {app.kubernetes.io/part-of: vibedb}
---
`

const kubeRoleTemplate = `apiVersion: v1
kind: Service
metadata: {name: vibedb-%s-peer, namespace: %s}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: %s}
  ports:
    - {name: peer, port: 7411, targetPort: peer}
    - {name: native, port: 7511, targetPort: native}
    - {name: snapshot, port: 7611, targetPort: snapshot}
    - {name: control, port: 7711, targetPort: control}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: vibedb-%s, namespace: %s}
spec:
  maxUnavailable: 1
  selector: {matchLabels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: %s}}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: vibedb-%s
  namespace: %s
  annotations: {vibedb.io/raft-role: %s}
spec:
  serviceName: vibedb-%s-peer
  replicas: 3
  minReadySeconds: 5
  revisionHistoryLimit: 2
  persistentVolumeClaimRetentionPolicy: {whenDeleted: Retain, whenScaled: Retain}
  podManagementPolicy: Parallel
  updateStrategy: {type: RollingUpdate}
  selector: {matchLabels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: %s}}
  template:
    metadata: {labels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: %s}}
    spec:
      automountServiceAccountToken: false
      enableServiceLinks: false
      terminationGracePeriodSeconds: 120
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        fsGroupChangePolicy: OnRootMismatch
        seccompProfile: {type: RuntimeDefault}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: %s}
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: %s}
      initContainers:
        - name: prepare
          image: %s
          command: [vibedb-operator]
          args: [prepare, -manifest-dir=/bootstrap, -data-dir=/var/lib/vibedb/member]
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts:
            - {name: data, mountPath: /var/lib/vibedb}
            - {name: bootstrap, mountPath: /bootstrap, readOnly: true}
            - {name: tls, mountPath: /run/secrets/vibedb, readOnly: true}
      containers:
        - name: shard
          image: %s
          command: [vibedb-shard]
          args: [serve-rf3, -manifest, /var/lib/vibedb/member/serve-rf3.vibejson]
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          ports:
            - {name: peer, containerPort: 7411}
            - {name: native, containerPort: 7511}
            - {name: snapshot, containerPort: 7611}
            - {name: control, containerPort: 7711}
          startupProbe: {tcpSocket: {port: native}, failureThreshold: 60, periodSeconds: 2}
          readinessProbe: {tcpSocket: {port: native}, periodSeconds: 2, failureThreshold: 2}
          livenessProbe: {tcpSocket: {port: native}, periodSeconds: 10, failureThreshold: 6}
          resources: {requests: {cpu: "500m", memory: 1Gi}}
          volumeMounts:
            - {name: data, mountPath: /var/lib/vibedb}
            - {name: bootstrap, mountPath: /bootstrap, readOnly: true}
            - {name: tls, mountPath: /run/secrets/vibedb, readOnly: true}
      volumes:
        - name: bootstrap
          configMap: {name: %s}
        - name: tls
          secret: {secretName: %s}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests: {storage: %s}%s
---
`

const kubeGatewayTemplate = `apiVersion: v1
kind: Service
metadata: {name: vibedb-gateway-peer, namespace: %s}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app.kubernetes.io/name: vibedb-gateway}
  ports:
    - {name: client, port: 7400, targetPort: client}
    - {name: control, port: 7401, targetPort: control}
---
apiVersion: v1
kind: Service
metadata: {name: vibedb-gateway, namespace: %s}
spec:
  type: ClusterIP
  selector: {app.kubernetes.io/name: vibedb-gateway}
  ports: [{name: client, port: 7400, targetPort: client}]
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: vibedb-gateway, namespace: %s}
spec:
  minAvailable: 1
  selector: {matchLabels: {app.kubernetes.io/name: vibedb-gateway}}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: vibedb-gateway, namespace: %s}
spec:
  serviceName: vibedb-gateway-peer
  replicas: 1
  persistentVolumeClaimRetentionPolicy: {whenDeleted: Retain, whenScaled: Retain}
  selector: {matchLabels: {app.kubernetes.io/name: vibedb-gateway}}
  template:
    metadata: {labels: {app.kubernetes.io/name: vibedb-gateway}}
    spec:
      automountServiceAccountToken: false
      enableServiceLinks: false
      terminationGracePeriodSeconds: 120
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        fsGroupChangePolicy: OnRootMismatch
        seccompProfile: {type: RuntimeDefault}
      initContainers:
        - name: prepare-catalog
          image: %s
          command: [vibedb-operator]
          args: [prepare-gateway, -catalog-source=/etc/vibedb/cluster.vibejson, -catalog-target=/var/lib/vibedb/catalog-genesis.vibejson]
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts:
            - {name: data, mountPath: /var/lib/vibedb}
            - {name: config, mountPath: /etc/vibedb, readOnly: true}
      containers:
        - name: gateway
          image: %s
          command: [vibedb-gateway]
          args:
            - serve
            - -catalog=/var/lib/vibedb/catalog-genesis.vibejson
            - -catalog-route-seed=/var/lib/vibedb/catalog-route-seed.vibejson
            - -catalog-bootstrap-if-missing
            - -initial-node-directory=/etc/vibedb/initial-node-directory.vibejson
            - -catalog-relation=1
            - -catalog-session-journal=/var/lib/vibedb/catalog-session
            - -catalog-client-id=21000000000000000000000000000000
            - -catalog-retry-home=2200000000000000
            - -durable-ack-key=/run/secrets/vibedb/durable-ack-key
            - -listen=0.0.0.0:7400
            - -tls-certificate=/run/secrets/vibedb/gateway-cert.pem
            - -tls-key=/run/secrets/vibedb/gateway-key.pem
            - -tls-roots=/run/secrets/vibedb/cluster-roots.pem
            - -tls-identity-oid=1.3.6.1.4.1.32473.1.1
            - -authorization-policy=/etc/vibedb/authorization-policy.vibejson
            - -replica-control-manifest=/etc/vibedb/replica-control.vibejson
            - -shard-peer=vibedb-catalog-0.vibedb-catalog-peer:7511=%s
            - -shard-peer=vibedb-catalog-1.vibedb-catalog-peer:7511=%s
            - -shard-peer=vibedb-catalog-2.vibedb-catalog-peer:7511=%s
            - -shard-peer=vibedb-ledger-0.vibedb-ledger-peer:7511=%s
            - -shard-peer=vibedb-ledger-1.vibedb-ledger-peer:7511=%s
            - -shard-peer=vibedb-ledger-2.vibedb-ledger-peer:7511=%s
            - -shard-peer=vibedb-data-0.vibedb-data-peer:7511=%s
            - -shard-peer=vibedb-data-1.vibedb-data-peer:7511=%s
            - -shard-peer=vibedb-data-2.vibedb-data-peer:7511=%s
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          ports:
            - {name: client, containerPort: 7400}
            - {name: control, containerPort: 7401}
          startupProbe: {tcpSocket: {port: client}, failureThreshold: 60, periodSeconds: 2}
          readinessProbe: {tcpSocket: {port: client}, periodSeconds: 2, failureThreshold: 2}
          livenessProbe: {tcpSocket: {port: client}, periodSeconds: 10, failureThreshold: 6}
          resources: {requests: {cpu: "500m", memory: 1Gi}}
          volumeMounts:
            - {name: data, mountPath: /var/lib/vibedb}
            - {name: config, mountPath: /etc/vibedb, readOnly: true}
            - {name: tls, mountPath: /run/secrets/vibedb, readOnly: true}
      volumes:
        - name: config
          configMap: {name: %s}
        - name: tls
          secret: {secretName: %s}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests: {storage: %s}%s
---
apiVersion: v1
kind: Service
metadata: {name: vibedb-replacement-peer, namespace: %s}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app.kubernetes.io/component: replacement}
  ports:
    - {name: peer, port: 7411, targetPort: peer}
    - {name: native, port: 7511, targetPort: native}
    - {name: snapshot, port: 7611, targetPort: snapshot}
    - {name: control, port: 7711, targetPort: control}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: vibedb-learner-bootstrap-template
  namespace: %s
  annotations: {vibedb.io/template: "true"}
spec:
  serviceName: vibedb-replacement-peer
  replicas: 0
  persistentVolumeClaimRetentionPolicy: {whenDeleted: Retain, whenScaled: Retain}
  selector: {matchLabels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: replacement}}
  template:
    metadata: {labels: {app.kubernetes.io/name: vibedb-shard, app.kubernetes.io/component: replacement}}
    spec:
      automountServiceAccountToken: false
      enableServiceLinks: false
      terminationGracePeriodSeconds: 120
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        fsGroupChangePolicy: OnRootMismatch
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: bootstrap
          image: %s
          command: [vibedb-shard]
          args: [bootstrap-rf3, -manifest, /etc/vibedb/bootstrap-rf3.vibejson]
          securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}
          volumeMounts:
            - {name: target-data, mountPath: /var/lib/vibedb}
            - {name: target-config, mountPath: /etc/vibedb, readOnly: true}
            - {name: target-tls, mountPath: /run/secrets/vibedb, readOnly: true}
      volumes:
        - name: target-config
          configMap: {name: replace-with-target-config}
        - name: target-tls
          secret: {secretName: replace-with-target-tls}
  volumeClaimTemplates:
    - metadata: {name: target-data}
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests: {storage: %s}%s
`
