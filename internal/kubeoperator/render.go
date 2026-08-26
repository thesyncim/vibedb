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
	Namespace         string
	Image             string
	ShardNodeIDs      [3]string
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

// Render writes one deterministic, Helm-free multi-document manifest. DNS is
// used only for endpoint discovery; no Kubernetes object asserts leadership.
func Render(writer io.Writer, config Config) error {
	if writer == nil || config.validate() != nil {
		return ErrConfig
	}
	storageClass := ""
	if config.StorageClass != "" {
		storageClass = fmt.Sprintf("\n      storageClassName: %s", config.StorageClass)
	}
	_, err := fmt.Fprintf(writer, manifestTemplate,
		config.Namespace,
		config.Namespace,
		config.Namespace, config.Image, config.Image, config.ManifestConfigMap, config.TLSSecret,
		config.ShardStorage, storageClass,
		config.Namespace,
		config.Namespace,
		config.Namespace,
		config.Namespace, config.Image,
		config.ShardNodeIDs[0], config.ShardNodeIDs[1], config.ShardNodeIDs[2],
		config.GatewayConfigMap, config.GatewayTLSSecret,
		config.GatewayStorage, storageClass,
		config.Namespace, config.Image,
		config.ShardStorage, storageClass,
	)
	return err
}

const manifestTemplate = `apiVersion: v1
kind: Service
metadata:
  name: vibedb-shard-peer
  namespace: %s
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app.kubernetes.io/name: vibedb-shard
    app.kubernetes.io/component: serving
  ports:
    - {name: peer, port: 7411, targetPort: peer}
    - {name: native, port: 7511, targetPort: native}
    - {name: snapshot, port: 7611, targetPort: snapshot}
    - {name: control, port: 7711, targetPort: control}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: vibedb-shard
  namespace: %s
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vibedb-shard
      app.kubernetes.io/component: serving
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: vibedb-shard
  namespace: %s
spec:
  serviceName: vibedb-shard-peer
  replicas: 3
  podManagementPolicy: Parallel
  updateStrategy: {type: RollingUpdate}
  selector:
    matchLabels:
      app.kubernetes.io/name: vibedb-shard
      app.kubernetes.io/component: serving
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vibedb-shard
        app.kubernetes.io/component: serving
    spec:
      terminationGracePeriodSeconds: 120
      initContainers:
        - name: prepare
          image: %s
          command: [vibedb-operator]
          args:
            - prepare
            - -manifest-dir=/bootstrap
            - -data-dir=/var/lib/vibedb/member
          volumeMounts:
            - {name: data, mountPath: /var/lib/vibedb}
            - {name: bootstrap, mountPath: /bootstrap, readOnly: true}
            - {name: tls, mountPath: /run/secrets/vibedb, readOnly: true}
      containers:
        - name: shard
          image: %s
          command: [vibedb-shard]
          args: [serve-rf3, -manifest, /var/lib/vibedb/member/serve-rf3.vibejson]
          ports:
            - {name: peer, containerPort: 7411}
            - {name: native, containerPort: 7511}
            - {name: snapshot, containerPort: 7611}
            - {name: control, containerPort: 7711}
          startupProbe:
            tcpSocket: {port: native}
            failureThreshold: 60
            periodSeconds: 2
          readinessProbe:
            tcpSocket: {port: native}
            periodSeconds: 2
            failureThreshold: 2
          resources:
            requests: {cpu: "1", memory: 1Gi}
          volumeMounts:
            - {name: data, mountPath: /var/lib/vibedb}
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
apiVersion: v1
kind: Service
metadata:
  name: vibedb-gateway-peer
  namespace: %s
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: vibedb-gateway
  ports:
    - {name: client, port: 7400, targetPort: client}
---
apiVersion: v1
kind: Service
metadata:
  name: vibedb-gateway
  namespace: %s
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: vibedb-gateway
  ports:
    - {name: client, port: 7400, targetPort: client}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: vibedb-gateway
  namespace: %s
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vibedb-gateway
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: vibedb-gateway
  namespace: %s
spec:
  serviceName: vibedb-gateway-peer
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: vibedb-gateway
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vibedb-gateway
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: gateway
          image: %s
          command: [vibedb-gateway]
          args:
            - serve
            - -catalog=/etc/vibedb/cluster.vibejson
            - -catalog-relation=1
            - -catalog-session-journal=/var/lib/vibedb/catalog-session
            - -catalog-client-id=21000000000000000000000000000000
            - -catalog-retry-home=2200000000000000
            - -listen=0.0.0.0:7400
            - -tls-certificate=/run/secrets/vibedb/gateway-cert.pem
            - -tls-key=/run/secrets/vibedb/gateway-key.pem
            - -tls-roots=/run/secrets/vibedb/cluster-roots.pem
            - -tls-identity-oid=1.3.6.1.4.1.32473.1.1
            - -authorization-policy=/etc/vibedb/authorization-policy.vibejson
            - -replica-control-manifest=/etc/vibedb/replica-control.vibejson
            - -shard-peer=vibedb-shard-0.vibedb-shard-peer:7511=%s
            - -shard-peer=vibedb-shard-1.vibedb-shard-peer:7511=%s
            - -shard-peer=vibedb-shard-2.vibedb-shard-peer:7511=%s
          ports:
            - {name: client, containerPort: 7400}
          startupProbe:
            tcpSocket: {port: client}
            failureThreshold: 60
            periodSeconds: 2
          readinessProbe:
            tcpSocket: {port: client}
            periodSeconds: 2
            failureThreshold: 2
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
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: vibedb-learner-bootstrap-template
  namespace: %s
  annotations:
    vibedb.io/template: "true"
spec:
  serviceName: vibedb-shard-peer
  replicas: 0
  selector:
    matchLabels:
      app.kubernetes.io/name: vibedb-shard
      app.kubernetes.io/component: replacement
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vibedb-shard
        app.kubernetes.io/component: replacement
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: bootstrap
          image: %s
          command: [vibedb-shard]
          args: [bootstrap-rf3, -manifest, /etc/vibedb/bootstrap-rf3.vibejson]
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
