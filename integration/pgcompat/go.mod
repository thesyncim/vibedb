module github.com/thesyncim/vibedb/integration/pgcompat

go 1.27

require github.com/thesyncim/vibedb v0.0.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/thesyncim/vibejson v0.0.0-20260904124246-ba74e524519b // indirect
	go.etcd.io/raft/v3 v3.7.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/thesyncim/vibedb => ../..
