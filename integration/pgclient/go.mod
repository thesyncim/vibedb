module github.com/thesyncim/vibedb/integration/pgclient

go 1.26

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/lib/pq v1.12.3
	github.com/thesyncim/vibedb v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/thesyncim/vibejson v0.0.0-20260730224651-50a62f7753df // indirect
	go.etcd.io/raft/v3 v3.7.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/thesyncim/vibedb => ../..
