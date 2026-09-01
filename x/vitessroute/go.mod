module github.com/thesyncim/vibedb/x/vitessroute

go 1.27

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/thesyncim/vibedb v0.0.0
	github.com/thesyncim/vibejson v0.0.0-20260730224651-50a62f7753df
	vitess.io/vitess v0.24.2
)

require (
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20240806141605-e8a1dd7889d6 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/lmittmann/tint v1.1.3 // indirect
	github.com/mattn/go-isatty v0.0.21 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20250313105119-ba97887b0a25 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/thesyncim/vibedb => ../..
