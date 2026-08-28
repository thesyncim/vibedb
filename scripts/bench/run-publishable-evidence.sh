#!/usr/bin/env bash
# Run the complete claim-free embedded and RF3 publication evidence matrix.
# The final receipt is created only after every artifact validates.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 ABSOLUTE_NEW_OUTPUT_DIRECTORY OUT_OF_RAM_DOCUMENTS" >&2
  exit 2
fi

evidence=$1
out_of_ram_documents=$2
if [[ ${evidence} != /* || -e ${evidence} ]]; then
  echo "output must be an absolute path that does not exist" >&2
  exit 2
fi
if [[ ! ${out_of_ram_documents} =~ ^[1-9][0-9]*$ ]]; then
  echo "OUT_OF_RAM_DOCUMENTS must be a positive integer sized above this host's RAM" >&2
  exit 2
fi
if [[ $(uname -s) != Linux ]]; then
  echo "publishable process-write, allocated-block, cold/fault evidence requires Linux" >&2
  exit 1
fi

repo=$(git rev-parse --show-toplevel)
if [[ -n $(git -C "${repo}" status --porcelain=v1 --untracked-files=normal) ]]; then
  echo "publication requires a clean working tree" >&2
  exit 1
fi
revision=$(git -C "${repo}" rev-parse HEAD)
mkdir -m 700 "${evidence}"
scratch=$(mktemp -d)
trap 'rm -rf -- "${scratch}"' EXIT

# Test binaries omit Go's vcs.* build settings. Stamp this binary from the
# checked compile-time tree, then run that same binary for every RF3 sample.
if [[ ! ${revision} =~ ^[0-9a-f]{40}$ && ! ${revision} =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid compile-time source revision" >&2
  exit 1
fi
cd "${repo}"
if [[ $(git rev-parse HEAD) != "${revision}" || -n $(git status --porcelain=v1 --untracked-files=normal) ]]; then
  echo "source changed before RF3 qualification build" >&2
  exit 1
fi
go test -c \
  -ldflags "-X github.com/thesyncim/vibedb/internal/rf3bench.buildRevision=${revision} -X github.com/thesyncim/vibedb/internal/rf3bench.buildModified=false" \
  -o "${scratch}/rf3-evidence.test" ./internal/raftservice
if [[ $(git rev-parse HEAD) != "${revision}" || -n $(git status --porcelain=v1 --untracked-files=normal) ]]; then
  echo "source changed during RF3 qualification build" >&2
  exit 1
fi

filesystem=$(stat -f -c '%T' "${evidence}")
{
  printf 'meta\tcommand_schema\tvibedb.publish-evidence/1\n'
  printf 'meta\trevision\t%s\n' "${revision}"
  printf 'meta\tvcs_modified\tfalse\n'
  printf 'meta\tgo_version\t%s\n' "$(go version | tr '\t\r\n' '   ')"
  printf 'meta\tgoos\t%s\n' "$(go env GOOS)"
  printf 'meta\tgoarch\t%s\n' "$(go env GOARCH)"
  printf 'meta\tkernel\t%s\n' "$(uname -srvmo | tr '\t\r\n' '   ')"
  printf 'meta\tfilesystem\t%s\n' "${filesystem}"
  printf 'meta\tout_of_ram_documents\t%s\n' "${out_of_ram_documents}"
  printf 'meta\tembedded_repetitions\t10\n'
  printf 'meta\trf3_repetitions\t9\n'
} > "${evidence}/metadata.tsv"
sync "${evidence}/metadata.tsv"

cd "${repo}/bench/competitive"
go build -trimpath -o "${scratch}/mixed" ./cmd/mixed
common_suite=(
  -mixed-bin="${scratch}/mixed"
  -engines=vibedb,bbolt,badger,pebble,sqlite
  -repetitions=10 -conditioning=true -timeout=30m
  -workload=ycsb-a -corpus=10000 -operations=20000 -warmup=2000
  -durability=ordinary-sync -checkpoint-mutations=0 -clients=1 -cardinality=low
)
go run ./cmd/mixedsuite "${common_suite[@]}" -document-shape=inline -exact-indexes=0 \
  -output="${evidence}/mixed-ordinary-sync.tsv"
go run ./cmd/mixedsuite -mixed-bin="${scratch}/mixed" -engines=vibedb,sqlite \
  -repetitions=10 -conditioning=true -timeout=30m -workload=ycsb-a \
  -corpus=10000 -operations=20000 -warmup=2000 -durability=ordinary-sync \
  -checkpoint-mutations=0 -clients=1 -cardinality=low -document-shape=inline \
  -exact-indexes=3 -output="${evidence}/mixed-indexed-ordinary-sync.tsv"
go run ./cmd/mixedsuite "${common_suite[@]}" -document-shape=overflow-heavy -exact-indexes=0 \
  -output="${evidence}/mixed-overflow-ordinary-sync.tsv"
go run ./cmd/mixedsuite -mixed-bin="${scratch}/mixed" -engines=vibedb,sqlite \
  -repetitions=10 -conditioning=true -timeout=30m -workload=ycsb-a \
  -corpus=10000 -operations=20000 -warmup=2000 -durability=power-safe \
  -checkpoint-mutations=0 -clients=1 -cardinality=low -document-shape=inline \
  -exact-indexes=3 -output="${evidence}/mixed-power-safe.tsv"

engines=(badger bbolt pebble sqlite vibedb)
for engine in "${engines[@]}"; do
  go run ./cmd/footprint -header -engine="${engine}" -corpus=100000 \
    -durability=ordinary-sync -document-shape=overflow-heavy -exact-indexes=0 \
    -storage-profile=intrinsic > "${evidence}/footprint-${engine}.tsv"
  go run ./cmd/churndisk -engine="${engine}" -corpus=100000 -mutations=200000 \
    -replace-percent=80 -sample-mutations=5000 -checkpoint-mutations=64 \
    -durability=buffered-visible -exact-indexes=0 -cardinality=low \
    -document-shape=inline -storage-profile=intrinsic \
    -require-physical-write=true -max-rss-bytes=2147483648 \
    -max-allocated-bytes=8589934592 -max-physical-write-bytes=17179869184 \
    > "${evidence}/churn-${engine}.tsv"
  go run ./cmd/outofram -engine="${engine}" -corpus="${out_of_ram_documents}" \
    -durability=ordinary-sync -exact-indexes=0 -cardinality=low \
    -document-shape=overflow-heavy -checkpoint-documents=4096 \
    -max-loader-bytes=8388608 -max-rss-bytes=0 -max-physical-write-bytes=0 \
    > "${evidence}/outofram-${engine}.tsv"
done
for engine in sqlite vibedb; do
  go run ./cmd/footprint -header -engine="${engine}" -corpus=100000 \
    -durability=ordinary-sync -document-shape=overflow-heavy -exact-indexes=3 \
    -storage-profile=intrinsic > "${evidence}/footprint-indexed-${engine}.tsv"
done

cd "${repo}"
for run in $(seq 1 9); do
  run_directory=$(printf '%s/rf3/run-%02d' "${evidence}" "${run}")
  mkdir -m 700 -p "${run_directory}"
  for workload in read write mixed; do
    VIBEDB_RF3_BENCH=1 \
    VIBEDB_RF3_OUTPUT="${run_directory}" \
    VIBEDB_RF3_OPERATIONS=10000 \
    VIBEDB_RF3_WARMUP=1000 \
    VIBEDB_RF3_WORKLOAD="${workload}" \
      "${scratch}/rf3-evidence.test" -test.count=1 -test.timeout=30m -test.run '^TestRF3EvidenceMatrix$' \
      > "${run_directory}/${workload}.log" 2>&1
  done
done
go run ./bench/rf3chaos -output "${evidence}/rf3-chaos.tsv" -runs 9 -timeout 5m

cd "${repo}/bench/competitive"
go run ./cmd/publishcheck -evidence "${evidence}" -write-receipt
sync "${evidence}/VALIDATED.tsv"
