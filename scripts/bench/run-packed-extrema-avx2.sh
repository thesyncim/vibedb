#!/usr/bin/env bash
# Qualify the durable packed integer extrema lane on AMD64. The same
# precompiled query binary is run with AVX2 enabled and disabled; all five
# samples for each state are retained before the paired medians are gated.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 ABSOLUTE_EVIDENCE_DIRECTORY HEAD_QUERY_TEST HEAD_STOREIO_TEST" >&2
  exit 2
fi

evidence=$1
query_binary=$2
storeio_binary=$3
if [[ ${evidence} != /* || -e ${evidence} ]]; then
  echo "evidence directory must be an absolute path that does not exist" >&2
  exit 2
fi
for binary in "${query_binary}" "${storeio_binary}"; do
  if [[ ! -x ${binary} ]]; then
    echo "test binary is not executable: ${binary}" >&2
    exit 2
  fi
done

if [[ $(go env GOARCH) != amd64 ]]; then
  echo "packed extrema AVX2 qualification requires GOARCH=amd64" >&2
  exit 1
fi
if [[ $(go env GOAMD64) != v1 ]]; then
  echo "packed extrema AVX2 qualification requires GOAMD64=v1" >&2
  exit 1
fi
if [[ $(go env GOEXPERIMENT) != *simd* ]]; then
  echo "packed extrema AVX2 qualification requires GOEXPERIMENT=simd" >&2
  exit 1
fi
case $(uname -m) in
  x86_64|amd64) ;;
  *) echo "packed extrema AVX2 qualification requires an x86_64 runner" >&2; exit 1 ;;
esac

mkdir -m 700 "${evidence}"
raw_directory=${evidence}/raw
metadata_directory=${evidence}/metadata
mkdir -m 700 "${raw_directory}" "${metadata_directory}"
commands_file=${evidence}/benchmark-commands.tsv
order_file=${evidence}/benchmark-order.tsv
summary_file=${evidence}/summary.tsv

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$1" | awk '{print $1}'
  else
    shasum -a 256 -- "$1" | awk '{print $1}'
  fi
}

query_sha256=$(sha256_file "${query_binary}")
storeio_sha256=$(sha256_file "${storeio_binary}")
{
  printf 'revision=%s\n' "$(git rev-parse HEAD 2>/dev/null || printf unknown)"
  printf 'query_binary=%s\n' "${query_binary}"
  printf 'query_sha256=%s\n' "${query_sha256}"
  printf 'storeio_binary=%s\n' "${storeio_binary}"
  printf 'storeio_sha256=%s\n' "${storeio_sha256}"
  printf 'go_version=%s\n' "$(go version)"
  printf 'GOARCH=%s\n' "$(go env GOARCH)"
  printf 'GOAMD64=%s\n' "$(go env GOAMD64)"
  printf 'GOEXPERIMENT=%s\n' "$(go env GOEXPERIMENT)"
  printf 'GOTOOLCHAIN=%s\n' "$(go env GOTOOLCHAIN)"
  printf 'GOMAXPROCS=1\n'
  printf 'uname=%s\n' "$(uname -a)"
  printf 'benchmark_rounds=5\n'
  printf 'benchmark_time=250ms\n'
} > "${metadata_directory}/environment.txt"
printf 'invocation\t%s %s %s %s\n' "$0" "${evidence}" "${query_binary}" "${storeio_binary}" > "${metadata_directory}/script-command.txt"

bench_expression='^BenchmarkFilePackedIntegerExtremaCount(Wide)?/min-max/(FOR10|FOR16)$'
printf 'kind\tmode\tcommand\n' > "${commands_file}"
printf 'round\tmode\toutput\n' > "${order_file}"

run_dispatch_check() {
  local mode=$1
  local output=${metadata_directory}/dispatch-${mode}.txt
  if [[ ${mode} == enabled ]]; then
    printf 'dispatch\t%s\tenv GOAMD64=v1 GOEXPERIMENT=simd GODEBUG= VIBEDB_TEST_REQUIRE_AVX2=1 %s -test.v -test.run %q -test.count=1\n' \
      "${mode}" "${storeio_binary}" '^TestCountCompactPackedExtremaDispatchAVX2$' >> "${commands_file}"
    if ! env GOAMD64=v1 GOEXPERIMENT=simd GODEBUG= VIBEDB_TEST_REQUIRE_AVX2=1 \
      "${storeio_binary}" -test.v -test.run '^TestCountCompactPackedExtremaDispatchAVX2$' \
      -test.count=1 > "${output}" 2>&1; then
      cat "${output}" >&2
      echo "AVX2 enabled dispatch check failed" >&2
      exit 1
    fi
    if ! grep -Eq 'AVX2=true' "${output}"; then
      echo "AVX2 enabled dispatch output did not prove AVX2=true" >&2
      exit 1
    fi
  else
    printf 'dispatch\t%s\tenv GOAMD64=v1 GOEXPERIMENT=simd GODEBUG=cpu.avx2=off %s -test.v -test.run %q -test.count=1\n' \
      "${mode}" "${storeio_binary}" '^TestCountCompactPackedExtremaDispatchAVX2$' >> "${commands_file}"
    if ! env GOAMD64=v1 GOEXPERIMENT=simd GODEBUG=cpu.avx2=off \
      "${storeio_binary}" -test.v -test.run '^TestCountCompactPackedExtremaDispatchAVX2$' \
      -test.count=1 > "${output}" 2>&1; then
      cat "${output}" >&2
      echo "AVX2 disabled dispatch check failed" >&2
      exit 1
    fi
    if ! grep -Eq 'AVX2=false' "${output}"; then
      echo "AVX2 disabled dispatch output did not prove AVX2=false" >&2
      exit 1
    fi
  fi
}

validate_benchmark_output() {
  local output=$1
  if ! awk '
    /^Benchmark/ {
      name = $1
      sub(/-[0-9]+$/, "", name)
      if (name != "BenchmarkFilePackedIntegerExtremaCount/min-max/FOR10" &&
          name != "BenchmarkFilePackedIntegerExtremaCountWide/min-max/FOR16") {
        bad = 1
        next
      }
      lines++
      if ($0 !~ /ns\/op/ || $0 !~ /B\/op/ || $0 !~ /allocs\/op/) {
        bad = 1
      }
      for (field = 1; field <= NF; field++) {
        if (($field == "B/op" || $field == "allocs/op") &&
            $(field - 1) + 0 != 0) {
          bad = 1
        }
      }
    }
    END { exit(lines != 2 || bad != 0) }
  ' "${output}"; then
    echo "benchmark output lacks the two exact zero-allocation extrema cases: ${output}" >&2
    return 1
  fi
}

run_benchmark() {
  local round=$1
  local mode=$2
  local output
  output=${raw_directory}/round-$(printf '%02d' "${round}")-${mode}.txt
  local debug=''
  if [[ ${mode} == disabled ]]; then
    debug='cpu.avx2=off'
  fi
  printf 'benchmark\t%s\tenv GOAMD64=v1 GOEXPERIMENT=simd GODEBUG=%s VIBEDB_EXPECT_EXTREMA=1 %s -test.run %q -test.bench %q -test.benchtime 250ms -test.count=1 -test.cpu=1 -test.benchmem\n' \
    "${mode}" "${debug}" "${query_binary}" '^$' "${bench_expression}" >> "${commands_file}"
  if ! env GOAMD64=v1 GOEXPERIMENT=simd GODEBUG="${debug}" VIBEDB_EXPECT_EXTREMA=1 \
    "${query_binary}" -test.run '^$' -test.bench "${bench_expression}" \
    -test.benchtime 250ms -test.count=1 -test.cpu=1 -test.benchmem > "${output}" 2>&1; then
    cat "${output}" >&2
    echo "${mode} extrema benchmark failed" >&2
    exit 1
  fi
  validate_benchmark_output "${output}"
  printf '%s\t%s\t%s\n' "${round}" "${mode}" "${output}" >> "${order_file}"
}

run_dispatch_check enabled
run_dispatch_check disabled

for round in 1 2 3 4 5; do
  if ((round % 2 == 1)); then
    run_benchmark "${round}" enabled
    run_benchmark "${round}" disabled
  else
    run_benchmark "${round}" disabled
    run_benchmark "${round}" enabled
  fi
done

median_for() {
  local name=$1
  local mode=$2
  local values=()
  local file value
  for file in "${raw_directory}"/round-*-"${mode}".txt; do
    value=$(awk -v expected="${name}" '
      {
        name = $1
        sub(/-[0-9]+$/, "", name)
        if (name == expected) {
          for (field = 1; field <= NF; field++) {
            if ($field == "ns/op") {
              print $(field - 1)
              exit
            }
          }
        }
      }
    ' "${file}")
    if [[ -z ${value} ]]; then
      echo "missing ${name} sample in ${file}" >&2
      exit 1
    fi
    values+=("${value}")
  done
  if [[ ${#values[@]} -ne 5 ]]; then
    echo "expected five ${mode} samples for ${name}, found ${#values[@]}" >&2
    exit 1
  fi
  printf '%s\n' "${values[@]}" | LC_ALL=C sort -n | awk 'NR == 3 { print; exit }'
}

printf 'case\tenabled_samples\tdisabled_samples\tenabled_median_ns_op\tdisabled_median_ns_op\tpooled_speedup\tpaired_speedups\tpaired_median_speedup\n' > "${summary_file}"
for case_name in \
  BenchmarkFilePackedIntegerExtremaCount/min-max/FOR10 \
  BenchmarkFilePackedIntegerExtremaCountWide/min-max/FOR16; do
  enabled_values=()
  disabled_values=()
  for file in "${raw_directory}"/round-*-enabled.txt; do
    enabled_values+=("$(awk -v expected="${case_name}" '{ name = $1; sub(/-[0-9]+$/, "", name); if (name == expected) { for (field = 1; field <= NF; field++) if ($field == "ns/op") { print $(field - 1); exit } } }' "${file}")")
  done
  for file in "${raw_directory}"/round-*-disabled.txt; do
    disabled_values+=("$(awk -v expected="${case_name}" '{ name = $1; sub(/-[0-9]+$/, "", name); if (name == expected) { for (field = 1; field <= NF; field++) if ($field == "ns/op") { print $(field - 1); exit } } }' "${file}")")
  done
  if [[ ${#enabled_values[@]} -ne 5 || ${#disabled_values[@]} -ne 5 ]]; then
    echo "expected five enabled and disabled samples for ${case_name}" >&2
    exit 1
  fi
  enabled_samples=$(printf '%s,' "${enabled_values[@]}" | sed 's/,$//')
  disabled_samples=$(printf '%s,' "${disabled_values[@]}" | sed 's/,$//')
  enabled_median=$(median_for "${case_name}" enabled)
  disabled_median=$(median_for "${case_name}" disabled)
  pooled_speedup=$(awk -v enabled="${enabled_median}" -v disabled="${disabled_median}" 'BEGIN { printf "%.2f", disabled / enabled }')
  paired_speedups=()
  for ((sample = 0; sample < 5; sample++)); do
    paired_speedups+=("$(awk -v enabled="${enabled_values[${sample}]}" -v disabled="${disabled_values[${sample}]}" 'BEGIN { printf "%.17g", disabled / enabled }')")
  done
  paired_speedups_csv=$(printf '%s,' "${paired_speedups[@]}" | sed 's/,$//')
  paired_median_speedup=$(printf '%s\n' "${paired_speedups[@]}" | LC_ALL=C sort -n | awk 'NR == 3 { print; exit }')
  if ! awk -v speedup="${paired_median_speedup}" 'BEGIN { exit !(speedup + 0 >= 1.5) }'; then
    echo "${case_name} paired AVX2 speedup ${paired_median_speedup}x is below the required 1.50x" >&2
    exit 1
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${case_name}" "${enabled_samples}" "${disabled_samples}" \
    "${enabled_median}" "${disabled_median}" "${pooled_speedup}" \
    "${paired_speedups_csv}" "${paired_median_speedup}" >> "${summary_file}"
done

cat "${summary_file}"
