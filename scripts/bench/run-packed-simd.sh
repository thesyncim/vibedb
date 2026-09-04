#!/usr/bin/env bash
# Run the packed-count SIMD qualification against precompiled base and head
# test binaries. Each output file is a raw go test benchmark result; this lane
# records evidence and deliberately makes no time-based pass/fail claim.
set -euo pipefail

if [[ $# -ne 5 ]]; then
	echo "usage: $0 EVIDENCE_DIRECTORY BASE_STOREIO_TEST HEAD_STOREIO_TEST BASE_QUERY_TEST HEAD_QUERY_TEST" >&2
	exit 2
fi

evidence=$1
base_storeio=$2
head_storeio=$3
base_query=$4
head_query=$5

if [[ ${evidence} != /* ]]; then
	echo "evidence directory must be an absolute path" >&2
	exit 2
fi
for binary in "${base_storeio}" "${head_storeio}" "${base_query}" "${head_query}"; do
	if [[ ! -x ${binary} ]]; then
		echo "benchmark binary is not executable: ${binary}" >&2
		exit 2
	fi
done

export GOEXPERIMENT=${GOEXPERIMENT:-simd}
benchtime=${PACKED_SIMD_BENCHTIME:-250ms}
repetitions=${PACKED_SIMD_REPETITIONS:-5}
if [[ ${repetitions} != 5 ]]; then
	echo "packed SIMD qualification requires exactly five repetitions" >&2
	exit 2
fi
if [[ ! ${benchtime} =~ ^2(0[0-9]|[1-4][0-9]|50)ms$ ]]; then
	echo "packed SIMD qualification requires a 200ms-250ms benchmark time" >&2
	exit 2
fi

raw_directory=${evidence}/raw
mkdir -m 700 -p "${raw_directory}"
commands_file=${evidence}/benchmark-commands.tsv
order_file=${evidence}/benchmark-order.tsv
printf 'round\trole\tpackage\toutput\n' > "${order_file}"
printf 'round\trole\tpackage\tcommand\n' > "${commands_file}"

# Cover the packed kernels, stream scans, and primary-stripe scans for all
# four specialized widths. Thresholds are measured separately from these
# full-column workloads.
storeio_bench='^BenchmarkCompact(Stream(Packed|Spelling|Integer)|PrimaryStripePacked)Equality(7|8|10|16)$'
query_bench='^BenchmarkFilePackedEqualityCount(Wide)?$'

record_command() {
	local round=$1
	local role=$2
	local package_name=$3
	shift 3
	{
		printf '%s\t%s\t%s\t' "${round}" "${role}" "${package_name}"
		printf '%q ' "$@"
		printf '\n'
	} >> "${commands_file}"
}

require_benchmark_metrics() {
	local output=$1
	if ! awk '
		/^Benchmark/ {
			lines++
			if ($0 !~ /ns\/op/ || $0 !~ /B\/op/ || $0 !~ /allocs\/op/) {
				bad=1
			}
		}
		END { exit(lines == 0 || bad != 0) }
	' "${output}"; then
		echo "benchmark output lacks ns/op, B/op, or allocs/op: ${output}" >&2
		return 1
	fi
}

validate_storeio() {
	local output=$1
	# These names are the established fixture surface on the current head. The
	# same regex and validation keep the 8/16 kernel additions in the run.
	local expected
	for expected in \
		BenchmarkCompactStreamPackedEquality7 \
		BenchmarkCompactStreamSpellingEquality7 \
		BenchmarkCompactStreamPackedEquality10 \
		BenchmarkCompactStreamIntegerEquality10 \
		BenchmarkCompactStreamPackedEquality8 \
		BenchmarkCompactStreamSpellingEquality8 \
		BenchmarkCompactStreamPackedEquality16 \
		BenchmarkCompactStreamIntegerEquality16 \
		BenchmarkCompactPrimaryStripePackedEquality7 \
		BenchmarkCompactPrimaryStripePackedEquality10 \
		BenchmarkCompactPrimaryStripePackedEquality8 \
		BenchmarkCompactPrimaryStripePackedEquality16; do
		if ! grep -Eq "^${expected}(-[0-9]+)?[[:space:]]" "${output}"; then
			echo "missing packed-count fixture benchmark ${expected}: ${output}" >&2
			return 1
		fi
	done
	if grep -Eq '^BenchmarkCompactStreamPackedEqualityThresholds' "${output}"; then
		echo "threshold benchmark unexpectedly selected: ${output}" >&2
		return 1
	fi
	require_benchmark_metrics "${output}"
}

validate_query() {
	local output=$1
	local expected
	for expected in \
		'BenchmarkFilePackedEqualityCount/label/dictionary7' \
		'BenchmarkFilePackedEqualityCount/n/FOR10' \
		'BenchmarkFilePackedEqualityCountWide/label/dictionary8' \
		'BenchmarkFilePackedEqualityCountWide/n/FOR16'; do
		if ! grep -Eq "^${expected}(-[0-9]+)?[[:space:]]" "${output}"; then
			echo "missing packed-count query case ${expected}: ${output}" >&2
			return 1
		fi
	done
	require_benchmark_metrics "${output}"
}

run_benchmark() {
	local round=$1
	local role=$2
	local package_name=$3
	local binary=$4
	local bench_expression=$5
	local output=${raw_directory}/round-$(printf '%02d' "${round}")-${role}-${package_name}.txt
	local -a args=(
		-test.run '^$'
		-test.bench "${bench_expression}"
		-test.benchtime "${benchtime}"
		-test.count=1
		-test.cpu=1
		-test.benchmem
	)
	record_command "${round}" "${role}" "${package_name}" "${binary}" "${args[@]}"
	printf '%s\n' "round=${round} role=${role} package=${package_name} output=${output}"
	# This invocation is intentionally synchronous. All test binaries were
	# compiled before this function is entered, so no build can overlap timing.
	"${binary}" "${args[@]}" > "${output}" 2>&1
	case ${package_name} in
		storeio) validate_storeio "${output}" ;;
		query) validate_query "${output}" ;;
		*) echo "unknown benchmark package: ${package_name}" >&2; return 2 ;;
	esac
	printf '%s\n' "validated=${output}"
	printf '%s\t%s\t%s\t%s\n' "${round}" "${role}" "${package_name}" "${output}" >> "${order_file}"
}

for ((round = 1; round <= repetitions; round++)); do
	if ((round % 2 == 1)); then
		roles=(base head)
	else
		roles=(head base)
	fi
	for role in "${roles[@]}"; do
		case ${role} in
			base)
				storeio_binary=${base_storeio}
				query_binary=${base_query}
				;;
			head)
				storeio_binary=${head_storeio}
				query_binary=${head_query}
				;;
			*) echo "unknown role: ${role}" >&2; exit 2 ;;
		esac
		run_benchmark "${round}" "${role}" storeio "${storeio_binary}" "${storeio_bench}"
		run_benchmark "${round}" "${role}" query "${query_binary}" "${query_bench}"
	done
done
