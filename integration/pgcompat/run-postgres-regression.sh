#!/usr/bin/env bash
set -euo pipefail

export GOEXPERIMENT=${GOEXPERIMENT:-simd}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

# shellcheck source=postgres.env
source "${script_dir}/postgres.env"

suite="smoke"
selected_tests=""
output_root="${script_dir}/artifacts"
corpus_dir="${VIBEDB_PG_CORPUS_DIR:-}"
per_test_timeout=20

usage() {
  printf '%s\n' \
    'usage: run-postgres-regression.sh [options]' \
    '  --suite smoke|full       smoke selection or every upstream scheduled test' \
    '  --tests a,b,c            explicit upstream test names (overrides --suite)' \
    '  --output DIR             evidence root (default: integration/pgcompat/artifacts)' \
    '  --corpus DIR             reuse an existing checkout of the pinned PostgreSQL source' \
    '  --per-test-timeout SEC   hard timeout for each SQL script (default: 20)'
}

while (($#)); do
  case "$1" in
    --suite)
      suite="${2:?missing --suite value}"
      shift 2
      ;;
    --tests)
      selected_tests="${2:?missing --tests value}"
      shift 2
      ;;
    --output)
      output_root="${2:?missing --output value}"
      shift 2
      ;;
    --corpus)
      corpus_dir="${2:?missing --corpus value}"
      shift 2
      ;;
    --per-test-timeout)
      per_test_timeout="${2:?missing --per-test-timeout value}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$suite" != "smoke" && "$suite" != "full" ]]; then
  printf 'suite must be smoke or full, got %s\n' "$suite" >&2
  exit 2
fi
if [[ ! "$per_test_timeout" =~ ^[1-9][0-9]*$ ]]; then
  printf 'per-test timeout must be a positive integer\n' >&2
  exit 2
fi

if [[ -z "$corpus_dir" ]]; then
  cache_parent="${XDG_CACHE_HOME:-${TMPDIR:-/tmp}/vibedb-pgcompat}"
  corpus_dir="${cache_parent}/${POSTGRES_COMMIT}"
fi
if [[ ! -d "${corpus_dir}/.git" ]]; then
  mkdir -p "$(dirname "$corpus_dir")"
  git clone --quiet --depth 1 --branch "$POSTGRES_TAG" "$POSTGRES_REPOSITORY" "$corpus_dir"
fi
actual_commit="$(git -C "$corpus_dir" rev-parse HEAD)"
if [[ "$actual_commit" != "$POSTGRES_COMMIT" ]]; then
  printf 'PostgreSQL corpus revision mismatch: got %s, want %s\n' \
    "$actual_commit" "$POSTGRES_COMMIT" >&2
  exit 2
fi

sql_dir="${corpus_dir}/src/test/regress/sql"
expected_dir="${corpus_dir}/src/test/regress/expected"
schedule_file="${corpus_dir}/src/test/regress/parallel_schedule"
for required in "$sql_dir" "$expected_dir" "$schedule_file"; do
  if [[ ! -e "$required" ]]; then
    printf 'incomplete PostgreSQL corpus: missing %s\n' "$required" >&2
    exit 2
  fi
done

if [[ -n "$selected_tests" ]]; then
  test_words="${selected_tests//,/ }"
elif [[ "$suite" == "full" ]]; then
  test_words="$(awk '/^test:/ { for (i = 2; i <= NF; i++) print $i }' "$schedule_file" | tr '\n' ' ')"
else
  # This is a broad, bounded cross-section of the unmodified upstream corpus.
  # Dependencies stay in upstream schedule order.
  test_words='test_setup boolean int4 int8 numeric text varchar strings insert insert_conflict select join aggregates transactions update delete returning window with json jsonb'
fi
read -r -a tests <<< "$test_words"
if ((${#tests[@]} == 0)); then
  printf 'no tests selected\n' >&2
  exit 2
fi

run_stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
run_dir="${output_root%/}/${run_stamp}"
mkdir -p "$run_dir/results" "$run_dir/diffs"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/vibedb-pgcompat.XXXXXX")"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT INT TERM

(cd "$script_dir" && go build -o "$work_dir/vibedb-pgtest-server" ./cmd/vibedb-pgtest-server)
ready_file="$work_dir/ready"
"$work_dir/vibedb-pgtest-server" \
  -catalog "$work_dir/catalog.vdb" \
  -ready-file "$ready_file" \
  >"$run_dir/server.log" 2>&1 &
server_pid=$!
for _ in {1..200}; do
  [[ -s "$ready_file" ]] && break
  if ! kill -0 "$server_pid" 2>/dev/null; then
    printf 'VibeDB test server exited before readiness\n' >&2
    cat "$run_dir/server.log" >&2
    exit 2
  fi
  sleep 0.05
done
if [[ ! -s "$ready_file" ]]; then
  printf 'timed out waiting for VibeDB test server\n' >&2
  exit 2
fi
address="$(tr -d '\r\n' < "$ready_file")"
host="${address%:*}"
port="${address##*:}"

if [[ -n "${VIBEDB_PSQL_IMAGE:-}" ]]; then
  docker pull "${VIBEDB_PSQL_IMAGE}" >/dev/null
  psql_command=(docker run --rm -i --pull=never --network host
    --volume "${corpus_dir}:/postgres-source:ro"
    -e LC_MESSAGES=C
    -e PGCLIENTENCODING=UTF8
    -e PGTZ=America/Los_Angeles
    -e PGDATESTYLE='Postgres, MDY'
    -e PGSSLMODE=disable
    -e PGGSSENCMODE=disable
    -e PGAPPNAME
    -e PG_ABS_SRCDIR=/postgres-source/src/test/regress
    -e PG_LIBDIR=
    -e PG_DLSUFFIX=
    "${VIBEDB_PSQL_IMAGE}" psql)
  "${psql_command[@]}" --version > "$run_dir/psql-version.txt"
else
  psql_bin="${PSQL:-psql}"
  if ! command -v "$psql_bin" >/dev/null 2>&1; then
    printf 'psql is required (or set VIBEDB_PSQL_IMAGE to the pinned image)\n' >&2
    exit 2
  fi
  psql_command=("$psql_bin")
  "${psql_command[@]}" --version > "$run_dir/psql-version.txt"
fi

run_bounded() {
  local seconds="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=ALRM "$seconds" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout --signal=ALRM "$seconds" "$@"
  else
    perl -e '$seconds = shift @ARGV; alarm $seconds; exec @ARGV' "$seconds" "$@"
  fi
}

printf 'test\tstatus\texit\tactual_errors\texpected_errors\tunsupported_hints\n' > "$run_dir/results.tsv"
printf '# PostgreSQL %s compatibility report\n\n' "$POSTGRES_TAG" > "$run_dir/report.md"
printf '%s\n\n' \
  "Corpus: \`${POSTGRES_REPOSITORY}@${POSTGRES_COMMIT}\`" \
  "Client: \`$(tr -d '\r\n' < "$run_dir/psql-version.txt")\`" \
  "VibeDB revision: \`$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)\`" \
  "Suite: \`${suite}\` (${#tests[@]} scripts, ${per_test_timeout}s maximum each)" \
  >> "$run_dir/report.md"
printf '| Test | Result | Actual/expected ERROR lines | Unsupported hints |\n|---|---:|---:|---:|\n' >> "$run_dir/report.md"

exact_count=0
mismatch_count=0
timeout_count=0
client_failure_count=0

for test_name in "${tests[@]}"; do
  if [[ ! "$test_name" =~ ^[A-Za-z0-9_.-]+$ ]]; then
    printf 'unsafe test name: %s\n' "$test_name" >&2
    exit 2
  fi
  input_file="${sql_dir}/${test_name}.sql"
  canonical_expected_file="${expected_dir}/${test_name}.out"
  actual_file="${run_dir}/results/${test_name}.out"
  diff_file="${run_dir}/diffs/${test_name}.diff"
  if [[ ! -f "$input_file" || ! -f "$canonical_expected_file" ]]; then
    printf 'upstream test %s is missing SQL or expected output\n' "$test_name" >&2
    exit 2
  fi

  set +e
  run_bounded "$per_test_timeout" env \
    LC_MESSAGES=C \
    PGCLIENTENCODING=UTF8 \
    PGTZ=America/Los_Angeles \
    PGDATESTYLE='Postgres, MDY' \
    PGSSLMODE=disable \
    PGGSSENCMODE=disable \
    PGAPPNAME="pg_regress/${test_name}" \
    PG_ABS_SRCDIR="${corpus_dir}/src/test/regress" \
    PG_LIBDIR='' \
    PG_DLSUFFIX='' \
    "${psql_command[@]}" \
      -X -a -q \
      -v HIDE_TABLEAM=on \
      -v HIDE_TOAST_COMPRESSION=on \
      --host "$host" \
      --port "$port" \
      --username regress \
      --dbname regression \
      < "$input_file" > "$actual_file" 2>&1
  command_status=$?
  set -e

  # pg_regress accepts canonical output plus numbered alternatives. Match the
  # same rule and, on failure, retain the smallest diff as the useful gap.
  expected_file="$canonical_expected_file"
  matched_expected=''
  best_diff="$work_dir/${test_name}.best.diff"
  diff -u "$expected_file" "$actual_file" > "$best_diff" || true
  best_diff_lines="$(wc -l < "$best_diff" | tr -d ' ')"
  if cmp -s "$expected_file" "$actual_file"; then
    matched_expected="$expected_file"
  else
    for alternative_number in {0..9}; do
      alternative_expected="${canonical_expected_file%.out}_${alternative_number}.out"
      [[ -f "$alternative_expected" ]] || continue
      if cmp -s "$alternative_expected" "$actual_file"; then
        expected_file="$alternative_expected"
        matched_expected="$alternative_expected"
        break
      fi
      candidate_diff="$work_dir/${test_name}.${alternative_number}.diff"
      diff -u "$alternative_expected" "$actual_file" > "$candidate_diff" || true
      candidate_diff_lines="$(wc -l < "$candidate_diff" | tr -d ' ')"
      if ((candidate_diff_lines < best_diff_lines)); then
        expected_file="$alternative_expected"
        best_diff_lines="$candidate_diff_lines"
        cp "$candidate_diff" "$best_diff"
      fi
    done
  fi

  actual_errors="$(grep -c '^ERROR:' "$actual_file" || true)"
  expected_errors="$(grep -c '^ERROR:' "$expected_file" || true)"
  unsupported_hints="$(grep -Eic '^ERROR:.*(not supported|unsupported feature|not implemented)' "$actual_file" || true)"
  status='mismatch'
  if [[ "$command_status" -eq 124 || "$command_status" -eq 142 ]]; then
    status='timeout'
    ((timeout_count += 1))
  elif [[ "$command_status" -ne 0 ]]; then
    status='client-failure'
    ((client_failure_count += 1))
  elif [[ -n "$matched_expected" ]]; then
    status='exact'
    ((exact_count += 1))
  else
    ((mismatch_count += 1))
  fi
  if [[ "$status" != 'exact' ]]; then
    cp "$best_diff" "$diff_file"
  fi

  printf '%s\t%s\t%d\t%s\t%s\t%s\n' \
    "$test_name" "$status" "$command_status" "$actual_errors" "$expected_errors" "$unsupported_hints" \
    >> "$run_dir/results.tsv"
  printf '| `%s` | %s | %s/%s | %s |\n' \
    "$test_name" "$status" "$actual_errors" "$expected_errors" "$unsupported_hints" \
    >> "$run_dir/report.md"
  printf '%-28s %s\n' "$test_name" "$status"
done

approved_failures=0
while IFS= read -r approved; do
  [[ -z "$approved" || "$approved" == \#* ]] && continue
  approved_status="$(awk -F '\t' -v name="$approved" '$1 == name { print $2 }' "$run_dir/results.tsv")"
  if [[ -z "$approved_status" && "$suite" != 'full' ]]; then
    continue
  fi
  if [[ "$approved_status" != 'exact' ]]; then
    printf 'approved upstream test regressed: %s (%s)\n' "$approved" "${approved_status:-not-run}" >&2
    ((approved_failures += 1))
  fi
done < "$script_dir/approved-tests.txt"

grep -h '^ERROR:' "$run_dir"/results/*.out \
  | sed -E 's/^ERROR:[[:space:]]*//' \
  | sort \
  | uniq -c \
  | sort -nr \
  > "$run_dir/error-signatures.txt" || true

cat > "$run_dir/summary.env" <<EOF
POSTGRES_TAG=${POSTGRES_TAG}
POSTGRES_COMMIT=${POSTGRES_COMMIT}
TEST_COUNT=${#tests[@]}
EXACT_COUNT=${exact_count}
MISMATCH_COUNT=${mismatch_count}
TIMEOUT_COUNT=${timeout_count}
CLIENT_FAILURE_COUNT=${client_failure_count}
APPROVED_FAILURE_COUNT=${approved_failures}
EOF

printf '\nExact: %d; mismatched: %d; timed out: %d; client failures: %d.\n' \
  "$exact_count" "$mismatch_count" "$timeout_count" "$client_failure_count" >> "$run_dir/report.md"
printf '\n## Most frequent observed errors\n\n```text\n' >> "$run_dir/report.md"
head -25 "$run_dir/error-signatures.txt" >> "$run_dir/report.md"
printf '```\n' >> "$run_dir/report.md"
printf '\nEvidence: %s\n' "$run_dir"

if ((approved_failures > 0 || timeout_count > 0 || client_failure_count > 0)); then
  exit 1
fi
