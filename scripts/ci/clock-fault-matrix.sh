#!/usr/bin/env bash
set -euo pipefail

evidence_dir="${VIBEDB_CLOCK_FAULT_EVIDENCE:?set VIBEDB_CLOCK_FAULT_EVIDENCE to an absolute directory}"
case "${evidence_dir}" in
  /*) ;;
  *) echo "clock-fault evidence directory must be absolute" >&2; exit 2 ;;
esac

mkdir -p "${evidence_dir}"
test_root="${TMPDIR:-${evidence_dir}/tmp}"
mkdir -p "${test_root}"

run_gate() {
  gate="$1"
  package="$2"
  test_name="$3"
  timeout="$4"
  output="${evidence_dir}/${gate}.jsonl"
  go test -json -count=1 -timeout="${timeout}" -run="^${test_name}$" "${package}" | tee "${output}"
  test "$(jq -r --arg test_name "${test_name}" \
    'select(.Action == "pass" and .Test == $test_name) | .Test' "${output}" | wc -l)" -eq 1
  if jq -e --arg test_name "${test_name}" \
    'select(.Action == "skip" and .Test == $test_name)' "${output}" >/dev/null; then
    echo "clock-fault gate skipped: ${test_name}" >&2
    exit 1
  fi
}

git rev-parse HEAD > "${evidence_dir}/revision.txt"
go version > "${evidence_dir}/go-version.txt"
go env GOOS GOARCH GOVERSION GOTOOLCHAIN > "${evidence_dir}/go-env.txt"
uname -a > "${evidence_dir}/uname.txt"

run_gate utc_tls ./internal/rafttransport TestPeerTLSIndependentUTCStepMatrix 2m
run_gate logical_pulse ./gateway TestRecoveryManifestMissingPageRequiresLogicalPulsesAcrossRestart 2m
run_gate transaction_recovery ./internal/raftservice TestTwoRealRF3GroupsExecuteFusedTwoParticipantTransactionAcrossLeaderIsolation 3m
run_gate foreground_suspend ./internal/raftservice TestRF3NativeServingThreeProcessRecoveryEvidence 4m

qualification="${evidence_dir}/shipped-rf3-qualification.tsv"
VIBEDB_RF3_QUALIFICATION_PATH="${qualification}" \
  run_gate shipped_suspend ./cmd/vibedb-shard TestServeRF3ShippedFaultHarness 6m
test -s "${qualification}"
test "$(stat -c %s "${qualification}")" -le 65536

cat > "${evidence_dir}/matrix.tsv" <<'EOF'
schema	vibedb.clock-fault-matrix	1
result	pass
fault	independent_utc_steps	fail_closed_tls_handshake
fault	certificate_validity_boundaries	fail_closed_tls_handshake
fault	logical_pulse_stall_restart	one_replicated_recovery_outcome
fault	leader_isolation_reelection	exact_transaction_retry
fault	process_suspend_resume	follower_catchup_and_progress
fault	former_leader_resume	linearizable_read_refused
fault	foreground_failover_latency	bounded_by_test_contract
contract	global_timestamp	not_offered
contract	live_process_utc_step	not_injected
EOF
test "$(stat -c %s "${evidence_dir}/matrix.tsv")" -le 65536

