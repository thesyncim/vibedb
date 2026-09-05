#!/usr/bin/env python3
"""Verify a horizontal benchmark candidate after simultaneous RF3 process kills.

Reuses immutable executables and pinned runtime from a completed comparison.
This is a recovery diagnostic, never a throughput comparison. SIGKILL tests
process loss, not host power loss or loss of the operating-system page cache.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison', type=Path)
    parser.add_argument('output', type=Path)
    selected = parser.parse_args()
    source = selected.comparison.resolve()
    original_manifest = json.loads((source / 'manifest.json').read_text())
    if original_manifest['status'] != 'complete':
        parser.error('a completed immutable comparison is required')
    spec = importlib.util.spec_from_file_location('fixture', ROOT / 'scripts/bench/run-fused-node-comparison.py')
    f = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(f)
    dest = f.require_new_directory(selected.output)
    f.COMMAND_LOG = dest / 'control-commands.jsonl'
    controls = dest / 'controls'
    controls.mkdir()
    for path in (Path(__file__), ROOT / 'scripts/bench/run-fused-node-comparison.py', ROOT / 'scripts/bench/summarize-crdb-sql.py'):
        shutil.copy2(path, controls / path.name)
    args = f.parser().parse_args([
        str(dest), '--candidate-ref', original_manifest['refs']['after'], '--matrix', 'all',
        '--groups', '16', '--physical-nodes', '3', '--distributions', 'uniform',
        '--endpoint-modes', 'single', '--multigroup-workloads', 'mixed_uniform',
        '--multigroup-clients', '8', '--rows', '512', '--operations', '8000',
        '--warmup', '500', '--repetitions', '1'])
    args.validator_path = controls / 'summarize-crdb-sql.py'
    args.runtime_image_id = original_manifest['images']['runtime']['Id']
    binaries = source / 'after-bin'
    manifest = {
        'diagnostic': 'simultaneous SIGKILL of cluster supervisor and all three RF3 servers',
        'not_a_throughput_comparison': True, 'power_loss_tested': False,
        'revision': original_manifest['refs']['after'], 'binary_sha256': f.binary_hashes(binaries),
        'source_manifest': str(source / 'manifest.json'), 'cycles': [], 'status': 'starting',
    }
    original_popen = subprocess.Popen
    original_stop = f.stop_processes
    captured = {}

    def popen(command, *argv, **kwargs):
        if isinstance(command, list) and command[:2] == ['docker', 'exec'] and len(command) > 4:
            if command[3:5] == ['/bench/candidate-vibedb', 'cluster']:
                captured['server'] = list(command)
            if command[3] == '/bench/rf3-sqlbench' and '-phase' in command and command[command.index('-phase') + 1] == 'run':
                command = [*command, '-recovery-oracle', '/evidence/client-oracle.json']
                captured['client'] = list(command)
                manifest['client_argv'] = command[3:]
                f.write_json(dest / 'manifest.json', manifest)
        return original_popen(command, *argv, **kwargs)

    def stop_with_crash(processes, container, destination):
        logs = []
        try:
            report = json.loads((destination / 'report.json').read_text())
            if report['status'] != 'complete':
                raise f.RunnerError('recovery requires a fully verified pre-crash workload')
            f.run(['docker', 'cp', container + ':/evidence/client-oracle.json', dest / 'client-oracle.json'])
            for cycle in range(1, 3):
                inventory = f.process_inventory(container)
                f.save_inventory(dest, inventory, f'pre-crash-{cycle}')
                pids = []
                for line in inventory['executables']['text'].splitlines():
                    fields = line.split('\t', 1)
                    if len(fields) == 2 and fields[1] in {'/bench/candidate-vibedb', '/bench/candidate-vibedb-shard'}:
                        pids.append(fields[0])
                if len(pids) != 4 or not all(pid.isdigit() for pid in pids):
                    raise f.RunnerError('crash target must be one supervisor and three shard processes')
                record = {'cycle': cycle, 'killed_pids': pids, 'signal': 'SIGKILL', 'status': 'killing'}
                manifest['cycles'].append(record)
                f.write_json(dest / 'manifest.json', manifest)
                f.run(['docker', 'exec', container, 'kill', '-KILL', *pids])
                for process in processes:
                    if process.poll() is None:
                        process.wait(timeout=20)
                log_path = dest / f'restart-{cycle}.log'
                log = log_path.open('wb')
                logs.append(log)
                started = time.monotonic()
                process = subprocess.Popen(captured['server'], stdout=log, stderr=subprocess.STDOUT)
                processes.append(process)
                f.wait_for_marker(process, log_path, ['VibeDB development RF3 physical cluster ready:'], args.ready_timeout)
                f.wait_for_tcp_ports(container, [5432], args.ready_timeout)
                record['restart_ready_seconds'] = time.monotonic() - started
                client = list(captured['client'])
                client[client.index('-phase') + 1] = 'recovery'
                client[client.index('-output') + 1] = f'/evidence/recovery-{cycle}.json'
                at = client.index('-diagnostic-targets')
                del client[at:at + 2]
                with (dest / f'recovery-{cycle}.log').open('wb') as output:
                    result = f.run(client, stdout=output, stderr=subprocess.STDOUT, check=False)
                f.run(['docker', 'cp', container + f':/evidence/recovery-{cycle}.json', dest / f'recovery-{cycle}.json'])
                recovered = json.loads((dest / f'recovery-{cycle}.json').read_text())
                if result.returncode or recovered['status'] != 'complete':
                    raise f.RunnerError('recovered SQL differs from the pre-crash client oracle')
                record.update(status='verified', tables=16, rows=8192, fields_per_row=4)
                f.write_json(dest / 'manifest.json', manifest)
            manifest['status'] = 'verified'
        except Exception as exc:
            manifest.update(status='failed', recovery_error=str(exc))
            raise
        finally:
            f.write_json(dest / 'manifest.json', manifest)
            try:
                original_stop(processes, container, destination)
            finally:
                for log in logs:
                    log.close()
        return []

    subprocess.Popen = popen
    f.stop_processes = stop_with_crash
    cell = f.cell_matrix(args)[1]
    manifest['cell'] = cell
    f.write_json(dest / 'manifest.json', manifest)
    try:
        result = f.run_engine(args, cell, 'candidate', 'crash-recovery', binaries, dest,
                              f.schema_files(dest, cell['tables']), original_manifest['docker_architecture'])
    finally:
        subprocess.Popen = original_popen
        f.stop_processes = original_stop
    manifest['run'] = result
    if result['status'] != 'completed':
        manifest['status'] = 'failed'
    f.write_json(dest / 'manifest.json', manifest)
    print(manifest['status'], result['errors'], flush=True)
    if manifest['status'] != 'verified':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
