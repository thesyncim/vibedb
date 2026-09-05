#!/usr/bin/env python3
"""Run a matched SIMD RF3 multigroup scheduler comparison against CockroachDB.

The reused fixture calls both VibeDB arms `candidate` internally. `arm` and
`revision` below are the authoritative before/after identities. Both arms have
identical process layout, client, diagnostics and resource settings.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
from pathlib import Path
import shutil
import tempfile

REPO = Path(__file__).resolve().parents[2]
CLIENT = REPO
CONTROL = CLIENT / 'scripts/bench/run-fused-node-comparison.py'


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('output', type=Path)
    cli.add_argument('--baseline-ref', required=True)
    cli.add_argument('--candidate-ref', required=True)
    cli.add_argument('--groups', type=int, default=16)
    cli.add_argument('--physical-nodes', default='3,6')
    cli.add_argument('--rows', type=int, default=512)
    cli.add_argument('--operations', type=int, default=4000)
    cli.add_argument('--warmup', type=int, default=500)
    cli.add_argument('--repetitions', type=int, default=2)
    cli.add_argument('--clients', default='8')
    selected = cli.parse_args()
    if not (2 <= selected.groups <= 63 and 64 <= selected.rows <= 1000000 and
            1 <= selected.operations <= 1000000 and 0 <= selected.warmup <= 100000 and
            1 <= selected.repetitions <= 20):
        cli.error('invalid bounded workload configuration')
    spec = importlib.util.spec_from_file_location('fixture', CONTROL)
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    args = fixture.parser().parse_args([
        str(selected.output), '--candidate-ref', selected.candidate_ref,
        '--repo', str(REPO), '--client-source', str(CLIENT),
        '--matrix', 'all', '--clients', selected.clients,
        '--multigroup-clients', selected.clients, '--groups', str(selected.groups),
        '--physical-nodes', selected.physical_nodes, '--distributions', 'uniform',
        '--endpoint-modes', 'single', '--multigroup-workloads', 'mixed_uniform',
        '--rows', str(selected.rows), '--operations', str(selected.operations),
        '--scans', '200', '--warmup', str(selected.warmup),
        '--repetitions', str(selected.repetitions), '--cpus', '12', '--memory', '24g',
        '--order', 'both', '--include-crdb', '--vibedb-sql-ports', '5432',
    ])
    for clients in fixture.parse_positive_csv(selected.clients, 'clients'):
        if clients > 15 or selected.operations < clients:
            cli.error('clients must be <=15 and have at least one operation each')
    fixture.cell_matrix(args)
    destination = fixture.require_new_directory(selected.output)
    fixture.COMMAND_LOG = destination / 'control-commands.jsonl'
    manifest = {
        'schema': 'vibedb.horizontal-scheduler-comparison/1',
        'workload_contract': {'rows_per_table': selected.rows, 'groups': selected.groups,
            'operations': selected.operations, 'scans': 200, 'warmup': selected.warmup,
            'repetitions': selected.repetitions, 'clients': selected.clients,
            'workloads': ['mixed_uniform']},
        'started_utc': datetime.now(timezone.utc).isoformat(),
        'status': 'preparing', 'runs': [],
        'arm_contract': 'before and after both use the existing candidate fused fixture; arm and revision identify source',
        'topology': 'RF3, one SQL endpoint, single Docker host; physical node counts: ' + selected.physical_nodes,
        'diagnostics': 'identical signal-acknowledged snapshots on both VibeDB arms',
        'limits': fixture.resource_limits(args.cpus, args.memory),
        'profiling': False,
        'orders': {'before-first': ['before', 'after', 'crdb'],
                   'after-first': ['crdb', 'after', 'before']},
    }
    try:
        controls = destination / 'controls'
        controls.mkdir()
        for source in (Path(__file__), CONTROL, CONTROL.with_name('summarize-crdb-sql.py')):
            shutil.copy2(source, controls / source.name)
        manifest['control_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in controls.iterdir()}
        args.validator_path = controls / 'summarize-crdb-sql.py'
        baseline = fixture.text_output(['git', 'rev-parse', selected.baseline_ref + '^{commit}'], cwd=REPO)
        candidate = fixture.text_output(['git', 'rev-parse', selected.candidate_ref + '^{commit}'], cwd=REPO)
        if baseline == candidate:
            raise fixture.RunnerError('distinct before/after commits required')
        fixture.run(['git', 'merge-base', '--is-ancestor', baseline, candidate], cwd=REPO)
        manifest['refs'] = {'before': baseline, 'after': candidate}
        with (destination / 'before-after.patch').open('wb') as patch:
            fixture.run(['git', 'diff', '--binary', baseline, candidate], cwd=REPO, stdout=patch)
        manifest['before_after_patch_sha256'] = hashlib.sha256((destination / 'before-after.patch').read_bytes()).hexdigest()
        client_info = fixture.git_info(CLIENT)
        if client_info['dirty']:
            raise fixture.RunnerError('shared client source must be clean')
        arch = fixture.docker_architecture()
        manifest['docker_architecture'] = arch
        manifest['vibedb_build_environment'] = {
            'GOOS': 'linux', 'GOARCH': arch, 'CGO_ENABLED': '0', 'GOEXPERIMENT': 'simd',
            'note': 'go_version identifies the cross-compiler host; benchmark executables run in Linux',
        }
        manifest['docker_host'] = fixture.docker_json(['docker', 'info', '--format', '{{json .}}'])
        with tempfile.TemporaryDirectory(prefix='vibedb-write-worktrees-') as tmp:
            work = Path(tmp)
            before, after = fixture.prepare_worktrees(REPO, baseline, candidate, work)
            client_tree = work / 'client'
            try:
                fixture.run(['git', 'worktree', 'add', '--detach', client_tree, client_info['revision']], cwd=CLIENT)
                sources = {'before': fixture.git_info(before), 'after': fixture.git_info(after),
                           'client': fixture.git_info(client_tree)}
                for name, info in sources.items():
                    fixture.write_git_evidence(destination, name, info)
                manifest['source'] = sources
                binaries, manifest['go_version'] = fixture.build_all(args, destination, before, after, client_tree, arch)
                manifest['images'] = {'runtime': fixture.ensure_image(fixture.RUNTIME),
                                      'crdb': fixture.ensure_image(fixture.CRDB)}
                args.runtime_image_id = manifest['images']['runtime']['Id']
                fixture.extract_crdb_binary(binaries)
                manifest['build_binary_sha256'] = fixture.binary_hashes(binaries)
                manifest['vibedb_build_metadata'] = {}
                for executable in sorted(binaries.iterdir()):
                    if executable.name == 'cockroach':
                        continue
                    metadata = fixture.text_output(['go', 'version', '-m', executable])
                    for setting in ('GOEXPERIMENT=simd', 'GOOS=linux', 'GOARCH=' + arch):
                        if '\tbuild\t' + setting not in metadata:
                            raise fixture.RunnerError(f'{executable.name} is missing required build setting {setting}')
                    manifest['vibedb_build_metadata'][executable.name] = metadata
                manifest['executable_formats'] = fixture.text_output([
                    'file', binaries / 'parent-vibedb-shard', binaries / 'candidate-vibedb-shard',
                    binaries / 'cockroach', binaries / 'rf3-sqlbench',
                ])
                arm_binaries = {}
                for arm, prefix in [('before', 'parent'), ('after', 'candidate')]:
                    target = destination / (arm + '-bin')
                    target.mkdir()
                    for suffix in ['vibedb', 'vibedb-shard', 'vibedb-gateway']:
                        shutil.copy2(binaries / (prefix + '-' + suffix), target / ('candidate-' + suffix))
                    shutil.copy2(binaries / 'rf3-sqlbench', target / 'rf3-sqlbench')
                    arm_binaries[arm] = target
                arm_binaries['crdb'] = binaries
                manifest['fixture_binary_sha256'] = {name: fixture.binary_hashes(path) for name, path in arm_binaries.items()}
                manifest['status'] = 'measuring'
                fixture.write_json(destination / 'manifest.json', manifest)
                cells = [cell for cell in fixture.cell_matrix(args) if cell['kind'] == 'multigroup']
                manifest['cells'] = cells
                for order, arms in manifest['orders'].items():
                    for cell in cells:
                        for arm in arms:
                            print(f'{order}: {arm} starting', flush=True)
                            run_dir = destination / cell['id'] / order / arm
                            schema = fixture.schema_files(run_dir, cell['tables'])
                            engine = 'crdb' if arm == 'crdb' else 'candidate'
                            manifest['active_run'] = {
                                'arm': arm, 'revision': manifest['refs'].get(arm, fixture.CRDB),
                                'order': order, 'cell': cell, 'evidence_path': str(run_dir.relative_to(destination)),
                            }
                            fixture.write_json(destination / 'manifest.json', manifest)
                            result = fixture.run_engine(args, cell, engine, order, arm_binaries[arm], run_dir, schema, arch)
                            result['arm'] = arm
                            result['revision'] = manifest['refs'].get(arm, fixture.CRDB)
                            manifest['runs'].append(result)
                            manifest.pop('active_run', None)
                            fixture.write_json(run_dir / 'run.json', result)
                            fixture.write_json(destination / 'manifest.json', manifest)
                            print(f"{order}: {arm} {result['status']} {result['errors']}", flush=True)
                            if result['status'] != 'completed' or result.get('client_exit_code') != 0:
                                raise fixture.RunnerError('stopped after incomplete fixture; retained evidence')
            finally:
                if client_tree.exists():
                    fixture.run(['git', 'worktree', 'remove', '--force', client_tree], cwd=CLIENT, check=False)
                fixture.cleanup_worktrees(REPO, work, before, after)
        manifest['status'] = 'complete'
        manifest['finished_utc'] = datetime.now(timezone.utc).isoformat()
        fixture.write_json(destination / 'manifest.json', manifest)
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        manifest['status'] = 'incomplete-or-failed'
        manifest['fatal_error'] = str(exc) or 'interrupted'
        fixture.write_json(destination / 'manifest.json', manifest)
        print(manifest['fatal_error'], flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
