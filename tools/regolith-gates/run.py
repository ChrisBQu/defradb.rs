#!/usr/bin/env python3
"""Run frozen DefraLevel benchmark binaries and retain verifiable evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

# A profiling helper in /tmp may be named like a stdlib module (for example
# inspect.py). Do not let the artifact directory shadow Python's stdlib.
sys.path[:] = [entry for entry in sys.path if entry not in ('', '/tmp', '/private/tmp')]


_SCRIPT_DIR = Path(__file__).resolve().parent
WORKTREE = (_SCRIPT_DIR.parent.parent
            if _SCRIPT_DIR.name == 'regolith-gates' and _SCRIPT_DIR.parent.name == 'tools'
            else Path.cwd())
BINARY_ROOT = Path('/tmp/defra-perf-artifacts/bin')
GATE_ROOT = Path('/tmp/defra-perf-artifacts/gates')
PROFILE = 'release-fast'
WRITERS = '1,4,16,64'
OPERATIONS = '256'
REPETITIONS = '3'
ALLOCATION_REPETITIONS = 5
CRITERION_FILES = {
    'document_create/4': 'document_create/4/new/estimates.json',
    'document_create/16': 'document_create/16/new/estimates.json',
    'document_create_branchable/14': 'document_create_branchable/14/new/estimates.json',
        # Criterion keeps the logical ID slashes, but 0.8.2 replaces slashes
        # from the function ID when it creates the on-disk directory.
        'startup/cold_open/embedded/10_collections':
        'startup/cold_open_embedded_10_collections/new/estimates.json',
}
COMMANDS = [
    ('write_baseline', 'write_baseline', [],
     {'write_baseline_creates', 'write_baseline_shared_updates', 'write_baseline_counters_indexes'}, ()),
    ('document_write', 'document_write',
     ['--bench', 'document_create/(4|16)$|document_create_branchable/14$'],
     {'document_ops'}, tuple(CRITERION_FILES)[:3]),
    ('startup', 'startup', ['--bench', 'startup/cold_open/embedded/10_collections'],
     {'startup', 'disk_footprint'}, (tuple(CRITERION_FILES)[3],)),
]
COMMANDS += [
    (f'allocs-{i}', 'allocs', [], {'allocs'}, ())
    for i in range(1, ALLOCATION_REPETITIONS + 1)
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def no_builds() -> None:
    ps = subprocess.check_output(['ps', '-axo', 'comm'], text=True)
    active = [p.strip() for p in ps.splitlines()
              if Path(p.strip()).name in {'cargo', 'rustc', 'clippy-driver'}]
    if active:
        raise RuntimeError(f'Build processes are still active: {active[:8]}')


def ensure_clean_tree(worktree: Path) -> None:
    status = subprocess.check_output(['git', 'status', '--porcelain'], cwd=worktree, text=True)
    if status.strip():
        raise RuntimeError(f'Runner worktree is dirty: {status}')


def load_build_metadata(binary_dir: Path) -> tuple[dict, str, str]:
    json_path = binary_dir / 'BUILD.json'
    txt_path = binary_dir / 'BUILD.txt'
    if json_path.is_file():
        metadata = json.loads(json_path.read_text())
        revision = metadata.get('revision')
        if not revision:
            raise ValueError(f'{json_path} has no revision')
        if metadata.get('dirty') is not False:
            raise ValueError(f'{json_path} does not certify a clean source tree')
        command = metadata.get('command')
        if not isinstance(command, list) or not any(
                command[i:i + 2] == ['--profile', PROFILE]
                for i in range(max(0, len(command) - 1))):
            raise ValueError(f'{json_path} does not show a {PROFILE} build command')
        if metadata.get('executed') is not False:
            raise ValueError(f'{json_path} must explicitly certify the frozen executable was built with --no-run')
        targets = {command[i + 1] for i, part in enumerate(command[:-1]) if part == '--bench'}
        required = {'allocs', 'write_baseline', 'document_write', 'startup'}
        if not required <= targets:
            raise ValueError(f'{json_path} benchmark targets omit {sorted(required - targets)}')
        if not isinstance(metadata.get('sha256'), dict):
            raise ValueError(f'{json_path} has no executable hash map')
        return metadata, revision, PROFILE

    if not txt_path.is_file():
        raise FileNotFoundError(f'No BUILD.json or BUILD.txt under {binary_dir}')
    text = txt_path.read_text()
    revision_match = re.search(r'^Revision: ([0-9a-fA-F]{7,64})$', text, re.MULTILINE)
    profile_match = re.search(r'^Profile: ([^\r\n]+)$', text, re.MULTILINE)
    tree_match = re.search(r'^Tree: (.+)$', text, re.MULTILINE)
    command_match = re.search(r'^Build command: (.+)$', text, re.MULTILINE)
    if not revision_match or not profile_match or not tree_match or not command_match:
        raise ValueError(f'{txt_path} is missing explicit source/profile/command provenance')
    if profile_match.group(1) != PROFILE:
        raise ValueError(f'{txt_path} profile is {profile_match.group(1)!r}, expected {PROFILE!r}')
    if tree_match.group(1) != 'clean at build':
        raise ValueError(f'{txt_path} does not certify a clean source tree')
    command = command_match.group(1)
    if not all(f'--bench {name}' in command for name in ('allocs', 'write_baseline', 'document_write', 'startup')):
        raise ValueError(f'{txt_path} does not show all required benchmark targets')
    metadata = {'format': 'legacy-BUILD.txt', 'text': text}
    return metadata, revision_match.group(1), PROFILE


def parse_family_file(path: Path) -> list[dict]:
    if not path.is_file():
        raise ValueError(f'Missing Family output: {path}')
    records = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f'{path}:{line_number}: invalid Family JSON: {exc}') from exc
        if not isinstance(record, dict) or not isinstance(record.get('family'), str) or not isinstance(record.get('data'), dict):
            raise ValueError(f'{path}:{line_number}: malformed Family record')
        records.append(record)
    names = [record['family'] for record in records]
    if len(names) != len(set(names)):
        raise ValueError(f'{path}: duplicate Family names: {names}')
    return records


def verify_criterion_outputs(target_dir: Path, expected_ids: tuple[str, ...]) -> dict[str, dict]:
    found = {}
    for benchmark_id in expected_ids:
        relative = CRITERION_FILES[benchmark_id]
        path = target_dir / 'criterion' / relative
        if not path.is_file():
            raise ValueError(f'Criterion filter produced no estimates for {benchmark_id}: {path}')
        try:
            estimates = json.loads(path.read_text())
            # This harness reports Criterion's per-iteration median. The
            # mean is a separate estimate and would not match the frozen gate.
            point = estimates['median']['point_estimate']
            if (isinstance(point, bool) or not isinstance(point, (int, float))
                    or not math.isfinite(point) or point <= 0):
                raise ValueError('median.point_estimate is not a positive number')
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValueError(f'Invalid Criterion estimates for {benchmark_id}: {path}: {exc}') from exc
        found[benchmark_id] = {'path': str(path), 'sha256': sha256(path),
                               'median_point_estimate': point}
    return found


def allocation_create_value(records: list[dict]) -> float:
    family = next(record['data'] for record in records if record['family'] == 'allocs')
    values = []
    for group in family.get('groups', []):
        for row in group.get('rows', []):
            if row.get('name') == 'document create':
                value = row.get('value')
                if (not isinstance(value, bool) and isinstance(value, (int, float))
                        and math.isfinite(value) and value > 0):
                    values.append(float(value))
    if len(values) != 1:
        raise ValueError(f'allocs Family must have exactly one positive document create row, found {len(values)}')
    return values[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('labels', nargs='+')
    parser.add_argument('--worktree', type=Path, required=True)
    parser.add_argument('--binary-root', type=Path, default=BINARY_ROOT)
    parser.add_argument('--artifact-root', type=Path, default=GATE_ROOT)
    parser.add_argument('--guard-script', type=Path,
                        help='load guard script (defaults to WORKTREE/tools/perf-site/loadguard.py)')
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    for label in args.labels:
        no_builds()
        worktree = args.worktree.resolve()
        guard_script = (args.guard_script or (worktree / 'tools/perf-site/loadguard.py')).resolve()
        if not guard_script.is_file():
            raise FileNotFoundError(f'load guard script not found: {guard_script}')
        binary_dir = args.binary_root.resolve() / label
        build_metadata, revision, profile = load_build_metadata(binary_dir)
        executable_names = sorted({binary for _, binary, _, _, _ in COMMANDS})
        executables = {name: binary_dir / name for name in executable_names}
        for name, path in executables.items():
            if not path.is_file():
                raise FileNotFoundError(path)
            if isinstance(build_metadata.get('sha256'), dict) and name not in build_metadata['sha256']:
                raise ValueError(f'{binary_dir}/BUILD.json has no hash for {name}')
        recorded_hashes = {name: sha256(path) for name, path in executables.items()}
        built_hashes = build_metadata.get('sha256') if isinstance(build_metadata, dict) else None
        if isinstance(built_hashes, dict):
            for name, digest in recorded_hashes.items():
                recorded = built_hashes.get(name)
                if not isinstance(recorded, str) or recorded.lower() != digest:
                    raise ValueError(f'{binary_dir}/BUILD.json {name} hash mismatch: {recorded} != {digest}')

        ensure_clean_tree(worktree)
        subprocess.run(['git', 'checkout', '--detach', revision], cwd=worktree, check=True)
        checked_out = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=worktree, text=True).strip()
        if not checked_out.startswith(revision.lower()):
            raise RuntimeError(f'Checked out {checked_out}, build metadata requires {revision}')
        ensure_clean_tree(worktree)
        out = args.artifact_root.resolve() / label
        if out.exists() and not args.resume:
            raise RuntimeError(f'Refusing to overwrite prior evidence: {out}')
        out.mkdir(parents=True, exist_ok=True)

        meta = {
            'revision': revision,
            'label': label,
            'source_path': str(worktree),
            'profile': profile,
            'binary_paths': {name: str(path) for name, path in executables.items()},
            'sha256': recorded_hashes,
            'build_metadata': build_metadata,
            'criterion_target_dir': str(out / 'criterion-output'),
            'started_unix': time.time(),
            'commands': [],
            'guard_attempts': [],
        }
        run_path = out / 'RUN.json'
        if args.resume and run_path.exists():
            previous = json.loads(run_path.read_text())
            for key in ('revision', 'sha256', 'binary_paths', 'profile', 'criterion_target_dir'):
                if previous.get(key) != meta[key]:
                    raise ValueError(f'Resume provenance mismatch in {key}: {previous.get(key)!r} != {meta[key]!r}')
            meta = previous
            meta.setdefault('guard_attempts', [])
        run_path.write_text(json.dumps(meta, indent=2) + '\n')

        for name, binary, flags, expected_families, criterion_ids in COMMANDS:
            prior = [command for command in meta['commands'] if command['name'] == name]
            if prior:
                previous = prior[-1]
                if previous.get('exit_code') == 0 and previous.get('artifacts_valid') is True:
                    continue
                raise RuntimeError(f'Prior incomplete/failed command requires inspection: {label} {name}')

            no_builds()
            ensure_clean_tree(worktree)
            attempt_number = 1 + sum(attempt.get('name') == name for attempt in meta['guard_attempts'])
            guard_path = out / f'{name}-loadguard-attempt-{attempt_number:02d}.json'
            guard_started = time.time()
            guard_result = subprocess.run(
                ['python3', str(guard_script), '--out', str(guard_path)],
                capture_output=True, text=True)
            guard = None
            guard_error = None
            if guard_path.is_file():
                try:
                    guard = json.loads(guard_path.read_text())
                except (OSError, json.JSONDecodeError) as exc:
                    guard_error = str(exc)
            guard_passed = (guard_result.returncode == 0 and isinstance(guard, dict)
                            and guard.get('passed') is True)
            guard_attempt = {
                'name': name, 'attempt': attempt_number, 'path': str(guard_path),
                'return_code': guard_result.returncode, 'guard': guard,
                'stdout': guard_result.stdout, 'stderr': guard_result.stderr,
                'guard_sha256': sha256(guard_path) if guard_path.is_file() else None,
                'started_unix': guard_started, 'finished_unix': time.time(),
                'passed': guard_passed,
            }
            if guard_error:
                guard_attempt['error'] = guard_error
            meta['guard_attempts'].append(guard_attempt)
            run_path.write_text(json.dumps(meta, indent=2) + '\n')
            if not guard_passed:
                raise RuntimeError(f'Host guard attempt failed before {label} {name}: {guard_attempt}')
            command_record = {
                'name': name,
                'binary': binary,
                'binary_path': str(executables[binary]),
                'binary_sha256': recorded_hashes[binary],
                'argv': [str(executables[binary]), *flags],
                'guard_path': str(guard_path),
                'guard': guard,
                'guard_sha256': sha256(guard_path),
                'guard_attempt': attempt_number,
                'started_unix': time.time(),
                'exit_code': None,
                'artifacts_valid': False,
            }
            meta['commands'].append(command_record)
            run_path.write_text(json.dumps(meta, indent=2) + '\n')

            families_path = out / f'{name}.families.jsonl'
            log_path = out / f'{name}.log'
            env = dict(os.environ)
            env.update({
                'DEFRA_BASELINE_WRITERS': WRITERS,
                'DEFRA_BASELINE_OPERATIONS': OPERATIONS,
                'DEFRA_BASELINE_REPETITIONS': REPETITIONS,
                'DEFRA_BASELINE_PROFILE': PROFILE,
                'DEFRA_BASELINE_LOAD_GUARD': str(guard_path),
                # Per-command Criterion directories prevent stale estimates from
                # satisfying a later command's filter check.
                'CARGO_TARGET_DIR': str(out / 'criterion-output' / name),
                'DEFRA_BENCH_OUT': str(families_path),
            })
            command_record['log_path'] = str(log_path)
            command_record['families_path'] = str(families_path)
            with log_path.open('w') as log:
                result = subprocess.run(command_record['argv'], cwd=worktree, env=env,
                                         stdout=log, stderr=subprocess.STDOUT)
            command_record['exit_code'] = result.returncode
            command_record['finished_unix'] = time.time()
            command_record['log_sha256'] = sha256(log_path)
            run_path.write_text(json.dumps(meta, indent=2) + '\n')
            if result.returncode != 0:
                raise RuntimeError(f'Benchmark failed: {label} {name} exit={result.returncode}')
            ensure_clean_tree(worktree)

            records = parse_family_file(families_path)
            found = {record['family'] for record in records}
            if found != expected_families:
                raise ValueError(f'{label} {name} Family names {sorted(found)} != expected {sorted(expected_families)}')
            command_record['families'] = [
                {'name': record['family'], 'sha256': hashlib.sha256(
                    json.dumps(record, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
                for record in records
            ]
            command_record['families_sha256'] = sha256(families_path)
            command_record['criterion_target_dir'] = env['CARGO_TARGET_DIR']
            if name.startswith('allocs-'):
                command_record['document_create_allocations_per_op'] = allocation_create_value(records)
                command_record['alloc_sample'] = int(name.rsplit('-', 1)[1])
            if criterion_ids:
                command_record['criterion_estimates'] = verify_criterion_outputs(
                    Path(env['CARGO_TARGET_DIR']), criterion_ids)
            command_record['artifacts_valid'] = True
            run_path.write_text(json.dumps(meta, indent=2) + '\n')
            print(f'RUN {label} {name}', flush=True)
        meta['complete'] = True
        meta['finished_unix'] = time.time()
        run_path.write_text(json.dumps(meta, indent=2) + '\n')
        print(f'COMPLETE {label}', flush=True)


if __name__ == '__main__':
    main()
