#!/usr/bin/env python3
"""Validate and compare the frozen DefraLevel timing/allocation evidence.

Usage:
  python3 tools/regolith-gates/analyze.py --main main-532aac3f \
      LABEL_IN_ORDER... [--json OUT.json] [--markdown OUT.md]

Labels must be ordered from main through each immediate predecessor. Structural
validity and observed strict non-regression are reported separately. No noise
tolerance is inferred from the observations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys
from typing import Any

sys.path[:] = [p for p in sys.path if p not in ('', '/tmp', '/private/tmp')]

GATE_ROOT = Path(os.environ.get('DEFRA_ANALYZER_GATE_ROOT', '/tmp/defra-perf-artifacts/gates'))
BINARY_ROOT = Path(os.environ.get('DEFRA_ANALYZER_BINARY_ROOT', '/tmp/defra-perf-artifacts/bin'))
WORKLOADS = ('creates', 'shared_updates', 'counters_indexes')
WRITERS = (1, 4, 16, 64)
REPETITIONS = 3
OPERATIONS = 256
ALLOC_REPETITIONS = 5
EXPECTED_COMMANDS = (
    'write_baseline', 'document_write', 'startup',
    'allocs-1', 'allocs-2', 'allocs-3', 'allocs-4', 'allocs-5',
)
EXPECTED_FAMILIES = {
    'write_baseline': {'write_baseline_creates', 'write_baseline_shared_updates', 'write_baseline_counters_indexes'},
    'document_write': {'document_ops'},
    'startup': {'startup', 'disk_footprint'},
    **{f'allocs-{i}': {'allocs'} for i in range(1, ALLOC_REPETITIONS + 1)},
}
EXPECTED_CRITERION = {
    'document_write': {
        'document_create/4': 'document_create/4/new/estimates.json',
        'document_create/16': 'document_create/16/new/estimates.json',
        'document_create_branchable/14': 'document_create_branchable/14/new/estimates.json',
    },
    'startup': {
        'startup/cold_open/embedded/10_collections':
            'startup/cold_open_embedded_10_collections/new/estimates.json',
    },
}
HISTORICAL_218 = {
    'document_create/4': 34_600.0,
    'document_create/16': 122_500.0,
    'document_create_branchable/14': 47_400.0,
    'startup/cold_open/embedded/10_collections': 14_000_000.0,
}
EARLIER_218 = {
    'document_create/4': 36_800.0,
    'document_create/16': 127_700.0,
    'document_create_branchable/14': 48_500.0,
    'startup/cold_open/embedded/10_collections': 15_200_000.0,
}
HISTORICAL_MAIN_CREATE_ALLOCATIONS = 2997.0


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def json_file(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f'{label}: cannot read valid JSON at {path}: {exc}') from exc


def parse_records(path: Path, label: str, *, family: bool = False) -> tuple[list[dict], list[dict]]:
    if not path.is_file():
        raise ValueError(f'{label}: missing records file {path}')
    records: list[dict] = []
    ignored: list[dict] = []
    for line_no, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            if line.lstrip().startswith(('{', '[')):
                raise ValueError(f'{label}:{line_no}: malformed JSON line: {exc}') from exc
            ignored.append({'line': line_no, 'text': line})
            continue
        if not isinstance(item, dict):
            raise ValueError(f'{label}:{line_no}: expected a JSON object')
        if family:
            if not isinstance(item.get('family'), str) or not isinstance(item.get('data'), dict):
                raise ValueError(f'{label}:{line_no}: malformed Family record')
        records.append(item)
    return records, ignored


def read_run(label: str) -> dict[str, Any]:
    out = GATE_ROOT / label
    log = out / 'write_baseline.log'
    if not log.is_file():
        raise ValueError(f'{label}: missing {log}')
    records, ignored = parse_records(log, label)
    run_rows = [record['baseline_run'] for record in records if 'baseline_run' in record]
    samples = [record['baseline_sample'] for record in records if 'baseline_sample' in record]
    if len(run_rows) != 1 or not isinstance(run_rows[0], dict):
        raise ValueError(f'{label}: expected one baseline_run record, found {len(run_rows)}')
    if any(not isinstance(sample, dict) for sample in samples):
        raise ValueError(f'{label}: baseline_sample records must be JSON objects')
    families = {}
    for command_name in EXPECTED_COMMANDS:
        path = out / f'{command_name}.families.jsonl'
        if path.is_file():
            families[command_name] = {
                'path': path,
                'sha256': sha256(path),
                'records': parse_records(path, f'{label}/{command_name}', family=True)[0],
            }
    manifest_path = out / 'RUN.json'
    manifest = json_file(manifest_path, label) if manifest_path.is_file() else None
    return {
        'label': label,
        'path': str(log),
        'log_sha256': sha256(log),
        'run': run_rows[0],
        'samples': samples,
        'ignored_log_lines': ignored,
        'manifest': manifest,
        'families': families,
    }


def schema_of(rows: list[dict]) -> str:
    # Step 5 includes engine_exhaustions but retains retry-layer HTTP accounting.
    return ('request-conflicts-engine-exhaustions' if any('client_conflicts' in r for r in rows)
            else 'legacy-http-retry-conflicts')


def number(metrics: dict, name: str) -> float | None:
    value = metrics.get(name)
    return float(value) if not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) else None


def count_value(value: Any, context: str, errors: list[str]) -> int | None:
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(float(value)) or float(value) < 0
            or not float(value).is_integer()):
        errors.append(f'{context}: expected a nonnegative integer count, got {value!r}')
        return None
    return int(value)


def metrics_of(row: dict, schema: str) -> dict[str, Any]:
    metrics = row.get('metrics') or {}
    standard = {
        'commits_per_second': number(metrics, 'commits'),
        'storage_conflicts_per_commit': number(metrics, 'storage conflicts'),
        'latency_p50_ms': number(metrics, 'latency p50'),
        'latency_p99_ms': number(metrics, 'latency p99'),
        'retry_metrics_per_commit': {
            name: number(metrics, name)
            for name in metrics
            if 'retries' in name.lower()
        },
    }
    if schema == 'request-conflicts-engine-exhaustions':
        commits = row.get('commits')
        client = row.get('client_conflicts')
        engine = row.get('engine_exhaustions')
        standard.update({
            'client_conflicts_per_commit': (
                float(client) / commits if isinstance(client, int) and commits else None),
            'client_conflict_requests': float(client) if isinstance(client, int) else None,
            'engine_exhaustions': float(engine) if isinstance(engine, int) else None,
            'engine_exhaustions_per_commit': (
                float(engine) / commits if isinstance(engine, int) and commits else None),
            'legacy_http_conflicts_per_commit': None,
            'legacy_http_exhausted_requests': None,
            'legacy_http_exhausted_per_commit': None,
        })
    else:
        http_exhausted = number(metrics, 'HTTP exhausted requests')
        if http_exhausted is None:
            http_exhausted = number(metrics, 'exhausted requests')
        engine = row.get('engine_exhaustions')
        if engine is None:
            engine = number(metrics, 'engine exhausted requests')
        standard.update({
            'client_conflicts_per_commit': None,
            'client_conflict_requests': None,
            'engine_exhaustions': float(engine) if isinstance(engine, (int, float)) else None,
            'engine_exhaustions_per_commit': (
                float(engine) / float(row.get('commits'))
                if isinstance(engine, (int, float)) and isinstance(row.get('commits'), (int, float))
                and row.get('commits') else None),
            'legacy_http_conflicts_per_commit': number(metrics, 'HTTP conflicts'),
            'legacy_http_exhausted_requests': http_exhausted,
            'legacy_http_exhausted_per_commit': (
                http_exhausted / float(row['commits'])
                if http_exhausted is not None and isinstance(row.get('commits'), (int, float))
                and row.get('commits') else None),
        })
    return standard


def command_expectations(command: dict) -> tuple[set[str], dict[str, str]]:
    name = command['name']
    return EXPECTED_FAMILIES.get(name, set()), EXPECTED_CRITERION.get(name, {})


def validate_manifest(rec: dict, errors: list[str]) -> dict[str, Any]:
    run = rec['run']
    manifest = rec['manifest']
    label = rec['label']
    if not isinstance(manifest, dict):
        errors.append('missing or malformed RUN.json')
        return {'commands': {}, 'build_profile': None, 'binary_hashes': {}}
    revision = run.get('revision')
    if manifest.get('revision') != revision:
        errors.append(f'RUN.json revision {manifest.get("revision")!r} differs from baseline_run revision {revision!r}')
    if manifest.get('profile') != 'release-fast':
        errors.append(f'RUN.json profile is {manifest.get("profile")!r}, expected release-fast')
    build = manifest.get('build_metadata')
    if not isinstance(build, dict):
        errors.append('RUN.json lacks copied build metadata')
        build = {}
    if build.get('format') == 'legacy-BUILD.txt':
        text = build.get('text', '')
        build_revision_match = re.search(r'^Revision: ([0-9a-fA-F]{7,64})$', text, re.MULTILINE)
        profile_match = re.search(r'^Profile: ([^\r\n]+)$', text, re.MULTILINE)
        tree_match = re.search(r'^Tree: (.+)$', text, re.MULTILINE)
        build_revision = build_revision_match.group(1) if build_revision_match else None
        build_profile = profile_match.group(1) if profile_match else None
        if not tree_match or tree_match.group(1) != 'clean at build':
            errors.append('legacy BUILD.txt does not certify a clean tree')
        command_match = re.search(r'^Build command: (.+)$', text, re.MULTILINE)
        command_text = command_match.group(1) if command_match else ''
        required = ('--bench allocs', '--bench write_baseline', '--bench document_write', '--bench startup')
        if any(target not in command_text for target in required):
            errors.append('legacy BUILD.txt build command omits one or more required benchmark targets')
    else:
        build_revision = build.get('revision')
        build_profile = build.get('profile')
        command = build.get('command')
        if build_profile is None and isinstance(command, list):
            build_profile = next((command[i + 1] for i, part in enumerate(command[:-1])
                                  if part == '--profile'), None)
        if build.get('dirty') is not False:
            errors.append('copied BUILD.json does not certify a clean tree')
        required_benches = {'allocs', 'write_baseline', 'document_write', 'startup'}
        built_benches = {command[i + 1] for i, part in enumerate(command[:-1])
                         if part == '--bench'} if isinstance(command, list) else set()
        if not required_benches <= built_benches:
            errors.append(f'BUILD.json bench targets {sorted(built_benches)} omit {sorted(required_benches - built_benches)}')
        if build.get('executed') is not False:
            errors.append('BUILD.json must certify frozen executables were built with --no-run')
    if not build_revision or not revision or not (str(build_revision).startswith(str(revision)) or str(revision).startswith(str(build_revision))):
        errors.append(f'build metadata revision {build_revision!r} does not match run revision {revision!r}')
    if build_profile != 'release-fast':
        errors.append(f'build metadata profile is {build_profile!r}, expected release-fast')

    recorded_hashes = manifest.get('sha256') or {}
    binary_paths = manifest.get('binary_paths') or {}
    build_hashes = build.get('sha256') or {}
    if build.get('format') != 'legacy-BUILD.txt' and not isinstance(build_hashes, dict):
        errors.append('BUILD.json executable hash map is missing')
    if not isinstance(recorded_hashes, dict) or not isinstance(binary_paths, dict):
        errors.append('RUN.json binary path/hash maps are malformed')
        recorded_hashes, binary_paths = {}, {}
    for name in ('allocs', 'write_baseline', 'document_write', 'startup'):
        path = Path(binary_paths.get(name, ''))
        digest = recorded_hashes.get(name)
        if not path.is_file():
            errors.append(f'missing frozen {name} executable at {path}')
        elif not isinstance(digest, str) or not re.fullmatch(r'[0-9a-fA-F]{64}', digest):
            errors.append(f'missing/invalid RUN.json SHA-256 for {name}')
        elif sha256(path) != digest.lower():
            errors.append(f'{name} executable hash does not match RUN.json')
        if build.get('format') != 'legacy-BUILD.txt' and name not in build_hashes:
            errors.append(f'BUILD.json has no recorded hash for {name}')
        if isinstance(build_hashes, dict) and name in build_hashes and digest != str(build_hashes[name]).lower():
            errors.append(f'{name} executable hash differs from copied BUILD.json')

    command_records = manifest.get('commands')
    if not isinstance(command_records, list):
        errors.append('RUN.json commands is not an array')
        command_records = []
    commands = {}
    attempt_records = manifest.get('guard_attempts')
    if not isinstance(attempt_records, list):
        errors.append('RUN.json guard_attempts is not an array')
        attempt_records = []
    attempts: dict[tuple[str, int], dict] = {}
    attempt_paths: set[Path] = set()
    for attempt in attempt_records:
        if not isinstance(attempt, dict) or not isinstance(attempt.get('name'), str):
            errors.append(f'malformed guard attempt record: {attempt!r}')
            continue
        name = attempt['name']
        index = attempt.get('attempt')
        if isinstance(index, bool) or not isinstance(index, int) or index < 1:
            errors.append(f'{name}: invalid guard attempt index {index!r}')
            continue
        key = (name, index)
        if key in attempts:
            errors.append(f'{name}: duplicate guard attempt {index}')
        attempts[key] = attempt
        rc = attempt.get('return_code')
        if isinstance(rc, bool) or not isinstance(rc, int):
            errors.append(f'{name} guard attempt {index}: return code must be an integer')
        if (isinstance(attempt.get('started_unix'), bool) or not isinstance(attempt.get('started_unix'), (int, float))
                or isinstance(attempt.get('finished_unix'), bool) or not isinstance(attempt.get('finished_unix'), (int, float))
                or attempt.get('finished_unix', 0) < attempt.get('started_unix', 0)):
            errors.append(f'{name} guard attempt {index}: invalid start/finish timestamps')
        path = Path(attempt.get('path', ''))
        if path.resolve() != (GATE_ROOT / label / f'{name}-loadguard-attempt-{index:02d}.json').resolve():
            errors.append(f'{name} guard attempt {index}: path is outside expected run artifact')
        if path.resolve() in attempt_paths:
            errors.append(f'{name}: duplicate guard attempt file {path}')
        attempt_paths.add(path.resolve())
        try:
            guard = json_file(path, f'{label}/{name} guard attempt {index}')
            passed = (not isinstance(rc, bool) and rc == 0 and guard.get('passed') is True)
            if (attempt.get('guard') != guard or attempt.get('guard_sha256') != sha256(path)
                    or attempt.get('passed') is not passed):
                errors.append(f'{name} guard attempt {index}: guard content/hash/result mismatch')
        except ValueError as exc:
            errors.append(str(exc))
    for name in set(key[0] for key in attempts):
        indices = sorted(index for attempt_name, index in attempts if attempt_name == name)
        if indices != list(range(1, len(indices) + 1)):
            errors.append(f'{name}: nonconsecutive guard attempt numbers {indices}')
    expected_binary = {
        'write_baseline': 'write_baseline', 'document_write': 'document_write',
        'startup': 'startup', **{f'allocs-{i}': 'allocs' for i in range(1, ALLOC_REPETITIONS + 1)},
    }
    seen_order = []
    for command in command_records:
        if not isinstance(command, dict) or not isinstance(command.get('name'), str):
            errors.append(f'malformed RUN.json command record: {command!r}')
            continue
        name = command['name']
        seen_order.append(name)
        if name in commands:
            errors.append(f'duplicate command record {name}')
        commands[name] = command
        if isinstance(command.get('exit_code'), bool) or command.get('exit_code') != 0:
            errors.append(f'{name}: command exit status is {command.get("exit_code")!r}, expected 0')
        if (isinstance(command.get('started_unix'), bool) or not isinstance(command.get('started_unix'), (int, float))
                or isinstance(command.get('finished_unix'), bool) or not isinstance(command.get('finished_unix'), (int, float))
                or command['finished_unix'] < command['started_unix']):
            errors.append(f'{name}: missing or invalid command start/finish timestamps')
        if command.get('artifacts_valid') is not True:
            errors.append(f'{name}: runner did not validate command artifacts')
        guard_path = Path(command.get('guard_path', ''))
        attempt_index = command.get('guard_attempt')
        if isinstance(attempt_index, bool) or not isinstance(attempt_index, int):
            attempt_index = -1
        guard_attempt = attempts.get((name, attempt_index), {})
        try:
            guard = json_file(guard_path, f'{label}/{name} load guard')
            if (guard.get('passed') is not True or command.get('guard') != guard
                    or command.get('guard_sha256') != sha256(guard_path)
                    or not guard_attempt.get('passed')
                    or guard_attempt.get('path') != str(guard_path)
                    or guard_attempt.get('guard') != guard
                    or isinstance(guard.get('cores'), bool) or not isinstance(guard.get('cores'), int) or guard['cores'] <= 0
                    or not isinstance(guard.get('loadavg'), list) or len(guard['loadavg']) != 3
                    or any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                           for value in guard.get('loadavg', []))
                    or isinstance(guard.get('ceiling'), bool) or not isinstance(guard.get('ceiling'), (int, float))
                    or not math.isclose(guard['ceiling'], guard['cores'] * 0.5, rel_tol=1e-9)
                    or guard.get('loadavg', [float('inf')])[0] > guard.get('ceiling', -1)):
                errors.append(f'{name}: load guard did not pass')
        except ValueError as exc:
            errors.append(str(exc))
        executable = Path(command.get('binary_path', ''))
        digest = command.get('binary_sha256')
        if not executable.is_file() or not isinstance(digest, str) or sha256(executable) != digest:
            errors.append(f'{name}: command executable path/hash is invalid')
        expected_executable = expected_binary.get(name)
        if command.get('binary') != expected_binary.get(name):
            errors.append(f'{name}: binary name {command.get("binary")!r} differs from expected {expected_binary.get(name)!r}')
        if expected_executable is None or executable.name != expected_executable:
            errors.append(f'{name}: executable {executable.name!r} is not expected {expected_executable!r}')
        elif Path(binary_paths.get(expected_executable, '')).resolve() != executable.resolve():
            errors.append(f'{name}: command executable differs from the RUN.json binary path')
        if expected_executable and recorded_hashes.get(expected_executable) != digest:
            errors.append(f'{name}: command executable hash differs from RUN.json binary hash')
        expected_argv = [str(executable)]
        if name == 'document_write':
            expected_argv += ['--bench', 'document_create/(4|16)$|document_create_branchable/14$']
        elif name == 'startup':
            expected_argv += ['--bench', 'startup/cold_open/embedded/10_collections']
        if command.get('argv') != expected_argv:
            errors.append(f'{name}: argv {command.get("argv")!r} differs from expected {expected_argv!r}')
        log_path = Path(command.get('log_path', ''))
        if (not log_path.is_file() or log_path.stat().st_size == 0
                or command.get('log_sha256') != sha256(log_path)):
            errors.append(f'{name}: command log is missing or empty')
        family_path = Path(command.get('families_path', ''))
        evidence_root = GATE_ROOT / label
        if log_path.resolve() != (evidence_root / f'{name}.log').resolve():
            errors.append(f'{name}: log path is outside the expected run artifact')
        if family_path.resolve() != (evidence_root / f'{name}.families.jsonl').resolve():
            errors.append(f'{name}: Family path is outside the expected run artifact')
        expected_names, criterion_ids = command_expectations(command)
        try:
            family_records, _ = parse_records(family_path, f'{label}/{name}', family=True)
            actual_names = {record['family'] for record in family_records}
            if len(actual_names) != len(family_records):
                errors.append(f'{name}: duplicate Family records')
            if actual_names != expected_names:
                errors.append(f'{name}: Family names {sorted(actual_names)} != {sorted(expected_names)}')
            if command.get('families_sha256') != sha256(family_path):
                errors.append(f'{name}: Family file SHA-256 mismatch')
            expected_record_hashes = [
                {'name': record['family'], 'sha256': hashlib.sha256(
                    json.dumps(record, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
                for record in family_records
            ]
            if command.get('families') != expected_record_hashes:
                errors.append(f'{name}: per-Family record hashes differ from Family output')
            if name.startswith('allocs-'):
                values = [row.get('value') for record in family_records if record.get('family') == 'allocs'
                          for group in record['data'].get('groups', [])
                          for row in group.get('rows', []) if row.get('name') == 'document create']
                if (len(values) != 1 or isinstance(values[0], bool)
                        or not isinstance(values[0], (int, float)) or not math.isfinite(values[0])
                        or values[0] <= 0 or values[0] != command.get('document_create_allocations_per_op')):
                    errors.append(f'{name}: allocation row is missing or differs from RUN.json value')
                sample_index = int(name.rsplit('-', 1)[1])
                if command.get('alloc_sample') != sample_index:
                    errors.append(f'{name}: allocation sample index mismatch')
        except (ValueError, OSError) as exc:
            errors.append(str(exc))
        if criterion_ids:
            estimates = command.get('criterion_estimates')
            if not isinstance(estimates, dict) or set(estimates) != set(criterion_ids):
                errors.append(f'{name}: Criterion IDs {sorted(estimates or {})} != {sorted(criterion_ids)}')
            else:
                target_dir = Path(command.get('criterion_target_dir', ''))
                if not target_dir.is_dir():
                    errors.append(f'{name}: missing isolated Criterion output directory {target_dir}')
                expected_relatives = {Path('criterion') / relative for relative in criterion_ids.values()}
                actual_relatives = {path.relative_to(target_dir) for path in target_dir.glob('criterion/**/new/estimates.json')}
                if actual_relatives != expected_relatives:
                    errors.append(f'{name}: Criterion estimate IDs {sorted(map(str, actual_relatives))} != {sorted(map(str, expected_relatives))}')
                for benchmark_id, relative in criterion_ids.items():
                    record = estimates[benchmark_id]
                    path = target_dir / 'criterion' / relative
                    if not path.is_file() or record.get('path') != str(path):
                        errors.append(f'{name}: expected Criterion estimate missing for {benchmark_id}')
                    elif sha256(path) != record.get('sha256'):
                        errors.append(f'{name}: Criterion estimate hash mismatch for {benchmark_id}')
                    else:
                        try:
                            data = json_file(path, f'{name}/{benchmark_id}')
                            estimate = data['median']['point_estimate']
                            if (isinstance(estimate, bool) or not isinstance(estimate, (int, float))
                                    or not math.isfinite(estimate) or estimate <= 0
                                    or estimate != record.get('median_point_estimate')):
                                errors.append(f'{name}: Criterion estimate value mismatch for {benchmark_id}')
                        except (ValueError, KeyError, TypeError) as exc:
                            errors.append(f'{name}: invalid Criterion estimate {benchmark_id}: {exc}')
    if set(commands) != set(EXPECTED_COMMANDS):
        errors.append(f'command set {sorted(commands)} != expected {sorted(EXPECTED_COMMANDS)}')
    if seen_order != list(EXPECTED_COMMANDS):
        errors.append(f'command order {seen_order} != expected {list(EXPECTED_COMMANDS)}')
    if manifest.get('complete') is not True:
        errors.append('RUN.json does not mark the complete gate run')

    alloc_commands = [commands.get(f'allocs-{i}', {}) for i in range(1, ALLOC_REPETITIONS + 1)]
    allocation_values = [command.get('document_create_allocations_per_op') for command in alloc_commands]
    if len(allocation_values) != ALLOC_REPETITIONS or any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0
                                                          for value in allocation_values):
        errors.append('expected five positive document-create allocation samples')
    return {'commands': commands, 'build_profile': build_profile,
            'binary_hashes': recorded_hashes,
            'allocation_samples': allocation_values if len(allocation_values) == ALLOC_REPETITIONS else []}


def validate_samples(rec: dict, errors: list[str]) -> dict[str, Any]:
    run, rows = rec['run'], rec['samples']
    schema = schema_of(rows)
    if run.get('dirty') is not False:
        errors.append(f'baseline_run dirty is {run.get("dirty")!r}, expected false')
    if run.get('profile') != 'release-fast':
        errors.append(f'baseline_run profile is {run.get("profile")!r}, expected release-fast')
    if run.get('durability') != 'Immediate' or run.get('store') != 'disk':
        errors.append('baseline_run does not certify on-disk Immediate durability')
    if run.get('operations_per_writer') != OPERATIONS or run.get('repetitions') != REPETITIONS:
        errors.append('baseline_run operations/repetitions mismatch')
    if tuple(run.get('writers') or ()) != WRITERS:
        errors.append(f'baseline_run writers {run.get("writers")!r} != {WRITERS}')
    guard = run.get('load_guard')
    if not isinstance(guard, dict) or guard.get('passed') is not True:
        errors.append('write_baseline embedded load guard did not pass')
    expected = {(w, workload, rep) for w in WRITERS for workload in WORKLOADS for rep in range(REPETITIONS)}
    seen: dict[tuple[int, str, int], int] = {}
    normalized_rows = []
    expected_metric_names: set[str] | None = None
    for row in rows:
        try:
            if isinstance(row.get('writers'), bool) or not isinstance(row.get('writers'), int):
                raise ValueError('writers must be encoded as an integer')
            if isinstance(row.get('repetition'), bool) or not isinstance(row.get('repetition'), int):
                raise ValueError('repetition must be encoded as an integer')
            writers = row['writers']
            workload = str(row['workload'])
            repetition = row['repetition']
            key = (writers, workload, repetition)
        except (KeyError, TypeError, ValueError):
            errors.append(f'invalid sample key: {row!r}')
            continue
        seen[key] = seen.get(key, 0) + 1
        if key not in expected:
            errors.append(f'unexpected sample key {key}')
        requests = count_value(row.get('requests'), f'{key} requests', errors)
        commits = count_value(row.get('commits'), f'{key} commits', errors)
        expected_requests = writers * OPERATIONS
        if requests != expected_requests:
            errors.append(f'{key}: requests={requests!r}, expected {expected_requests}')
        if isinstance(row.get('elapsed_seconds'), bool) or not isinstance(row.get('elapsed_seconds'), (int, float)) or not math.isfinite(row['elapsed_seconds']) or row['elapsed_seconds'] <= 0:
            errors.append(f'{key}: invalid elapsed_seconds={row.get("elapsed_seconds")!r}')
        metrics = row.get('metrics')
        if not isinstance(metrics, dict):
            errors.append(f'{key}: missing metrics object')
            metrics = {}
        metric_names = set(metrics)
        if expected_metric_names is None:
            expected_metric_names = metric_names
        elif metric_names != expected_metric_names:
            errors.append(f'{key}: metric schema {sorted(metric_names)} differs from other sample rows')
        required = {'commits', 'storage conflicts', 'latency p50', 'latency p99'}
        if schema == 'request-conflicts-engine-exhaustions':
            required |= {
                'client conflicts', 'client conflict requests', 'engine exhausted requests',
                'HTTP retries (retired)', 'embedded retries (retired)',
                'short ID reservation retries (retired)', 'merge retries (retired)',
                'push marker retries',
            }
            client = count_value(row.get('client_conflicts'), f'{key} client_conflicts', errors)
            engine = count_value(row.get('engine_exhaustions'), f'{key} engine_exhaustions', errors)
            if requests is not None and commits is not None and client is not None and engine is not None and requests != commits + client + engine:
                errors.append(f'{key}: requests do not equal commits + client conflicts + engine exhaustion')
            if client is not None and metrics.get('client conflict requests') != client:
                errors.append(f'{key}: client conflict metric disagrees with row count')
            if engine is not None and metrics.get('engine exhausted requests') != engine:
                errors.append(f'{key}: engine exhaustion metric disagrees with row count')
            for retired in ('HTTP retries (retired)', 'embedded retries (retired)',
                            'short ID reservation retries (retired)', 'merge retries (retired)'):
                value = number(metrics, retired)
                if value != 0:
                    errors.append(f'{key}: retired layer {retired} must be zero, got {value!r}')
        else:
            required |= {'HTTP conflicts', 'HTTP retries', 'embedded retries', 'merge retries', 'push marker retries'}
            http_key = 'HTTP exhausted requests' if 'HTTP exhausted requests' in metrics else 'exhausted requests'
            if http_key not in metrics:
                errors.append(f'{key}: missing legacy HTTP exhaustion metric')
                http_exhausted = None
            else:
                http_exhausted = count_value(metrics.get(http_key), f'{key} {http_key}', errors)
            engine_raw = row.get('engine_exhaustions', metrics.get('engine exhausted requests'))
            engine = count_value(engine_raw, f'{key} engine_exhaustions', errors) if engine_raw is not None else 0
            if requests is not None and commits is not None and http_exhausted is not None and engine is not None and requests != commits + http_exhausted + engine:
                errors.append(f'{key}: requests do not equal commits + HTTP exhaustion + engine exhaustion')
            http_conflicts = number(metrics, 'HTTP conflicts')
            http_retries = number(metrics, 'HTTP retries')
            if http_conflicts is not None and http_retries is not None and http_exhausted is not None and commits:
                if not math.isclose(http_conflicts * commits, http_retries * commits + http_exhausted,
                                    rel_tol=1e-9, abs_tol=1e-7):
                    errors.append(f'{key}: legacy HTTP conflicts do not conserve retries + exhausted requests')
        if any(name not in metrics for name in required):
            errors.append(f'{key}: missing required metrics {sorted(required - metrics.keys())}')
        for name in ('commits', 'storage conflicts', 'latency p50', 'latency p99'):
            value = number(metrics, name)
            if value is None or value < 0:
                errors.append(f'{key}: invalid metric {name}={metrics.get(name)!r}')
        normalized_rows.append({'key': key, 'row': row, 'metrics': metrics_of(row, schema)})
        if commits is not None and commits > 0 and isinstance(row.get('elapsed_seconds'), (int, float)):
            expected_rate = commits / row['elapsed_seconds']
            actual_rate = number(metrics, 'commits')
            if actual_rate is None or not math.isclose(actual_rate, expected_rate, rel_tol=1e-9, abs_tol=1e-9):
                errors.append(f'{key}: commits/s metric disagrees with commits/elapsed_seconds')
        if schema == 'request-conflicts-engine-exhaustions' and commits:
            client = row.get('client_conflicts')
            expected_client_rate = client / commits if isinstance(client, int) else None
            actual_client_rate = number(metrics, 'client conflicts')
            if expected_client_rate is not None and (actual_client_rate is None or not math.isclose(
                    actual_client_rate, expected_client_rate, rel_tol=1e-9, abs_tol=1e-9)):
                errors.append(f'{key}: client-conflicts/commit metric disagrees with request count')
    for key in sorted(expected):
        if seen.get(key, 0) != 1:
            errors.append(f'{key}: expected one sample, found {seen.get(key, 0)}')

    # Families are the dashboard transport; verify they agree with the raw NDJSON samples.
    write_family = rec['families'].get('write_baseline')
    if write_family:
        records = write_family['records']
        expected_names = EXPECTED_FAMILIES['write_baseline']
        actual_names = {item['family'] for item in records}
        if actual_names != expected_names:
            errors.append(f'write_baseline Family names {sorted(actual_names)} != {sorted(expected_names)}')
        for item in records:
            scenario = item['family'].removeprefix('write_baseline_')
            groups = item['data'].get('groups', [])
            for group in groups:
                metric_name = group.get('name')
                for row in group.get('rows', []):
                    try:
                        writers = int(row['name'])
                        values = [r['row'].get('metrics', {}).get(metric_name) for r in normalized_rows
                                  if r['key'][0] == writers and r['key'][1] == scenario]
                        raw_values = [v for v in values if not isinstance(v, bool)
                                      and isinstance(v, (int, float)) and math.isfinite(v)]
                        if len(raw_values) != REPETITIONS:
                            errors.append(f'{scenario}/{writers}/{metric_name}: expected {REPETITIONS} raw samples, found {len(raw_values)}')
                        else:
                            expected_value = statistics.median(raw_values)
                            if not math.isclose(float(row['value']), expected_value, rel_tol=0, abs_tol=1e-9):
                                errors.append(f'{scenario}/{writers}/{metric_name}: Family median differs from raw sample median')
                    except (KeyError, TypeError, ValueError):
                        errors.append(f'malformed write_baseline Family row: {row!r}')
        family_metrics = {
            group.get('name')
            for item in records for group in item.get('data', {}).get('groups', [])
        }
        if expected_metric_names is not None and family_metrics != expected_metric_names:
            errors.append(f'write_baseline Family metric set {sorted(family_metrics)} != raw metric set {sorted(expected_metric_names)}')
        for item in records:
            for group in item.get('data', {}).get('groups', []):
                group_names = [g.get('name') for g in item.get('data', {}).get('groups', [])]
                if len(group_names) != len(set(group_names)):
                    errors.append(f"{item['family']}: duplicate metric groups")
                writer_names = [row.get('name') for row in group.get('rows', [])]
                if writer_names != [str(writer) for writer in WRITERS]:
                    errors.append(f"{item['family']}/{group.get('name')}: writer rows {writer_names} != {[str(w) for w in WRITERS]}")
    else:
        errors.append('missing write_baseline Family output')
    return {'schema': schema, 'expected_samples': len(expected), 'observed_samples': len(rows),
            'unique_sample_keys': len(seen), 'normalized_rows': normalized_rows}


def summarize(rec: dict, normalized: dict) -> dict[str, Any]:
    rows = normalized['normalized_rows']
    groups: dict[tuple[str, int], list[dict]] = {}
    for item in rows:
        workload, writer_count, _ = item['key'][1], item['key'][0], item['key'][2]
        groups.setdefault((workload, writer_count), []).append(item)
    grouped = {}
    for key, items in sorted(groups.items(), key=lambda pair: (WORKLOADS.index(pair[0][0]) if pair[0][0] in WORKLOADS else 99, pair[0][1])):
        names = items[0]['metrics'].keys()
        medians = {}
        for name in names:
            values = [item['metrics'][name] for item in items]
            if name == 'retry_metrics_per_commit':
                retry_names = set().union(*(value.keys() for value in values))
                medians[name] = {retry_name: statistics.median(
                    [value[retry_name] for value in values if value.get(retry_name) is not None])
                    for retry_name in sorted(retry_names)
                    if any(value.get(retry_name) is not None for value in values)}
            else:
                finite = [value for value in values if isinstance(value, (int, float)) and math.isfinite(value)]
                medians[name] = statistics.median(finite) if finite else None
        grouped[f'{key[0]}|writers={key[1]}'] = {
            'sample_count': len(items), 'medians': medians,
            'raw_samples': [item['row'] for item in sorted(items, key=lambda i: i['key'][2])],
        }
    scaling = {}
    for workload in WORKLOADS:
        rates = [grouped.get(f'{workload}|writers={count}', {}).get('medians', {}).get('commits_per_second')
                 for count in WRITERS]
        complete = all(grouped.get(f'{workload}|writers={count}', {}).get('sample_count') == REPETITIONS
                       for count in WRITERS)
        monotonic = None if not complete or any(rate is None for rate in rates) else all(a < b for a, b in zip(rates, rates[1:]))
        scaling[workload] = {
            'throughput_by_writer_count': dict(zip(WRITERS, rates)),
            'complete_sample_coverage': complete,
            'strict_monotonic_median_throughput': monotonic,
            'gate_status': ('observed_pass' if monotonic is True else 'observed_fail' if monotonic is False else 'incomplete'),
        }
    creates_coverage = all(grouped.get(f'creates|writers={writer}', {}).get('sample_count') == REPETITIONS for writer in WRITERS)
    schema = normalized['schema']
    create_rows = [item['row'] for item in rows if item['key'][1] == 'creates']
    creates_zero = None
    if creates_coverage and schema == 'request-conflicts-engine-exhaustions':
        creates_zero = all(
            number(item['row'].get('metrics', {}), 'storage conflicts') == 0
            and item['row'].get('client_conflicts') == 0
            and item['row'].get('engine_exhaustions') == 0
            for item in [item for item in normalized['normalized_rows'] if item['key'][1] == 'creates']
        )
    legacy_create_zero = None
    if creates_coverage and schema == 'legacy-http-retry-conflicts':
        legacy_create_zero = all(
            number(item.get('metrics', {}), 'storage conflicts') == 0
            and number(item.get('metrics', {}), 'HTTP conflicts') == 0
            and (number(item.get('metrics', {}), 'HTTP exhausted requests')
                 if 'HTTP exhausted requests' in item.get('metrics', {})
                 else number(item.get('metrics', {}), 'exhausted requests')) == 0
            for item in create_rows
        )
    create_zero = creates_zero if schema == 'request-conflicts-engine-exhaustions' else legacy_create_zero
    create_gate_status = ('incomplete' if not creates_coverage or create_zero is None
                          else 'observed_pass' if create_zero else 'observed_fail')
    allocations = [rec['manifest_validated']['commands'].get(f'allocs-{i}', {}).get(
        'document_create_allocations_per_op') for i in range(1, ALLOC_REPETITIONS + 1)]
    alloc_median = statistics.median(allocations) if len(allocations) == ALLOC_REPETITIONS else None
    return {'schema': schema, 'groups': grouped, 'scaling': scaling,
            'create_sample_coverage_complete': creates_coverage,
            'current_create_conflicts_zero_proven': creates_zero,
            'legacy_create_conflict_indicators_zero': legacy_create_zero,
            'create_conflicts_gate_status': create_gate_status,
            'allocation_samples': allocations, 'allocation_median': alloc_median}


def comparable_gate_values(summary: dict) -> dict[tuple[str, str], dict[str, float]]:
    values = {}
    for key, cell in summary['groups'].items():
        workload, writer_str = key.split('|writers=')
        medians = cell['medians']
        base = {
            'commits_per_second': medians.get('commits_per_second'),
            'storage_conflicts_per_commit': medians.get('storage_conflicts_per_commit'),
            'latency_p50_ms': medians.get('latency_p50_ms'),
            'latency_p99_ms': medians.get('latency_p99_ms'),
        }
        if medians.get('client_conflicts_per_commit') is not None:
            base['client_conflicts_per_commit'] = medians['client_conflicts_per_commit']
        if medians.get('legacy_http_conflicts_per_commit') is not None:
            base['legacy_http_conflicts_per_commit'] = medians['legacy_http_conflicts_per_commit']
        if medians.get('legacy_http_exhausted_per_commit') is not None:
            base['legacy_http_exhausted_per_commit'] = medians['legacy_http_exhausted_per_commit']
        if medians.get('engine_exhaustions_per_commit') is not None:
            base['engine_exhaustions_per_commit'] = medians['engine_exhaustions_per_commit']
        if medians.get('engine_exhaustions') is not None:
            base['engine_exhaustions'] = medians['engine_exhaustions']
        values[(workload, writer_str)] = base
    return values


GATE_METRICS = {
    'commits_per_second',
    'storage_conflicts_per_commit',
    'client_conflicts_per_commit',
    'legacy_http_conflicts_per_commit',
    'legacy_http_exhausted_per_commit',
    'engine_exhaustions_per_commit',
}


def nonregression(reference: dict, candidate: dict) -> dict[str, Any]:
    ref_values = comparable_gate_values(reference['summary'])
    cand_values = comparable_gate_values(candidate['summary'])
    details, regressions, missing, not_comparable = [], [], [], []
    incomparable_gate_metric = False
    for cell, candidate_metrics in cand_values.items():
        reference_metrics = ref_values.get(cell)
        if reference_metrics is None:
            missing.append(f'{cell}: reference sample group missing')
            continue
        for metric in sorted(set(candidate_metrics) | set(reference_metrics)):
            value = candidate_metrics.get(metric)
            reference_value = reference_metrics.get(metric)
            if value is None or reference_value is None:
                record = {
                    'workload': cell[0], 'writers': int(cell[1]), 'metric': metric,
                    'candidate_schema': candidate['summary']['schema'],
                    'reference_schema': reference['summary']['schema'],
                    'reason': 'metric absent in one source schema; kept distinct, not conflated',
                    'gate_metric': metric in GATE_METRICS,
                }
                not_comparable.append(record)
                incomparable_gate_metric |= metric in GATE_METRICS
                continue
            is_gate = metric in GATE_METRICS
            passed = (value >= reference_value if metric == 'commits_per_second'
                      else value <= reference_value) if is_gate else None
            record = {'workload': cell[0], 'writers': int(cell[1]), 'metric': metric,
                      'reference_median': reference_value, 'candidate_median': value,
                      'delta': value - reference_value, 'gate_metric': is_gate,
                      'observed_pass': passed}
            details.append(record)
            if is_gate and not passed:
                regressions.append(record)
    status = ('observed_fail' if regressions else
              'incomplete' if missing or incomparable_gate_metric
              or not any(item['gate_metric'] for item in details) else 'observed_pass')
    return {'status': status, 'strict_no_tolerance': True, 'compared_cells': len(details),
            'regressions': regressions, 'missing_comparisons': missing,
            'not_comparable_schema_metrics': not_comparable, 'details': details}


def allocation_nonregression(reference: dict, candidate: dict) -> dict[str, Any]:
    ref = reference['summary'].get('allocation_median')
    value = candidate['summary'].get('allocation_median')
    if ref is None or value is None:
        return {'status': 'incomplete', 'reference_median': ref, 'candidate_median': value}
    return {'status': 'observed_pass' if value <= ref else 'observed_fail',
            'strict_no_tolerance': True, 'reference_median': ref,
            'candidate_median': value, 'delta': value - ref}


def criterion_values(rec: dict) -> dict[str, float]:
    result = {}
    for command in rec.get('manifest', {}).get('commands', []):
        for benchmark_id, estimate in command.get('criterion_estimates', {}).items():
            result[benchmark_id] = float(estimate['median_point_estimate'])
    return result


def historical_comparison(rec: dict) -> dict[str, Any]:
    measured = criterion_values(rec)
    criteria = {}
    for benchmark_id, reference in HISTORICAL_218.items():
        value = measured.get(benchmark_id)
        earlier = EARLIER_218[benchmark_id]
        criteria[benchmark_id] = {
            'earlier_local_reference_ns': earlier,
            '218_local_reference_ns': reference,
            'measured_median_ns': value,
            'delta_vs_218_ns': value - reference if value is not None else None,
            'delta_vs_218_percent': (100.0 * (value - reference) / reference) if value is not None else None,
        }
    allocation = rec['summary'].get('allocation_median')
    return {
        'provenance': 'DefraLevel plan §5.3 historical #218 local figures; descriptive only, hardware/profile comparability unknown',
        'criterion': criteria,
        'allocation': {
            '218_main_create_allocations': HISTORICAL_MAIN_CREATE_ALLOCATIONS,
            'measured_create_allocations_median': allocation,
            'delta_allocations': allocation - HISTORICAL_MAIN_CREATE_ALLOCATIONS if allocation is not None else None,
        },
        'affects_gate_status': False,
    }


def criterion_nonregression(reference: dict, candidate: dict) -> dict[str, Any]:
    ref, cur = criterion_values(reference), criterion_values(candidate)
    details, missing = [], []
    for benchmark_id, value in cur.items():
        if benchmark_id not in ref:
            missing.append(f'{benchmark_id}: reference estimate missing')
            continue
        baseline = ref[benchmark_id]
        details.append({'criterion_id': benchmark_id, 'reference_nanoseconds': baseline,
                        'candidate_nanoseconds': value, 'delta_nanoseconds': value - baseline,
                        'observed_pass': value <= baseline})
    regressions = [record for record in details if not record['observed_pass']]
    status = 'observed_fail' if regressions else 'incomplete' if missing or not details else 'observed_pass'
    return {'status': status, 'strict_no_tolerance': True, 'details': details,
            'regressions': regressions, 'missing_comparisons': missing}


def validate(rec: dict) -> dict[str, Any]:
    errors: list[str] = []
    run = rec['run']
    if not isinstance(run, dict):
        errors.append('baseline_run is not an object')
        return {'valid': False, 'errors': errors, 'run': {}, 'summary': {}}
    if not re.fullmatch(r'[0-9a-fA-F]{7,64}', str(run.get('revision', ''))):
        errors.append('baseline_run has invalid/missing git revision')
    if run.get('load_guard', {}).get('passed') is not True:
        errors.append('baseline_run load guard did not pass')
    embedded_guard = run.get('load_guard')
    if isinstance(embedded_guard, dict) and 'detail' in embedded_guard:
        detail = embedded_guard.get('detail')
        if not isinstance(detail, dict):
            errors.append('baseline_run load guard detail is missing or malformed')
            embedded_guard_detail = None
        else:
            if (embedded_guard.get('passed') is not detail.get('passed')
                    or embedded_guard.get('note') != detail.get('note')):
                errors.append('baseline_run load guard wrapper flags differ from detail')
            embedded_guard_detail = detail
    else:
        # Older frozen benchmark binaries emitted the raw load-guard verdict
        # directly. Keep accepting that exact form while validating the
        # current wrapper's flags and detail against the command artifact.
        embedded_guard_detail = embedded_guard
    normalized = validate_samples(rec, errors)
    manifest = validate_manifest(rec, errors)
    write_command = manifest['commands'].get('write_baseline', {})
    if write_command.get('guard') != embedded_guard_detail:
        errors.append('baseline_run load guard differs from write_baseline command guard')
    rec['manifest_validated'] = manifest
    rec['normalized'] = normalized
    rec['summary'] = summarize(rec, normalized)
    return {'valid': not errors, 'errors': errors,
            'source_revision': run.get('revision'),
            'profile': run.get('profile'),
            'schema': normalized['schema'],
            'expected_samples': normalized['expected_samples'],
            'observed_samples': normalized['observed_samples'],
            'complete_create_coverage': rec['summary']['create_sample_coverage_complete'],
            'binary_hashes': manifest['binary_hashes'],
            'build_profile': manifest['build_profile']}


def markdown(report: dict) -> str:
    lines = ['# Frozen DefraLevel gate results', '',
             'Structural validity and observed gates are separate. Strict comparisons use the measured medians/point estimates with no tolerance; they do not account for timing noise.', '']
    for rec in report['runs']:
        validation = rec['validation']
        summary = rec['summary']
        lines += [f"## {rec['label']}", '',
                  f"Revision `{validation.get('source_revision')}`; schema `{validation.get('schema')}`; structurally valid: **{validation['valid']}**; create coverage complete: **{validation['complete_create_coverage']}**.", '']
        if validation['errors']:
            lines.append('Structural findings: ' + '; '.join(validation['errors']))
            lines.append('')
        lines += ['| Workload | Writers | Commits/s median | Storage conflicts/commit | Client conflicts/commit | Legacy HTTP conflicts/commit | Engine exhausted requests | p50 ms | p99 ms |',
                  '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
        for key, cell in summary['groups'].items():
            workload, writer = key.split('|writers=')
            med = cell['medians']
            f = lambda value: '—' if value is None else f'{value:.5g}'
            lines.append(f"| {workload} | {writer} | {f(med.get('commits_per_second'))} | {f(med.get('storage_conflicts_per_commit'))} | {f(med.get('client_conflicts_per_commit'))} | {f(med.get('legacy_http_conflicts_per_commit'))} | {f(med.get('engine_exhaustions'))} | {f(med.get('latency_p50_ms'))} | {f(med.get('latency_p99_ms'))} |")
        lines += ['', 'Per-layer retry medians:', '']
        for key, cell in summary['groups'].items():
            retries = cell['medians'].get('retry_metrics_per_commit', {})
            lines.append(f"- {key}: {json.dumps(retries, sort_keys=True)}")
        lines += ['', f"Create-conflict gate: **{summary['create_conflicts_gate_status']}**; current-schema zero proof `{summary['current_create_conflicts_zero_proven']}`; legacy indicators zero `{summary['legacy_create_conflict_indicators_zero']}`. Allocation samples `{summary['allocation_samples']}`, median `{summary['allocation_median']}`.", '',
                  '| Workload | Strict median throughput scaling | Rates by writers |', '|---|---|---|']
        for workload, gate in summary['scaling'].items():
            lines.append(f"| {workload} | {gate['gate_status']} | {json.dumps(gate['throughput_by_writer_count'], sort_keys=True)} |")
        lines.append('')
    lines += ['## Historical #218 comparison (descriptive)', '',
              'These are local historical measurements from DefraLevel plan §5.3. They are reported as context only: hardware and profile comparability are unknown, so they do not determine gate status.', '',
              '| Run | Criterion ID | Earlier local | #218 reference | Measured median | Delta vs #218 |',
              '|---|---|---:|---:|---:|---:|']
    for rec in report['runs']:
        for benchmark_id, item in rec['historical_comparison']['criterion'].items():
            measured = item['measured_median_ns']
            delta = item['delta_vs_218_ns']
            f_us = lambda value: '—' if value is None else f'{value / 1000:.3f} µs'
            f_ms = lambda value: '—' if value is None else f'{value / 1_000_000:.3f} ms'
            scale = f_ms if benchmark_id.startswith('startup/') else f_us
            lines.append(f"| {rec['label']} | {benchmark_id} | {scale(item['earlier_local_reference_ns'])} | {scale(item['218_local_reference_ns'])} | {scale(measured)} | {scale(delta)} |")
        allocation = rec['historical_comparison']['allocation']
        lines.append(f"| {rec['label']} | document create allocations | — | {allocation['218_main_create_allocations']:.0f} alloc/op | {allocation['measured_create_allocations_median'] if allocation['measured_create_allocations_median'] is not None else '—'} alloc/op | {allocation['delta_allocations'] if allocation['delta_allocations'] is not None else '—'} alloc/op |")
    lines.append('')
    lines += ['## Main and immediate-predecessor comparisons', '']
    for pair in report['comparisons']:
        base = pair['baseline_nonregression']
        pred = pair['predecessor_nonregression']
        lines += [f"### {pair['reference']} → {pair['candidate']}", '',
                  f"Strict no-tolerance baseline comparison: **{base['status']}** ({base.get('compared_cells', 0)} metric cells, {len(base.get('regressions', []))} regressions, {len(base.get('not_comparable_schema_metrics', []))} schema-distinct metrics); immediate-predecessor comparison: **{pred['status']}** ({pred.get('compared_cells', 0)} metric cells, {len(pred.get('regressions', []))} regressions, {len(pred.get('not_comparable_schema_metrics', []))} schema-distinct metrics). Allocation vs main/predecessor: **{pair['allocation_vs_main']['status']} / {pair['allocation_vs_predecessor']['status']}**; Criterion vs main/predecessor: **{pair['criterion_vs_main']['status']} / {pair['criterion_vs_predecessor']['status']}**.", '']
        for ref_key, label in (('baseline_nonregression', 'main'), ('predecessor_nonregression', 'predecessor')):
            gate = pair[ref_key]
            if gate.get('regressions'):
                lines.append(f"Observed regressions vs {label}: `{json.dumps(gate['regressions'], sort_keys=True)}`")
                lines.append('')
    lines += ['## Scope limits', '',
              'The shared-updates workload includes same-field contention among writers in each parity group, so its aggregate conflict count cannot prove the separate disjoint-field target. The focused disjoint-update regression test must provide that evidence. Legacy HTTP retry metrics are not relabeled as current client conflicts.', '']
    return '\n'.join(lines)


def main() -> int:
    global GATE_ROOT, BINARY_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--main', required=True, help='main baseline label; must be the first supplied label')
    parser.add_argument('--gate-root', type=Path, default=GATE_ROOT)
    parser.add_argument('--binary-root', type=Path, default=BINARY_ROOT)
    parser.add_argument('--json', dest='json_out')
    parser.add_argument('--markdown', dest='md_out')
    parser.add_argument('labels', nargs='+')
    args = parser.parse_args()
    GATE_ROOT = args.gate_root.resolve()
    BINARY_ROOT = args.binary_root.resolve()
    try:
        if len(args.labels) < 2:
            raise ValueError('supply main plus at least one candidate revision for predecessor comparisons')
        if args.labels[0] != args.main:
            raise ValueError('--main must be the first label; remaining labels define immediate-predecessor order')
        recs = [read_run(label) for label in args.labels]
        for rec in recs:
            rec['validation'] = validate(rec)
            rec['historical_comparison'] = historical_comparison(rec)
        comparisons = []
        for index, candidate in enumerate(recs[1:], 1):
            main_rec = recs[0]
            predecessor = recs[index - 1]
            structurally_valid = main_rec['validation']['valid'] and candidate['validation']['valid']
            predecessor_valid = predecessor['validation']['valid'] and candidate['validation']['valid']
            main_comparison = nonregression(main_rec, candidate) if structurally_valid else {
                'status': 'not_assessed_invalid_structure', 'strict_no_tolerance': True,
                'regressions': [], 'missing_comparisons': ['one or both runs failed structural validation'],
            }
            predecessor_comparison = nonregression(predecessor, candidate) if predecessor_valid else {
                'status': 'not_assessed_invalid_structure', 'strict_no_tolerance': True,
                'regressions': [], 'missing_comparisons': ['one or both runs failed structural validation'],
            }
            comparisons.append({
                'reference': args.main,
                'predecessor': predecessor['label'],
                'candidate': candidate['label'],
                'baseline_nonregression': main_comparison,
                'predecessor_nonregression': predecessor_comparison,
                'allocation_vs_main': allocation_nonregression(main_rec, candidate) if structurally_valid else {'status': 'not_assessed_invalid_structure'},
                'allocation_vs_predecessor': allocation_nonregression(predecessor, candidate) if predecessor_valid else {'status': 'not_assessed_invalid_structure'},
                'criterion_vs_main': criterion_nonregression(main_rec, candidate) if structurally_valid else {'status': 'not_assessed_invalid_structure'},
                'criterion_vs_predecessor': criterion_nonregression(predecessor, candidate) if predecessor_valid else {'status': 'not_assessed_invalid_structure'},
            })
        report = {'format_version': 2, 'main_label': args.main,
                  'runs': [{**rec, 'validation': rec['validation'], 'summary': rec['summary'],
                            'historical_comparison': rec['historical_comparison'],
                            'manifest': rec['manifest'], 'families': {
                                name: {'path': str(value['path']), 'sha256': value['sha256'],
                                       'records': value['records']}
                                for name, value in rec['families'].items()},
                            'normalized': None}
                           for rec in recs],
                  'comparisons': comparisons}
        output = markdown(report)
        if args.json_out:
            Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        if args.md_out:
            Path(args.md_out).write_text(output)
        if not args.json_out and not args.md_out:
            print(output)
        # Structural failures and observed-gate failures have separate exit codes.
        if not all(rec['validation']['valid'] for rec in recs):
            return 2
        statuses = []
        for rec in recs[1:]:
            statuses.append(rec['summary']['create_conflicts_gate_status'])
            statuses.extend((gate.get('gate_status', 'incomplete') for gate in rec['summary']['scaling'].values()))
        for pair in comparisons:
            statuses.extend(pair.get(key, {}).get('status', 'incomplete') for key in (
                'baseline_nonregression', 'predecessor_nonregression', 'allocation_vs_main',
                'allocation_vs_predecessor', 'criterion_vs_main', 'criterion_vs_predecessor'))
        return 0 if all(status == 'observed_pass' for status in statuses) else 3
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f'error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
