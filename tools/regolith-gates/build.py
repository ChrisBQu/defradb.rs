#!/usr/bin/env python3
"""Freeze benchmark executables from clean revisions in a detached worktree."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('labels', nargs='+', help='LABEL-REVISION, for example main-532aac3f')
    parser.add_argument('--worktree', type=Path, required=True)
    parser.add_argument('--target-dir', type=Path, required=True)
    parser.add_argument('--binary-root', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=4)
    args = parser.parse_args()
    work = args.worktree.resolve()
    env = dict(os.environ, CARGO_TARGET_DIR=str(args.target_dir.resolve()))
    if subprocess.run(['git', 'symbolic-ref', '-q', 'HEAD'], cwd=work,
                      stdout=subprocess.DEVNULL).returncode == 0:
        parser.error('--worktree must be a dedicated detached worktree')
    targets = {'allocs', 'write_baseline', 'document_write', 'startup'}
    for label in args.labels:
        if subprocess.check_output(['git', 'status', '--porcelain'], cwd=work).strip():
            raise RuntimeError('Refusing to change a dirty worktree')
        revision = label.rsplit('-', 1)[-1]
        subprocess.run(['git', 'checkout', '--detach', revision], cwd=work, check=True)
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=work,
                                           text=True).strip()
        destination = args.binary_root.resolve() / label
        destination.mkdir(parents=True, exist_ok=False)
        command = ['cargo', 'bench', '-p', 'benches', '--profile', 'release-fast',
                   '--no-run', '--message-format=json', '-j', str(args.jobs)]
        for target in sorted(targets):
            command += ['--bench', target]
        manifest = destination / 'cargo-artifacts.jsonl'
        with manifest.open('w') as output:
            subprocess.run(command, cwd=work, env=env, stdout=output, check=True)
        hashes = {}
        for line in manifest.read_text().splitlines():
            artifact = json.loads(line)
            if artifact.get('reason') != 'compiler-artifact' or not artifact.get('executable'):
                continue
            name = artifact['target']['name']
            if name in targets:
                output = destination / name
                shutil.copy2(artifact['executable'], output)
                hashes[name] = hashlib.sha256(output.read_bytes()).hexdigest()
        if hashes.keys() != targets:
            raise RuntimeError(f'Missing executables: {targets - hashes.keys()}')
        metadata = dict(revision=revision, dirty=False, source_path=str(work),
                        command=command, target_dir=env['CARGO_TARGET_DIR'],
                        sha256=hashes, executed=False)
        (destination / 'BUILD.json').write_text(json.dumps(metadata, indent=2) + '\n')
        print('STAGED', label, flush=True)


if __name__ == '__main__':
    main()
