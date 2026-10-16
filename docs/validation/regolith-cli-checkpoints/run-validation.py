import hashlib
import json
import os
import pathlib
import subprocess
import sys

work = pathlib.Path('/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb-regolith-step-validation')
target = '/tmp/defra-kms-ci-target'
go = '/Users/johnzampolin/.cache/defra-harness/53f0e76a3'
go_revision = '53f0e76a32d1761e3396a88ce607ebc3e37baa5a'
manifest_path = pathlib.Path('/tmp/defra-main-refreshed-heads.json')
report_path = pathlib.Path('/tmp/defra-main-cli-validation.json')
assert subprocess.run(['git', 'symbolic-ref', '-q', 'HEAD'], cwd=work, stdout=subprocess.DEVNULL).returncode != 0
assert pathlib.Path(go, 'defradb').is_file()
assert not report_path.exists(), f'refusing overwrite of {report_path}'
manifest = json.loads(manifest_path.read_text())
entries = manifest[:10]
env = dict(os.environ, CARGO_TARGET_DIR=target, CARGO_BUILD_JOBS='4')
cli = target + '/super-dev/defra'
report = []

for item in entries:
    rev = item['head']
    assert subprocess.run(['git', 'cat-file', '-e', rev + '^{commit}'], cwd=work).returncode == 0
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=work).strip()
    subprocess.run(['git', 'checkout', '--detach', rev], cwd=work, check=True)
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=work, text=True).strip()
    assert sha == rev
    prefix = pathlib.Path('/tmp') / ('defra-main-cli-' + sha[:9])
    for suffix in ('build', 'external', 'counter'):
        assert not pathlib.Path(str(prefix) + '-' + suffix + '.log').exists(), f'refusing overwrite: {prefix}-{suffix}.log'

    def run(name, args, environment):
        print('RUN', sha, name, flush=True)
        with pathlib.Path(str(prefix) + '-' + name + '.log').open('w') as log:
            log.write('Revision: ' + sha + '\n')
            log.flush()
            result = subprocess.run(args, cwd=work, env=environment, stdout=log, stderr=subprocess.STDOUT)
        print('RESULT', sha, name, result.returncode, flush=True)
        if result.returncode:
            sys.exit(result.returncode)

    run('build', ['cargo', 'build', '-p', 'cli', '--bin', 'defra', '--profile', 'super-dev'], env)
    binary_sha = hashlib.sha256(pathlib.Path(cli).read_bytes()).hexdigest()
    ext = dict(env, PATH=go + ':' + os.environ['PATH'], DEFRA_RUST_BINARY=cli,
               DEFRA_GO_BIN=go + '/defradb', DEFRA_GO_BINARY=go + '/defradb',
               DEFRA_ROOTDIR=str(prefix) + '-db', DEFRA_CONFORMANCE_BINARY=cli)
    run('external', ['cargo', 'test', '-p', 'integration-test', '--profile', 'super-dev',
                     '--test', 'basic', '--test', 'acp', '--test', 'encryption', '--',
                     '--test-threads=4'], ext)
    run('counter', ['cargo', 'test', '-p', 'conformance', '--profile', 'super-dev',
                    'counter', '--', '--test-threads=4'], ext)
    report.append(dict(pr=item['pr'], revision=sha, expected= item['expected'],
                       cli_sha256=binary_sha, logs=str(prefix), go_revision=go_revision))
    report_path.write_text(json.dumps(report, indent=2) + '\n')

print('COMPLETE', flush=True)
