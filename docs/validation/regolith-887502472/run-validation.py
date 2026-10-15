import subprocess, os, pathlib, json, sys
work=pathlib.Path('/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb-regolith-proofs-final')
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=work,text=True).strip()
assert not subprocess.check_output(['git','status','--porcelain'],cwd=work,text=True).strip()
prefix=pathlib.Path('/tmp')/('defra-final-surface-'+revision[:9])
env=dict(os.environ,CARGO_TARGET_DIR='/tmp/defra-parts-target',CARGO_BUILD_JOBS='4')
def run(name,cmd,environment):
 print('RUN',revision,name,flush=True)
 with pathlib.Path(str(prefix)+'-'+name+'.log').open('w') as log:
  log.write('Revision: '+revision+'\n');log.flush()
  r=subprocess.run(cmd,cwd=work,env=environment,stdout=log,stderr=subprocess.STDOUT)
 print('RESULT',name,r.returncode,flush=True)
 if r.returncode: sys.exit(r.returncode)
run('core',['cargo','test','-p','db','-p','query','-p','acp','-p','zanzibar','-p','datastore','-p','document'],env)
run('frontdoors',['cargo','test','-p','defra-node','-p','defra-http','--tests'],env)
run('bench-integration',['cargo','test','-p','benches','--test','write_baseline'],env)
meta=json.loads(subprocess.check_output(['cargo','metadata','--no-deps','--format-version','1'],cwd=work,env=env,text=True))
names={'storage','document','acp','datastore','zanzibar','db','query','p2p','defra-http','defra-p2p-adapter','defra-node','benches','cli'}
cmd=['cargo','clippy']
for p in meta['packages']:
 if p['name'] in names: cmd+=['-p',p['id']]
run('native-clippy',cmd+['--all-targets','--','-D','warnings'],env)
cli_env=dict(os.environ,CARGO_TARGET_DIR='/tmp/defra-step5-cli-target',CARGO_BUILD_JOBS='4')
run('cli-build',['cargo','build','-p','cli','--bin','defra','--profile','super-dev'],cli_env)
cli='/tmp/defra-step5-cli-target/super-dev/defra'
go='/Users/johnzampolin/.cache/defra-harness/53f0e76a3'
ext=dict(cli_env,PATH=go+':'+os.environ['PATH'],DEFRA_RUST_BINARY=cli,DEFRA_GO_BIN=go+'/defradb',DEFRA_GO_BINARY=go+'/defradb',DEFRA_ROOTDIR=str(prefix)+'-db',DEFRA_CONFORMANCE_BINARY=cli)
run('external',['cargo','test','-p','integration-test','--profile','super-dev','--test','basic','--test','acp','--test','encryption','--','--test-threads=4'],ext)
run('counter',['cargo','test','-p','conformance','--profile','super-dev','counter','--','--test-threads=4'],ext)
run('write-skew',['cargo','test','-p','conformance','--profile','super-dev','ssi_write_skew_prevented','--','--test-threads=1'],ext)
print('COMPLETE',revision,flush=True)
