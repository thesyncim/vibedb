import subprocess,json,datetime,pathlib,time
p=pathlib.Path(__file__).parent
image="golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b"
trials=[]
for name,arm in [("before-1","before"),("after-1","after"),("after-2","after"),("before-2","before")]:
 pattern="^BenchmarkReplicatedReadExecutor$/(point_hit|point_miss|range_32|range_64|range_256)/fresh$" if arm=="before" else "^BenchmarkReplicatedReadExecutor(PreparedReuse)?$/(point_hit|point_miss|range_32|range_64|range_256)/(fresh|prepared_reuse)$"
 cmd=["docker","run","--rm","--pull=never","--platform","linux/arm64","--mount",f"type=bind,src={p},dst=/evidence,readonly","--mount","type=volume,src=vibedb-read-executor-baseline-cache,dst=/fixture-tmp","--workdir","/fixture-tmp","-e","TMPDIR=/fixture-tmp","-e","GOMAXPROCS=4",image,f"/evidence/driver-{arm}.test","-test.run","^$","-test.bench",pattern,"-test.benchtime=1000x","-test.benchmem","-test.count=3","-test.timeout=10m"]
 row={"name":name,"arm":arm,"command":cmd,"started_utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}
 with (p/(name+".log")).open("w") as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
 row.update(exit_code=r.returncode,ended_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());trials.append(row)
 (p/"trials.json").write_text(json.dumps(trials,indent=2)+"\n")
 print(name,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
