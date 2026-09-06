import json,statistics,pathlib,sys
root=pathlib.Path(sys.argv[1]) if len(sys.argv)>1 else pathlib.Path('/private/tmp/vibedb-pr194-perf')
reports={}
for path in sorted(root.glob('*/report.json')):
 d=json.load(open(path)); reports[path.parent.name]=d['results']
keys=sorted({(r['workload'],r['clients']) for rows in reports.values() for r in rows})
out={'arms':{},'comparison':[]}
for arm,rows in reports.items():
 out['arms'][arm]={'trials':len(rows),'operations':sum(r['operations'] for r in rows),'errors':sum(r['errors'] for r in rows),'all_verified':all(r['verified'] for r in rows),'cells':[]}
 for w,c in keys:
  rs=[r for r in rows if (r['workload'],r['clients'])==(w,c)]
  if rs:out['arms'][arm]['cells'].append({'workload':w,'clients':c,'ops_s':statistics.median(r['successful_ops_per_second'] for r in rs),'p99_ms':statistics.median(r['p99_ns']/1e6 for r in rs)})
if len(reports)==4:
 for w,c in keys:
  arms={k:[r for r in rows if (r['workload'],r['clients'])==(w,c)] for k,rows in reports.items()}
  before=sum([v for k,v in arms.items() if 'baseline' in k],[]);after=sum([v for k,v in arms.items() if 'scheduler' in k],[])
  a=statistics.median(r['successful_ops_per_second'] for r in before);b=statistics.median(r['successful_ops_per_second'] for r in after)
  out['comparison'].append({'workload':w,'clients':c,'baseline_ops_s':a,'candidate_ops_s':b,'change_percent':100*(b/a-1),'baseline_p99_ms':statistics.median(r['p99_ns']/1e6 for r in before),'candidate_p99_ms':statistics.median(r['p99_ns']/1e6 for r in after)})
(root/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2))
