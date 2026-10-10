#!/usr/bin/env python3
"""Sample only a specified benchmark Compose project's container resources."""
import argparse,datetime,json,pathlib,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--project',required=True);p.add_argument('--output',required=True);a=p.parse_args()
if not a.project.startswith('msgmate-perf-'):p.error('benchmark project required')
seen=False;empty=0
with pathlib.Path(a.output).open('w') as out:
 for _ in range(1200):
  ids=subprocess.run(['docker','ps','-q','--filter','label=com.docker.compose.project='+a.project],capture_output=True,text=True,timeout=15).stdout.split()
  if not ids:
   empty+=1
   if seen and empty>=3:break
   time.sleep(2);continue
  seen=True;empty=0
  sample=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}',*ids],capture_output=True,text=True,timeout=20)
  for line in sample.stdout.splitlines():
   try:data=json.loads(line)
   except ValueError:continue
   data['sampled_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();out.write(json.dumps(data)+'\n')
  out.flush();time.sleep(2)
