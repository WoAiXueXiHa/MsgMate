import subprocess,time,json,pathlib
root=pathlib.Path('/tmp/msgmate-bench')
url='http://127.0.0.1:18109/msg/get_template?templateID=0ddd02e0-f1a8-4762-8980-6d27cda6dff8'
for c in [10,50,100,200,400,800]:
 p=subprocess.Popen(['./wrk/wrk','-t4',f'-c{c}','-d10s','--latency','--timeout','2s','-s',str(root/'check.lua'),url],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
 out=p.communicate()[0]; (root/f'c{c}.txt').write_text(out)
 print(f'CONCURRENCY {c}\n{out}',flush=True)
