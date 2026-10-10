#!/usr/bin/env python3
"""Isolated adaptive benchmarks. Real storage/consumers; fake external channel only."""
import argparse, datetime, hashlib, json, os, pathlib, platform, re, shutil, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[2]
TABLES = ['t_msg_queue_high','t_msg_queue_middle','t_msg_queue_low','t_msg_queue_retry','t_msg_record']

class Bench:
    def __init__(self, args):
        self.args = args
        stamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y%m%d-%H%M%S')
        self.project = 'msgmate-perf-' + stamp
        self.out = ROOT / 'reports' / 'performance' / stamp
        self.out.mkdir(parents=True)
        self.work = pathlib.Path('/tmp') / self.project
        self.work.mkdir()
        self.env = dict(os.environ, PERF_WORK=str(self.work))
        self.base_cmd = ['docker','compose','-p',self.project,'-f',str(ROOT/'scripts/perf/compose.yml')]
        self.stopped = []
        self.results = []
        self.mode = 'mysql'
        self.serial = 0
        self.environment = {}
        self.monitor = None

    def command(self, cmd, timeout=120, check=True):
        p = subprocess.run(cmd, cwd=ROOT, env=self.env, text=True, capture_output=True, timeout=timeout)
        if check and p.returncode:
            # Config contains only synthetic local credentials; still do not print full config.
            raise RuntimeError('Command failed: ' + ' '.join(cmd[:8]) + '\n' + p.stderr[-3000:])
        return p.stdout

    def compose(self, *args, timeout=120):
        return self.command(self.base_cmd + list(args), timeout)

    def capture(self):
        for key, cmd in [('uname',['uname','-a']),('cpu',['lscpu']),('memory',['free','-h']),('go',['go','version']),('git',['git','rev-parse','HEAD']),('git_status',['git','status','--short']),('containers_before',['docker','ps','--format','{{.Names}} {{.Image}}']),('disk',['df','-h',str(ROOT)])]:
            self.environment[key] = self.command(cmd).strip()
        wsl = pathlib.Path('/mnt/c/Users/hp/.wslconfig')
        if wsl.exists():
            # Only resource/network settings, not arbitrary user config or comments.
            self.environment['wslconfig'] = [s.strip() for s in wsl.read_text(encoding='utf-8-sig').splitlines() if re.match(r'(?i)^\s*(memory|processors|swap|networkingMode|dnsTunneling|autoProxy)\s*=',s)]
        self.environment['source_hashes'] = {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for base in ['src','cmd/perf','scripts/perf'] for p in (ROOT/base).rglob('*') if p.is_file() and p.suffix in ['.go','.py','.lua','.yml','.sh']}
        self.environment.update({'time_zone':'Asia/Shanghai','same_machine':True,'channel':'in-process local fake, zero delay, no external calls','mysql_pool_max':50,'mysql_pool_idle':10,'record_dataset':10000,'kafka_partitions_per_topic':1,'kafka_replicas':1,'kafka_ack':-1,'kafka_async':False,'log':'info file logging and Gin access log to file; no interactive terminal','thresholds':{'business_success':0.99,'transport_errors':0,'query_p99_ms':200,'send_p99_ms':2000,'queue_p99_ms':2000,'queue_backlog_growth_allowed_rps_fraction':0.05,'queue_tail_completion_min_offered_fraction':0.95,'queue_drain_timeout_seconds':60,'queue_tail_pending_max_seconds_of_offered_load':0.25},'duration_seconds':self.args.duration,'verify_seconds':self.args.verify_duration})
        self.save_environment()

    def save_environment(self):
        (self.out/'environment.json').write_text(json.dumps(self.environment,ensure_ascii=False,indent=2)+'\n')

    def pause(self):
        if not self.args.pause_unrelated: return
        names = self.command(['docker','ps','--format','{{.Names}}']).splitlines()
        self.stopped = [n for n in names if n.startswith(('devsupport-','learnq_demo_20261003-')) or n in ['ollama','kafka-ui']]
        self.environment['paused_containers'] = self.stopped
        self.save_environment()
        if self.stopped:
            print('Pausing unrelated containers:', ', '.join(self.stopped), flush=True)
            self.command(['docker','stop',*self.stopped],timeout=120)
        self.environment['memory_after_pause'] = self.command(['free','-h']).strip()
        self.save_environment()

    def build(self):
        print('Building Go benchmark runtime and statically linked wrk...',flush=True)
        self.command(['go','build','-o',str(self.work/'msgmate-perf'),'./cmd/perf'],timeout=180)
        # Build uses CGO_ENABLED=0 in the environment set by main.
        wrk_source=ROOT/'wrk'
        if not (wrk_source/'Makefile').exists():
            wrk_source=self.work/'wrk-source'
            self.command(['git','clone','https://github.com/wg/wrk.git',str(wrk_source)],timeout=180)
            self.command(['git','-C',str(wrk_source),'checkout','a211dd5a7050b1f9e8a9870b95513060e72ac4a0'])
        self.command(['make','-C',str(wrk_source),'-j2'],timeout=300)
        objects = sorted(str(p) for p in (wrk_source/'obj').glob('*.o'))
        self.command(['cc','-static','-Wl,-E','-o',str(self.work/'wrk'),*objects,'-L'+str(wrk_source/'obj/lib'),'-lluajit-5.1','-lm','-lssl','-lcrypto','-lpthread','-ldl'],timeout=120)
        (self.work/'lib').mkdir()
        shutil.copy(wrk_source/'src/wrk.lua',self.work/'lib/wrk.lua')
        shutil.copy(ROOT/'scripts/perf/wrk.lua',self.work/'wrk.lua')
        shutil.copy(ROOT/'scripts/perf/wrk.lua',self.out/'wrk.lua')
        self.environment['wrk_revision'] = self.command(['git','-C',str(wrk_source),'rev-parse','HEAD'],check=False).strip()
        self.environment['binary_sha256'] = {n:hashlib.sha256((self.work/n).read_bytes()).hexdigest() for n in ['msgmate-perf','wrk']}
        self.save_environment()

    def start(self):
        self.compose('up','-d','mysql','redis','load',timeout=180)
        if self.args.suite in ['all','send','kafka']:
            self.compose('up','-d','zookeeper','kafka',timeout=180)
            for i in range(60):
                cid=self.compose('ps','-q','kafka').strip()
                p=subprocess.run(['docker','exec',cid,'kafka-topics','--bootstrap-server','kafka:9092','--list'],capture_output=True,text=True,timeout=15)
                if p.returncode==0:break
                if i%5==0:print('Waiting for isolated Kafka broker...',flush=True)
                time.sleep(2)
            else: raise RuntimeError('Kafka broker not ready')
        self.load_id=self.compose('ps','-q','load').strip()
        self.monitor=subprocess.Popen([sys.executable,str(ROOT/'scripts/perf/monitor.py'),'--project',self.project,'--output',str(self.out/'resource-samples.jsonl')],cwd=ROOT,env=self.env)
        self.environment['isolated_project']=self.project
        self.environment['images']=self.compose('images','--format','json').strip()
        self.save_environment()

    def configure(self,mode,consume):
        self.mode=mode
        cfg='''[Common]
port=18081
mysql_as_mq=%s
open_cache=true
max_retry_count=3
[MySQL]
url="mysql:3306"
user="root"
pwd="benchmark-local-only"
db_name="msgmate_perf_test"
[Redis]
url="redis:6379"
pwd="benchmark-local-only"
max_idle=16
max_active=256
idle_timeout=300
[Kafka]
brokers=["kafka:9092"]
''' % ('true' if mode=='mysql' else 'false')
        for priority,name in enumerate(['low','middle','high','retry'],1):
            cfg += f'[Kafka.topics.{name}]\nname="perf_{mode}_{"queue" if consume else "ingress"}_{name}"\npriority={priority}\nack=-1\nasync=false\ngroup_id="perf_{mode}_{"queue" if consume else "ingress"}_{name}"\npartition=0\n'
        (self.work/'config.toml').write_text(cfg)
        override=self.work/'runtime.yml'
        override.write_text('services:\n  runtime:\n    command: '+json.dumps(['/perf/msgmate-perf','-config-file','/perf/config.toml','-consume='+str(consume).lower()])+'\n')
        self.command(self.base_cmd+['-f',str(override),'up','-d','--force-recreate','runtime'],timeout=180)
        self.runtime_id=self.compose('ps','-q','runtime').strip()
        for i in range(60):
            try:
                s=self.stats()
                if s['mode_mysql'] == (mode=='mysql') and s['consume']==consume and not s.get('stats_error'):break
            except (RuntimeError,ValueError,KeyError):pass
            if i%10==0: print(f'Waiting for {mode} runtime, consume={consume}...',flush=True)
            time.sleep(1)
        else:
            raise RuntimeError('runtime not ready\n'+self.compose('logs','--tail','20','runtime'))
        self.command(['docker','exec',self.load_id,'/perf/wrk','--version'],check=False)

    def stats(self):
        return json.loads(self.command(['docker','exec',self.load_id,'/perf/msgmate-perf','-action','stats'],timeout=20))

    def reset(self, stop=True):
        if stop: self.compose('stop','runtime',timeout=60)
        sql=''.join(f'TRUNCATE TABLE {t};' for t in TABLES[:4])+"DELETE FROM t_msg_record WHERE source_id='perf-send';"
        self.compose('exec','-T','mysql','sh','-c','MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot msgmate_perf_test -e "$1"','sh',sql)
        if stop: self.compose('exec','-T','redis','sh','-c','REDISCLI_AUTH=benchmark-local-only redis-cli FLUSHDB')

    def resources(self):
        return self.command(['docker','stats','--no-stream','--format','{{json .}}',*self.compose('ps','-q').split()],timeout=20)

    def store(self,name,result,raw=None,samples=None):
        self.serial+=1
        stem=f'{self.serial:03d}-{name}'
        result['artifact']=stem
        self.results.append(result)
        (self.out/(stem+'.json')).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        if raw is not None:(self.out/(stem+'.txt')).write_text(raw)
        if samples is not None:(self.out/(stem+'-samples.json')).write_text(json.dumps(samples,ensure_ascii=False,indent=2)+'\n')
        self.report()

    def wrk_stage(self,kind,c,duration,verify=False):
        before=self.stats()
        cmd=['docker','exec',self.load_id,'/perf/wrk','-t4',f'-c{c}',f'-d{duration}s','--timeout','2s','--latency','-s','/perf/wrk.lua','http://runtime:18081','--',kind]
        p=subprocess.Popen(cmd,cwd=ROOT,env=self.env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        samples=[];started=time.monotonic()
        while p.poll() is None:
            time.sleep(2)
            samples.append({'elapsed':time.monotonic()-started,'stats':self.stats()})
            if time.monotonic()-started>duration+30:p.kill();raise RuntimeError('wrk hung')
        raw=p.communicate()[0]
        if p.returncode:raise RuntimeError('wrk failed\n'+raw)
        match=re.search(r'BENCH_JSON (\{[^\n]+\})',raw)
        if not match: raise RuntimeError('missing business metrics\n'+raw)
        m=json.loads(match.group(1));m['rps']=m['ok']/(m['duration_us']/1e6)
        m.update({'suite':kind,'mode':self.mode,'concurrency':c,'duration':duration,'verify':verify,'before':before,'after':self.stats(),'command':cmd,'resources':self.resources()})
        errors=sum(m[k] for k in ['connect','read','write','timeout','http_errors'])
        m['passed']=m['ok']>=.99*max(1,m['requests']) and m['bad']==0 and errors==0 and not any(s['stats'].get('stats_error') for s in samples) and m['p99_ms']<= (200 if kind=='record' else 2000)
        print(f'{kind}/{self.mode} c={c} ok_rps={m["rps"]:.1f} p99={m["p99_ms"]:.1f}ms errors={errors} pass={m["passed"]}',flush=True)
        self.store(f'{kind}-{self.mode}-c{c}',m,raw,samples)
        if kind=='send':
            if m['after']['consume']: raise RuntimeError('refuse to reset while consumers enabled')
            self.reset(stop=False)
        return m

    def wrk_ramp(self,kind):
        good=[];plateau=0;best=0
        for c in [4,8,16,32,64,128,256,512,1024]:
            m=self.wrk_stage(kind,c,self.args.duration)
            if m['passed']:
                good.append(m)
                plateau=plateau+1 if m['rps']<=best*1.05 else 0
                best=max(best,m['rps'])
            if not m['passed'] or plateau>=2:break
        if not good:return
        # Start verification with a fresh network namespace, then keep the SAME
        # runtime for two consecutive trials. Failure triggers a lower candidate.
        for candidate in sorted(good,key=lambda m:m['rps'],reverse=True):
            self.configure(self.mode,False)
            verified=[]
            for i in range(2):
                trial=self.wrk_stage(kind,candidate['concurrency'],self.args.verify_duration,verify=True)
                verified.append(trial)
                if not trial['passed']:break
            if len(verified)==2 and all(m['passed'] for m in verified):break

    def queue_stage(self,rate,duration,verify=False):
        baseline=self.stats();samples=[];started=time.monotonic()
        cmd=['docker','exec',self.load_id,'/perf/msgmate-perf','-action','load','-rps',str(rate),'-duration',f'{duration}s','-workers','2048']
        p=subprocess.Popen(cmd,cwd=ROOT,env=self.env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        while p.poll() is None:
            time.sleep(2);samples.append({'elapsed':time.monotonic()-started,'stats':self.stats()})
            if time.monotonic()-started>duration+30:p.kill();raise RuntimeError('load scheduling hung')
        raw=p.communicate()[0]
        if p.returncode:raise RuntimeError('load failed\n'+raw)
        m=json.loads(raw);at_stop=self.stats();drain_start=time.monotonic()
        while True:
            final=self.stats()
            if final['counts']['Pending']==0 or time.monotonic()-drain_start>=60:break
            time.sleep(1)
        drain=time.monotonic()-drain_start
        # Compare first/last samples in the final half of the scheduling window.
        tail=[s for s in samples if duration*.5<=s['elapsed']<duration]
        slope=0;completion=0
        if len(tail)>=2:
            a,b=tail[0],tail[-1]
            dt=(datetime.datetime.fromisoformat(b['stats']['time'].replace('Z','+00:00'))-datetime.datetime.fromisoformat(a['stats']['time'].replace('Z','+00:00'))).total_seconds()
            slope=(b['stats']['counts']['Pending']-a['stats']['counts']['Pending'])/dt
            completion=(b['stats']['counts']['Completed']-a['stats']['counts']['Completed'])/dt
        completed=final['counts']['Completed']-baseline['counts']['Completed']
        failed=final['counts']['Failed']-baseline['counts']['Failed']
        persisted=final['counts']['Total']-baseline['counts']['Total']
        m.update({'suite':'queue','mode':self.mode,'duration':duration,'verify':verify,'before':baseline,'at_stop':at_stop,'after':final,'backlog_slope_rps':slope,'tail_completion_rps':completion,'drain_seconds':drain,'completed':completed,'failed':failed,'persisted':persisted,'resources':self.resources(),'command':cmd})
        m['passed']=m['accepted']>=m['scheduled']*.99 and m['transport_errors']==0 and m['business_errors']==0 and m['missed']==0 and failed==0 and final['counts']['Retries']==baseline['counts']['Retries'] and at_stop['counts']['Pending']<=max(10,rate*.25) and final['counts']['Pending']==0 and completed==m['accepted']==persisted and len(tail)>=2 and slope<=rate*.05 and completion>=rate*.95 and not any(s['stats'].get('stats_error') for s in samples) and m['p99_ms']<=2000
        print(f'queue/{self.mode} offered={rate}/s accepted={m["accepted"]}/{m["scheduled"]} completed={completed} backlog_slope={slope:.1f}/s tail_completion={completion:.1f}/s drain={drain:.1f}s pass={m["passed"]}',flush=True)
        self.store(f'queue-{self.mode}-r{rate}',m,raw,samples)
        if final['counts']['Pending']:raise RuntimeError('queue did not drain within 60s; stop ramp')
        return m

    def queue_ramp(self):
        low=0;high=None
        for rate in self.args.queue_rates:
            m=self.queue_stage(rate,self.args.duration)
            if m['passed']:low=rate
            else:high=rate;break
        if low and high:
            for i in range(3):
                if high-low<=max(2,low*.1):break
                rate=(low+high)//2
                m=self.queue_stage(rate,self.args.verify_duration)
                if m['passed']:low=rate
                else:high=rate
        if low:
            candidates=sorted({m['offered_rps'] for m in self.results if m['suite']=='queue' and m['mode']==self.mode and m['passed']},reverse=True)
            for candidate in candidates:
                trials=[]
                for i in range(2):
                    trial=self.queue_stage(candidate,self.args.verify_duration,verify=True)
                    trials.append(trial)
                    if not trial['passed']:break
                if len(trials)==2 and all(m['passed'] for m in trials):break

    def report(self):
        lines=['# MsgMate 渐进性能测试日志','','测试日期与时区：'+self.out.name+' / Asia/Shanghai。', '执行状态：'+self.environment.get('status','running')+'。','',
        '**范围**：当前真实 HTTP 路由、MySQL/Redis/Kafka、生产消费者；外部渠道替换为进程内零延迟模拟发送器。不能据此宣称 SMTP/飞书吞吐、收件质量、多实例安全或生产 SLA。', '',
        '**方法**：wrk 固定并发阶梯测试入口和记录查询；独立于响应完成的固定到达速率测试队列，均衡 High/Middle/Low。未改变生产消费者批量、轮询、连接池及 Kafka 写入策略。查询轮转 10,000 条记录，每次直接读 MySQL。入口测试关闭消费者，单独量化持久化受理；队列测试开启真实消费者。', '',
        '**测试判据**：业务成功率至少 99%，且无业务/传输错误；查询 P99 ≤ 200ms、发送 P99 ≤ 2000ms；队列后半段积压增长 ≤ 提交速率的 5%，完成速率 ≥ 目标到达速率的 95%，控制器观察负载停止时待处理数不超过 max(10,目标速率×0.25)，之后 60 秒内排空，全部受理消息最终成功且无失败/重试。判据是本次实验选定的容量边界，并非生产承诺。', '',
        '**可复核性**：environment.json 记录资源、源码哈希、Git 状态、暂停容器、镜像及阈值；每轮 txt 为原始输出，json 为结构化结果，samples.json 为运行期间快照。固定速率测试的 missed 也计入未达标，不能隐藏负载生成器过载。', '',
        '| 测试 | 模式 | 并发 / 目标RPS | 时长 | 成功响应RPS / 后半段完成RPS | P99 ms | 错误或漏发 | 积压增长/s | 排空秒 | 达标 | 复测 | 原始日志 |',
        '|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|']
        for m in self.results:
            queue=m['suite']=='queue'
            rate=m['tail_completion_rps'] if queue else m['rps']
            errs=m['transport_errors']+m['business_errors']+m['missed'] if queue else sum(m[k] for k in ['bad','connect','read','write','timeout','http_errors'])
            lines.append(f'| {m["suite"]} | {m["mode"]} | {m.get("offered_rps",m.get("concurrency"))} | {m["duration"]}s | {rate:.1f} | {m["p99_ms"]:.2f} | {errs} | {m.get("backlog_slope_rps",0):.1f} | {m.get("drain_seconds",0):.1f} | {"是" if m["passed"] else "否"} | {"是" if m["verify"] else "否"} | [{m["artifact"]}]({m["artifact"]}.txt) |')
        lines+=['','## 可重复验证的边界','']
        for kind,mode in [('record','mysql'),('send','mysql'),('send','kafka'),('queue','mysql'),('queue','kafka')]:
            selected=[m for m in self.results if m['suite']==kind and m['mode']==mode]
            if not selected:continue
            verified=[]
            for candidate in selected:
                key='offered_rps' if kind=='queue' else 'concurrency'
                if candidate['verify']:
                    candidate_group=[m for m in selected if m['verify'] and m.get(key)==candidate.get(key)]
                    if len(candidate_group)>=2 and all(m['passed'] for m in candidate_group):verified=candidate_group
            if len(verified)>=2 and all(m['passed'] for m in verified):
                if kind=='queue':text=f'目标 {verified[0]["offered_rps"]} 条/s，两次复测均满足积压与完成判据。'
                else:text=f'{verified[0]["concurrency"]} 并发，两次复测成功响应吞吐 {min(m["rps"] for m in verified):.1f}–{max(m["rps"] for m in verified):.1f} req/s，P99 最大 {max(m["p99_ms"] for m in verified):.2f}ms。'
                lines.append(f'- {kind}/{mode}：{text}')
            else:lines.append(f'- {kind}/{mode}：尚未获得两次达标复测，不能写成稳定阈值。')
        (self.out/'TEST_LOG.md').write_text('\n'.join(lines)+'\n')
        (self.out/'results.json').write_text(json.dumps(self.results,ensure_ascii=False,indent=2)+'\n')

    def execute(self):
        self.capture()
        try:
            self.pause();self.build();self.start()
            suite=self.args.suite
            if suite in ['all','record']:
                self.configure('mysql',False);self.wrk_ramp('record')
            for mode in ['mysql','kafka']:
                if suite in ['all','send']:
                    self.configure(mode,False);self.wrk_ramp('send');self.reset()
                if suite in ['all',mode]:
                    self.configure(mode,True);self.queue_ramp();self.reset()
            self.environment['status']='complete'
        except BaseException as e:
            self.environment['status']='incomplete'
            (self.out/'ERROR.txt').write_text(str(e)+'\n')
            raise
        finally:
            print('Cleaning disposable benchmark containers and restoring paused services...',flush=True)
            try:
                (self.out/'container-logs.txt').write_text(self.compose('logs','--tail','80'))
                cid=self.compose('ps','-q','mysql').strip()
                if cid:
                    diag=self.command(['docker','exec',cid,'sh','-c','MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -e "SHOW GLOBAL STATUS WHERE Variable_name IN (\"Aborted_connects\",\"Connections\",\"Threads_connected\",\"Max_used_connections\");"'],check=False)
                    (self.out/'mysql-status.txt').write_text(diag)
            except Exception as diagnostic_error:(self.out/'diagnostic-error.txt').write_text(str(diagnostic_error))
            if self.monitor:
                self.monitor.terminate();self.monitor.wait(timeout=10)
            cleanup=self.command(self.base_cmd+['down','-v','--remove-orphans'],timeout=120,check=False)
            self.environment['cleanup_output']=cleanup
            if self.stopped:self.command(['docker','start',*self.stopped],timeout=120)
            self.environment['containers_after']=self.command(['docker','ps','--format','{{.Names}} {{.Image}}']).strip()
            self.save_environment();self.report()
            print('Report:',self.out/'TEST_LOG.md',flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite',choices=['all','send','mysql','kafka','record'],default='all')
    parser.add_argument('--duration',type=int,default=20)
    parser.add_argument('--verify-duration',type=int,default=40)
    parser.add_argument('--pause-unrelated',action='store_true')
    parser.add_argument('--queue-rates',default='10,20,40,80,160,320,640,1280',help='ascending comma-separated offered rates for queue refinement')
    args=parser.parse_args()
    try: args.queue_rates=[int(x) for x in args.queue_rates.split(',')]
    except ValueError: parser.error('queue rates must be integers')
    if not args.queue_rates or min(args.queue_rates)<1 or args.queue_rates!=sorted(set(args.queue_rates)):parser.error('queue rates must be positive, distinct, ascending')
    if args.duration<10 or args.verify_duration<20:parser.error('use duration >= 10 and verify-duration >= 20')
    os.environ['CGO_ENABLED']='0';os.environ.setdefault('GOCACHE','/tmp/msgmate-go-cache')
    Bench(args).execute()
