import base64
import json
import logging
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from .remote import SOURCES, timestamp
from .store import redact, utc_now


DEFAULTS = {
    "ssh_host":"root@38.147.163.155", "ssh_port":22, "ssh_identity_file":"",
    "db_container":"migration-replica-postgres-production", "db_port":55432, "db_user":"sub2api",
    "poll_seconds":10, "initial_hours":24, "overlap_seconds":120, "batch_size":500,
    "sources":["webchat","audio","gateway","message","tool"], "host":"127.0.0.1", "port":8765,
}


class Collector:
    def __init__(self, store, config, transport=None):
        self.store = store
        self.config = {**DEFAULTS, **config}
        self.transport = transport or self.ssh_fetch
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self._status = {"state":"starting", "message":"正在连接正式服", "sources":{}, "last_success":None, "last_attempt":None, "version":0}
        self.last_users = 0
        self.thread = None
        if not store.get_state("coverage_start"):
            store.set_state("coverage_start",(datetime.now(timezone.utc)-timedelta(hours=self.config['initial_hours'])).isoformat())

    def ssh_fetch(self, payload):
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        source = Path(__file__).with_name('remote.py').read_text(encoding='utf-8')
        script = base64.b64encode(source.encode()).decode()
        runner = "import base64;exec(compile(base64.b64decode('"+script+"'),'<readonly-collector>','exec'))"
        import shlex
        remote_command = 'python3 - '+shlex.quote(encoded)
        command = ['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=12','-o','ServerAliveInterval=10','-o','ServerAliveCountMax=2','-p',str(self.config['ssh_port'])]
        if self.config.get('ssh_identity_file'):
            command += ['-i',self.config['ssh_identity_file']]
        command += [self.config['ssh_host'],remote_command]
        result = subprocess.run(command,input=source,capture_output=True,text=True,encoding='utf-8',timeout=110)
        if result.returncode:
            raise RuntimeError(redact(result.stderr.strip() or result.stdout.strip() or 'SSH 连接失败'))
        data = json.loads(result.stdout)
        if data.get('fatal_error'):
            raise RuntimeError(data['fatal_error'])
        return data

    def poll_once(self):
        started = time.monotonic()
        with self.lock:
            self._status['last_attempt'] = utc_now()
        cursors = {}
        for source in self.config['sources']:
            checkpoint = self.store.checkpoint(source)
            if checkpoint is None:
                checkpoint = {'time':self.store.get_state('coverage_start'),'id':'0' if SOURCES[source][3] else ''}
            elif not checkpoint.get('paging'):
                checkpoint = {**checkpoint,'time':(timestamp(checkpoint['time'])-timedelta(seconds=self.config['overlap_seconds'])).isoformat(),'id':'0' if SOURCES[source][3] else ''}
            cursors[source] = checkpoint
        payload = {'config':{k:self.config[k] for k in ['db_container','db_port','db_user','batch_size']},'cursors':cursors,'include_users':time.monotonic()-self.last_users>300 or not self.last_users}
        try:
            data = self.transport(payload)
            if 'users' in data:
                self.store.save_users(data['users']); self.last_users=time.monotonic()
            errors = {}
            if data.get('users_error'): errors['users']=redact(data['users_error'])
            sources, changed, pending = {}, 0, False
            for source in self.config['sources']:
                part = data.get('sources',{}).get(source,{'error':'采集结果缺少该来源'})
                if part.get('error'):
                    errors[source]=redact(part['error']); sources[source]={'state':'error','error':redact(part['error'])}
                    continue
                changed += self.store.ingest(part['items'],source=source,checkpoint=part['cursor'])
                pending = pending or part['has_more']
                sources[source]={'state':'backfill' if part['has_more'] else 'live','received':len(part['items']),'watermark':part['cursor']['time']}
            state = 'degraded' if errors else ('backfill' if pending else 'live')
            with self.lock:
                self._status.update(state=state,message='部分来源采集失败，正在重试' if errors else ('正在补采历史记录' if pending else '正式服同步正常'),sources=sources,errors=errors,last_success=utc_now() if len(errors)<len(self.config['sources']) else self._status['last_success'],server_time=data['server_time'],duration_ms=round((time.monotonic()-started)*1000),last_changed=changed,version=self._status['version']+1)
            self.store.set_state('collector',self.status())
            return pending
        except Exception as exc:
            with self.lock:
                self._status.update(state='offline',message='连接中断，将自动重试',error=str(redact(str(exc)))[:1500],version=self._status['version']+1)
            logging.warning('采集失败：%s',redact(str(exc)))
            return False

    def status(self):
        with self.lock:
            return {**self._status,'coverage_start':self.store.get_state('coverage_start'),'poll_seconds':self.config['poll_seconds'],'storage_path':self.store.path,'target':'www.string.ink','readonly':True}

    def run(self):
        while not self.stop_event.is_set():
            pending = self.poll_once()
            self.stop_event.wait(2 if pending else max(3,self.config['poll_seconds']))

    def start(self):
        self.thread = threading.Thread(target=self.run,name='正式服日志采集',daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
