import json,time
from webchat_log.store import Store
from webchat_log.collector import Collector
c=Collector(Store('data/webchat.sqlite3'),{})
payload={'config':{k:c.config[k] for k in ['db_container','db_port','db_user','batch_size']},'cursors':{s:{'time':c.store.get_state('coverage_start'),'id':'0' if s in ['webchat','audio','gateway'] else ''} for s in ['audio','gateway','message','tool']},'include_users':False}
a=time.monotonic()
d=c.ssh_fetch(payload); print(json.dumps({'users':len(d.get('users',[])),'sources':{k:{'rows':len(v.get('items',[])),'error':v.get('error')} for k,v in d['sources'].items()}},ensure_ascii=False),time.monotonic()-a)
