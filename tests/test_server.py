import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from webchat_log.store import Store
from webchat_log.collector import Collector
try:
    from webchat_log.server import create_server
except ImportError:
    create_server = None


class ServerTests(unittest.TestCase):
    def test_http_filters_detail_security_and_stream(self):
        self.assertIsNotNone(create_server, "尚未实现网页接口")
        with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as folder:
            store=Store(Path(folder)/'db.sqlite3')
            store.save_users([{'id':'7','name':'测试账号','email':'test@example.test'}])
            store.ingest([{'source':'webchat','id':'1','user_id':'7','status':'failed','error_message':'连接超时','created_at':'2026-10-08T01:00:00Z','updated_at':'2026-10-08T01:00:01Z'}])
            collector=Collector(store,{})
            server=create_server(store,collector,port=0)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            base='http://127.0.0.1:'+str(server.server_port)
            try:
                with urlopen(base+'/api/events?mode=errors&user_id=7') as r:
                    data=json.load(r)
                    self.assertEqual(data['total'],1)
                    self.assertEqual(r.headers['Cache-Control'],'no-store')
                with urlopen(base+'/api/events?user_id=8') as r:self.assertEqual(json.load(r)['total'],0)
                with urlopen(base+'/api/detail?key=webchat%3A1') as r:self.assertEqual(len(json.load(r)['history']),1)
                with self.assertRaises(HTTPError) as err:urlopen(base+'/api/events?limit=invalid')
                self.assertEqual(err.exception.code,400)
                with self.assertRaises(HTTPError) as err:urlopen(Request(base+'/api/status',headers={'Host':'evil.example'}))
                self.assertEqual(err.exception.code,403)
                with self.assertRaises(HTTPError):urlopen(base+'/../config.json')
                with urlopen(base+'/api/stream',timeout=3) as r:
                    self.assertEqual(r.headers['Content-Type'],'text/event-stream; charset=utf-8')
                    self.assertIn(b'event: status',r.readline())
            finally:
                server.shutdown();server.server_close();thread.join(timeout=3)


if __name__=='__main__':unittest.main()
