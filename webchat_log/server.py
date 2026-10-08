import json
import logging
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

STATIC = Path(__file__).resolve().parent.parent / 'static'


def create_server(store, collector, host='127.0.0.1', port=8765):
    if host not in {'127.0.0.1','localhost'}:
        raise ValueError('此版本仅允许本机监听，请通过安全隧道访问')

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            pass

        def headers_common(self):
            self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('X-Frame-Options','DENY')
            self.send_header('Referrer-Policy','no-referrer')
            self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")

        def send_data(self, data, code=200, kind='application/json; charset=utf-8'):
            body = json.dumps(data,ensure_ascii=False).encode() if not isinstance(data,bytes) else data
            self.send_response(code)
            self.headers_common()
            self.send_header('Content-Type',kind)
            self.send_header('Content-Length',str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            host_header = self.headers.get('Host','')
            if host_header not in {f'127.0.0.1:{self.server.server_port}',f'localhost:{self.server.server_port}'}:
                self.send_data({'error':'仅允许本机访问'},403); return
            origin=self.headers.get('Origin')
            if origin and origin not in {f'http://127.0.0.1:{self.server.server_port}',f'http://localhost:{self.server.server_port}'}:
                self.send_data({'error':'不允许跨站访问'},403);return
            parsed=urlsplit(self.path)
            args={k:v[0] for k,v in parse_qs(parsed.query).items()}
            try:
                if parsed.path=='/api/events':
                    allowed={'user_id','mode','q','source','status','since','until','limit','offset','grouped'}
                    args={k:v for k,v in args.items() if k in allowed}
                    args['limit']=int(args.get('limit',50));args['offset']=int(args.get('offset',0))
                    if args.get('mode','all') not in {'all','errors'}: raise ValueError('无效筛选')
                    args['grouped']=args.get('grouped')=='true'
                    self.send_data(store.query(**args))
                elif parsed.path=='/api/stats':
                    self.send_data(store.stats(**args))
                elif parsed.path=='/api/users':
                    self.send_data({'items':store.users()})
                elif parsed.path=='/api/status':
                    self.send_data(collector.status())
                elif parsed.path=='/api/detail':
                    item=store.detail(args.get('key',''))
                    self.send_data(item or {'error':'记录不存在'},200 if item else 404)
                elif parsed.path=='/api/stream':
                    self.send_response(200);self.headers_common()
                    self.send_header('Content-Type','text/event-stream; charset=utf-8')
                    self.send_header('Connection','close');self.end_headers()
                    self.close_connection=True
                    last=None
                    for _ in range(30):
                        status=collector.status()
                        if status['version']!=last:
                            self.wfile.write(('event: status\ndata: '+json.dumps(status,ensure_ascii=False)+'\n\n').encode());last=status['version']
                        else:self.wfile.write(b': heartbeat\n\n')
                        self.wfile.flush()
                        time.sleep(2)
                elif parsed.path in {'/','/app.js','/styles.css'}:
                    name={'/':'index.html','/app.js':'app.js','/styles.css':'styles.css'}[parsed.path]
                    mime={'index.html':'text/html','app.js':'text/javascript','styles.css':'text/css'}[name]
                    self.send_data((STATIC/name).read_bytes(),kind=mime+'; charset=utf-8')
                else:self.send_data({'error':'页面不存在'},404)
            except (ValueError,TypeError):
                self.send_data({'error':'查询参数无效'},400)
            except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):
                pass
            except Exception:
                logging.exception('网页请求失败')
                self.send_data({'error':'读取失败，请查看本地服务日志'},500)

    server=ThreadingHTTPServer((host,port),Handler)
    server.daemon_threads=True
    return server
