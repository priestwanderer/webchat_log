import argparse
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from webchat_log.collector import Collector, DEFAULTS
from webchat_log.server import create_server
from webchat_log.store import Store

ROOT=Path(__file__).resolve().parent


def main():
    parser=argparse.ArgumentParser(description='正式服 webchat 只读日志监控')
    parser.add_argument('--config',default=str(ROOT/'config.json'))
    parser.add_argument('--once',action='store_true',help='采集一轮后退出')
    parser.add_argument('--port',type=int)
    args=parser.parse_args()
    path=Path(args.config)
    config={**DEFAULTS,**(json.loads(path.read_text(encoding='utf-8')) if path.exists() else {})}
    if args.port:config['port']=args.port
    (ROOT/'data').mkdir(exist_ok=True)
    handler=RotatingFileHandler(ROOT/'data'/'collector.log',maxBytes=5_000_000,backupCount=3,encoding='utf-8')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[handler,logging.StreamHandler()])
    store=Store(ROOT/'data'/'webchat.sqlite3')
    collector=Collector(store,config)
    if args.once:
        collector.poll_once()
        print(json.dumps({'status':collector.status(),'stats':store.stats()},ensure_ascii=False))
        return
    server=create_server(store,collector,host=config['host'],port=config['port'])
    collector.start()
    print(f"日志监控已启动：http://127.0.0.1:{server.server_port}",flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:print('正在停止日志监控。',flush=True)
    finally:collector.stop();server.server_close()


if __name__=='__main__':main()
