import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from .grouping import describe, fallback_parent, summarize


def utc_now():
    return datetime.now(timezone.utc).isoformat()


SENSITIVE_KEY = re.compile(r"password|passwd|secret|authorization|cookie|encrypted[_-]?token|api[_-]?key$|session[_-]?hash|^(?:access|refresh|id|auth|session)[_-]?token$|^token$", re.I)
TEXT_KEY = r"(?:password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|auth[_-]?token|session[_-]?token|token|authorization|cookie|set-cookie)"


def normalize_time(value):
    dt = datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if dt.tzinfo is None:
        raise ValueError('时间必须包含时区')
    return dt.astimezone(timezone.utc).isoformat(timespec='microseconds')


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: ("[已隐藏]" if SENSITIVE_KEY.search(str(k)) else redact(v)) for k,v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(('{','[')):
            try:
                decoded = json.loads(value)
                if isinstance(decoded,(dict,list)):
                    return json.dumps(redact(decoded),ensure_ascii=False)
            except (ValueError,RecursionError):
                pass
        value = re.sub(r'(?im)((?:authorization|proxy-authorization|cookie|set-cookie)\s*:\s*)[^\r\n]+', r'\1[已隐藏]', value)
        value = re.sub(r'(?i)(bearer\s+|basic\s+)[A-Za-z0-9._~+/=-]+',r'\1[已隐藏]',value)
        value = re.sub(r'\bsk-[A-Za-z0-9_-]{8,}', '[已隐藏]', value)
        value = re.sub(r'''(?i)(["']?''' + TEXT_KEY + r'''["']?\s*[=:]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s&,;}\]]+)''',r'\1"[已隐藏]"',value)
    return value


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events (event_key TEXT PRIMARY KEY, source TEXT, user_id TEXT, status TEXT, is_error INTEGER, created_at TEXT, updated_at TEXT, payload TEXT);
                CREATE INDEX IF NOT EXISTS events_user_time ON events(user_id,created_at DESC);
                CREATE INDEX IF NOT EXISTS events_error_time ON events(is_error,created_at DESC);
                CREATE INDEX IF NOT EXISTS events_time ON events(created_at DESC,event_key DESC);
                CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, event_key TEXT, observed_at TEXT, status TEXT, digest TEXT, payload TEXT);
                CREATE INDEX IF NOT EXISTS history_key ON history(event_key,id);
                CREATE TABLE IF NOT EXISTS checkpoints (source TEXT PRIMARY KEY, payload TEXT);
                CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, email TEXT);
                CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, payload TEXT);
            """)
        self.upgrade_storage()

    def upgrade_storage(self):
        if self.get_state('storage_version') == 2:
            return
        def clean(payload):
            item=redact(json.loads(payload))
            for field in ('created_at','updated_at'):
                if item.get(field): item[field]=normalize_time(item[field])
            if item.get('source') in ('message','tool') and not item.get('mapping_status'):
                item['user_id']='unverified:'+item['source']+':'+str(item['id'])
                item['mapping_status']='pending_recheck'
                item['user_name']='未归属账号（待重新核对）'
                item.pop('user_email',None)
            return item,json.dumps(item,ensure_ascii=False,sort_keys=True)
        with self.connect() as db:
            db.execute('PRAGMA secure_delete=ON')
            for row in db.execute('SELECT event_key,payload FROM events').fetchall():
                item,payload=clean(row['payload'])
                db.execute('UPDATE events SET user_id=?,created_at=?,updated_at=?,payload=? WHERE event_key=?',(item.get('user_id','unattributed'),item.get('created_at',''),item.get('updated_at',''),payload,row['event_key']))
            for row in db.execute('SELECT id,payload FROM history').fetchall():
                _,payload=clean(row['payload'])
                db.execute('UPDATE history SET payload=?,digest=? WHERE id=?',(payload,hashlib.sha256(payload.encode()).hexdigest(),row['id']))
            # 保留旧记录，重新补采关联与边界，去重后恢复确认过的账号归属。
            db.execute('DELETE FROM checkpoints')
            db.execute("INSERT OR REPLACE INTO state VALUES ('storage_version','2')")
        with self.connect() as db:
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            db.execute('VACUUM')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def ingest(self, rows, source=None, checkpoint=None):
        changed = 0
        with self.connect() as db:
            for row in rows:
                item = redact(dict(row))
                for field in ("created_at","updated_at"):
                    if item.get(field): item[field] = normalize_time(item[field])
                item["id"] = str(item["id"])
                item["user_id"] = str(item.get("user_id") or "unattributed")
                item["key"] = item["source"] + ":" + item["id"]
                item["is_error"] = item.get("status") in {"failed", "error", "timeout"} or bool(item.get("error_code") or item.get("error_message") or item.get("error"))
                payload = json.dumps(item, ensure_ascii=False, sort_keys=True)
                old = db.execute("SELECT payload FROM events WHERE event_key=?", (item["key"],)).fetchone()
                if old and old[0] == payload:
                    continue
                changed += 1
                db.execute("INSERT OR REPLACE INTO events VALUES (?,?,?,?,?,?,?,?)", (item["key"],item["source"],item["user_id"],item.get("status", ""),int(item["is_error"]),item.get("created_at", ""),item.get("updated_at", ""),payload))
                digest = hashlib.sha256(payload.encode()).hexdigest()
                db.execute("INSERT INTO history(event_key,observed_at,status,digest,payload) VALUES (?,?,?,?,?)", (item["key"],utc_now(),item.get("status", ""),digest,payload))
            if source and checkpoint is not None:
                db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?,?)", (source,json.dumps(checkpoint)))
        return changed

    def checkpoint(self, source):
        with self.connect() as db:
            row = db.execute("SELECT payload FROM checkpoints WHERE source=?", (source,)).fetchone()
        return json.loads(row[0]) if row else None

    def set_state(self, key, value):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (key,json.dumps(value,ensure_ascii=False)))

    def get_state(self, key, default=None):
        with self.connect() as db:
            row = db.execute("SELECT payload FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def save_users(self, users):
        with self.connect() as db:
            db.executemany("INSERT OR REPLACE INTO users VALUES (?,?,?)", [(str(u["id"]),u.get("name") or "未命名账号",u.get("email") or "") for u in users])

    def _where(self, user_id="", mode="all", q="", source="", status="", since="", until="", **_):
        where, params = [], []
        for column, value in [("e.user_id",user_id),("e.source",source),("e.status",status)]:
            if value:
                where.append(column+"=?")
                params.append(str(value))
        if mode == "errors":
            where.append("e.is_error=1")
        if q:
            where.append("(e.payload LIKE ? ESCAPE '\\' OR u.name LIKE ? ESCAPE '\\' OR u.email LIKE ? ESCAPE '\\')")
            text = "%"+q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")+"%"
            params.extend([text]*3)
        if since:
            where.append("e.created_at>=?"); params.append(normalize_time(since))
        if until:
            where.append("e.created_at<=?"); params.append(normalize_time(until))
        return (" WHERE "+" AND ".join(where) if where else ""), params

    def query(self, limit=50, offset=0, grouped=False, **filters):
        if grouped:
            return self.grouped_query(limit=limit, offset=offset, **filters)
        clause, params = self._where(**filters)
        join = " FROM events e LEFT JOIN users u ON u.id=e.user_id"
        with self.connect() as db:
            total = db.execute("SELECT count(*)"+join+clause, params).fetchone()[0]
            rows = db.execute("SELECT e.payload,u.name,u.email"+join+clause+" ORDER BY e.created_at DESC,e.event_key DESC LIMIT ? OFFSET ?", params+[max(1,min(int(limit),200)),max(0,int(offset))]).fetchall()
        items = []
        for row in rows:
            item = json.loads(row[0]); item.update(user_name=row[1] or item.get("user_name") or "未归属账号",user_email=row[2] or item.get("user_email") or "")
            items.append(item)
        return {"total":total,"items":items}

    def grouped_query(self, limit=50, offset=0, **filters):
        # Match records first, but resolve complete groups before pagination.
        clause, params = self._where(**filters)
        with self.connect() as db:
            matched = {r[0] for r in db.execute('SELECT e.event_key FROM events e LEFT JOIN users u ON u.id=e.user_id'+clause, params)}
            scope, values = self._where(user_id=filters.get('user_id', ''))
            rows = db.execute('SELECT e.payload,u.name,u.email FROM events e LEFT JOIN users u ON u.id=e.user_id'+scope, values).fetchall()
        records = []
        for row in rows:
            item = json.loads(row[0])
            item.update(user_name=row[1] or item.get('user_name') or '未归属账号', user_email=row[2] or item.get('user_email') or '')
            describe(item)
            item['matched'] = item['key'] in matched
            records.append(item)
        index = {}
        for item in records:
            if item['source']=='webchat' and item.get('request_id'):
                index.setdefault((item['user_id'],item['request_id']),[]).append(item)
        def parent_of(item):
            request = item.get('parent_request_id')
            if not request and item['source']=='gateway': request=item.get('client_request_id') or item.get('request_id')
            seen_requests = set()
            while request and request not in seen_requests:
                seen_requests.add(request)
                candidates = index.get((item['user_id'], request), [])
                if len(candidates) == 1 and candidates[0]['key'] != item['key']:
                    candidate = candidates[0]
                    if item.get('conversation_id') and candidate.get('conversation_id') and item['source'] != 'message' and item['conversation_id'] != candidate['conversation_id']:
                        return None
                    return candidate
                if candidates: return None
                request = fallback_parent(request)
            return None
        groups = {}
        for item in records:
            root=item; seen={item['key']}
            while (parent:=parent_of(root)) is not None:
                if parent['key'] in seen: root=item; break
                seen.add(parent['key']);root=parent
            group=groups.setdefault(root['key'],{**root,'children':[]})
            if root['key']!=item['key']: group['children'].append(item)
        selected = []
        for group in groups.values():
            members = [group]+group['children']
            if not any(r['key'] in matched for r in members): continue
            group['record_count'] = len(members)
            group['matched_count'] = sum(r['key'] in matched for r in members)
            group['error_count'] = sum(bool(r.get('is_error')) for r in members)
            summarize(group)
            group['children'].sort(key=lambda r:(r.get('created_at',''),r['key']))
            selected.append(group)
        selected.sort(key=lambda r:(r.get('created_at',''),r['key']), reverse=True)
        start = max(0,int(offset))
        return {'total':len(selected), 'record_total':len(matched), 'items':selected[start:start+max(1,min(int(limit),200))]}

    def users(self):
        with self.connect() as db:
            rows = db.execute("""SELECT u.id,u.name,u.email,count(e.event_key) total,coalesce(sum(e.is_error),0) errors
                FROM users u LEFT JOIN events e ON e.user_id=u.id GROUP BY u.id
                UNION ALL SELECT e.user_id,'未归属账号','',count(*),sum(e.is_error) FROM events e
                WHERE NOT EXISTS(SELECT 1 FROM users u WHERE u.id=e.user_id) GROUP BY e.user_id
                ORDER BY total DESC,name""").fetchall()
        return [dict(r) for r in rows]

    def detail(self, key):
        with self.connect() as db:
            row = db.execute("SELECT payload FROM events WHERE event_key=?",(key,)).fetchone()
            if not row:
                return None
            item = json.loads(row[0])
            user = db.execute("SELECT name,email FROM users WHERE id=?",(item["user_id"],)).fetchone()
            if user: item.update(user_name=user[0],user_email=user[1])
            history = db.execute("SELECT observed_at,status,payload FROM history WHERE event_key=? ORDER BY id",(key,)).fetchall()
        item["history"] = [{"observed_at":r[0],"status":r[1],"record":json.loads(r[2])} for r in history]
        return item

    def stats(self, **filters):
        clause, params = self._where(**filters)
        with self.connect() as db:
            row = db.execute("SELECT count(*) total,coalesce(sum(e.is_error),0) errors,count(DISTINCT e.user_id) accounts,coalesce(sum(e.status IN ('running','pending','settlement_pending')),0) active,min(e.created_at) earliest,max(e.updated_at) latest FROM events e LEFT JOIN users u ON u.id=e.user_id"+clause,params).fetchone()
            counts = db.execute("SELECT source,count(*) total FROM events GROUP BY source").fetchall()
        return {**dict(row),"sources":[dict(r) for r in counts]}
