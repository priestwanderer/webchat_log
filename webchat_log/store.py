import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: ("[已隐藏]" if re.search(r"password|secret|authorization|cookie|encrypted_token|api_key$|session_hash", k, re.I) else redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        value = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+", r"\1[已隐藏]", value)
        value = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}", "[已隐藏]", value)
        value = re.sub(r"(?i)((?:api[_-]?key|access[_-]?token|password|secret)\s*[=:]\s*)[^\s&,;]+", r"\1[已隐藏]", value)
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
            where.append("e.created_at>=?"); params.append(since)
        if until:
            where.append("e.created_at<=?"); params.append(until)
        return (" WHERE "+" AND ".join(where) if where else ""), params

    def query(self, limit=50, offset=0, **filters):
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
