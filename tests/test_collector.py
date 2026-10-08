import json
import os
import tempfile
import unittest
from pathlib import Path
from webchat_log.store import Store

try:
    from webchat_log.collector import Collector
    from webchat_log.remote import build_query
except ImportError:
    Collector = None
    build_query = None


class CollectorTests(unittest.TestCase):
    def test_readonly_query_uses_tuple_cursor_and_rejects_invalid_source(self):
        self.assertIsNotNone(build_query, "尚未实现只读增量采集")
        sql = build_query("webchat", {"time":"2026-10-08T00:00:00+00:00", "id":"42"}, "2026-10-08T01:00:00+00:00", 100)
        self.assertIn("r.updated_at", sql)
        self.assertIn("r.id", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("LIMIT 100", sql)
        self.assertNotIn("password_hash", sql)
        with self.assertRaises(ValueError):
            build_query("users; DROP TABLE users", {}, "", 100)
        with self.assertRaises(ValueError):
            build_query("webchat", {"time":"';DROP TABLE users;--", "id":"1"}, "2026-10-08T00:00:00Z", 100)

    def test_partial_source_failure_keeps_checkpoint_and_successful_rows(self):
        self.assertIsNotNone(Collector, "尚未实现采集调度")
        with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as folder:
            store = Store(Path(folder)/"logs.sqlite3")
            store.ingest([], source="message", checkpoint={"time":"2026-10-07T00:00:00Z","id":"x"})
            row = {"source":"webchat","id":"1","user_id":"7","status":"failed","error_message":"超时","created_at":"2026-10-08T00:00:00Z","updated_at":"2026-10-08T00:00:01Z"}
            def fetch(payload):
                return {"server_time":"2026-10-08T00:00:03Z","users":[{"id":"7","name":"测试","email":"test@example.test"}],"sources":{"webchat":{"items":[row],"cursor":{"time":"2026-10-08T00:00:01Z","id":"1"},"has_more":False},"message":{"error":"查询超时"}}}
            collector = Collector(store, {"sources":["webchat","message"],"initial_hours":24}, transport=fetch)
            collector.poll_once()
            self.assertEqual(store.query(mode="errors")["total"],1)
            self.assertEqual(store.checkpoint("message")["id"],"x")
            self.assertEqual(store.checkpoint("webchat")["id"],"1")
            self.assertEqual(collector.status()["state"],"degraded")
            collector.poll_once()
            self.assertEqual(len(store.detail("webchat:1")["history"]),1)


if __name__ == "__main__":
    unittest.main()
