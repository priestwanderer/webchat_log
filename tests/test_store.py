import tempfile
import os

tempfile.tempdir = os.environ.get("TMPDIR", "C:/Users/Administrator/AppData/Local/hermes/cache/scratch")
import unittest
import json
from pathlib import Path

try:
    from webchat_log.store import Store
except ImportError:
    Store = None


class StoreTests(unittest.TestCase):
    def test_records_are_durable_and_filterable_by_account_and_error(self):
        self.assertIsNotNone(Store, "尚未实现日志存储")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "events.sqlite3"
            store = Store(path)
            rows = [
                {"id": "1", "source": "webchat", "user_id": "7", "request_id": "req-a", "status": "completed", "created_at": "2026-10-08T01:00:00+00:00", "updated_at": "2026-10-08T01:01:00+00:00"},
                {"id": "2", "source": "webchat", "user_id": "8", "request_id": "req-b", "status": "failed", "error_message": "连接超时", "created_at": "2026-10-08T01:00:00+00:00", "updated_at": "2026-10-08T01:01:00+00:00"},
            ]
            store.ingest(rows)
            self.assertEqual(store.query()["total"], 2)
            self.assertEqual(store.query(mode="errors")["items"][0]["request_id"], "req-b")
            self.assertEqual(store.query(user_id="7", mode="errors")["total"], 0)
            self.assertEqual(Store(path).query()["total"], 2)


    def test_updates_keep_history_and_checkpoint_in_same_commit(self):
        self.assertTrue(hasattr(Store, "checkpoint"), "尚未实现断点和状态历史")
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "events.sqlite3")
            row = {"id":"1", "source":"webchat", "user_id":"7", "request_id":"req-a", "status":"running", "created_at":"2026-10-08T01:00:00+00:00", "updated_at":"2026-10-08T01:00:00+00:00"}
            store.ingest([row], source="webchat", checkpoint={"time":"2026-10-08T01:00:00+00:00","id":"1"})
            row.update(status="failed", error_message="Authorization: Bearer secret-value", updated_at="2026-10-08T01:01:00+00:00")
            store.ingest([row], source="webchat", checkpoint={"time":"2026-10-08T01:01:00+00:00","id":"1"})
            store.ingest([row])
            detail = store.detail("webchat:1")
            self.assertEqual(len(detail["history"]), 2)
            self.assertNotIn("secret-value", json.dumps(detail))
            self.assertEqual(store.checkpoint("webchat")["id"], "1")
            self.assertEqual(store.query()["total"], 1)

    def test_user_search_and_status_filters_are_parameterized(self):
        self.assertTrue(hasattr(Store, "users"), "尚未实现账号目录")
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "events.sqlite3")
            store.save_users([{"id":"7", "name":"测试账号", "email":"test@example.test"}])
            store.ingest([{"id":"1", "source":"webchat", "user_id":"7", "status":"cancelled", "request_id":"needle", "created_at":"2026-10-08T01:00:00Z", "updated_at":"2026-10-08T01:00:00Z"}])
            self.assertEqual(store.query(q="测试账号")["total"], 1)
            self.assertEqual(store.query(q="' OR 1=1 --")["total"], 0)
            self.assertEqual(store.query(mode="errors")["total"], 0)
            self.assertEqual(store.query(status="cancelled")["total"], 1)
            self.assertEqual(store.users()[0]["total"], 1)

if __name__ == "__main__":
    unittest.main()
