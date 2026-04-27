import asyncio
import csv
import io
import os
import sqlite3
import shutil
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch
import uuid

import config
import campaign_queue
import database
import tasks
from verification import verifier
from web import create_app
from web.routes._campaign_runner import run_campaign


@contextmanager
def isolated_db():
    original_path = config.DATABASE_PATH
    database.close_db()
    config.DATABASE_PATH = f"file:test-{uuid.uuid4().hex}?mode=memory&cache=shared"
    keeper = sqlite3.connect(config.DATABASE_PATH, uri=True)
    database.init_db()
    try:
        yield
    finally:
        database.close_db()
        keeper.close()
        config.DATABASE_PATH = original_path


@contextmanager
def isolated_config_paths():
    original_settings_path = config.SETTINGS_PATH
    original_secret_key_path = config.SECRET_KEY_PATH

    temp_dir = os.path.join(config.BASE_DIR, ".test-config", uuid.uuid4().hex)
    os.makedirs(temp_dir, exist_ok=True)
    config.SETTINGS_PATH = os.path.join(temp_dir, "settings.json")
    config.SECRET_KEY_PATH = os.path.join(temp_dir, ".flask_secret_key")
    try:
        yield
    finally:
        config.SETTINGS_PATH = original_settings_path
        config.SECRET_KEY_PATH = original_secret_key_path
        shutil.rmtree(temp_dir, ignore_errors=True)


class RegressionTests(unittest.TestCase):
    def setUp(self):
        self._config_context = isolated_config_paths()
        self._config_context.__enter__()
        self._db_context = isolated_db()
        self._db_context.__enter__()
        tasks._tasks.clear()

    def tearDown(self):
        tasks._tasks.clear()
        self._db_context.__exit__(None, None, None)
        self._config_context.__exit__(None, None, None)

    def test_get_db_is_thread_local(self):
        main_conn = database.get_db()
        thread_conn_ids = []

        def worker():
            conn = database.get_db()
            thread_conn_ids.append(id(conn))
            database.close_db()

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()

        self.assertEqual(len(thread_conn_ids), 1)
        self.assertNotEqual(id(main_conn), thread_conn_ids[0])

    def test_campaign_failure_marks_campaign_failed(self):
        campaign_id = database.insert_campaign(
            "Broken campaign",
            ["plumber"],
            ["USA"],
            ["Seattle"],
        )
        task_id = tasks.create_task("campaign")

        with patch("web.routes._campaign_runner.config.get_locations", return_value={"USA": {"tld": ".com", "cities": ["Seattle"]}}), \
             patch("web.routes._campaign_runner._generate_for_combo", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                asyncio.run(run_campaign(task_id, campaign_id))

        campaign = database.get_campaign(campaign_id)
        self.assertEqual(campaign["status"], "failed")

    def test_pagination_urls_preserve_structured_query_params(self):
        app = create_app()
        app.testing = True
        campaign_id = database.insert_campaign("Emails", ["agency"], ["USA"], ["Seattle"])

        for index in range(101):
            database.insert_email(
                email=f"user{index}@example.com",
                domain="example.com",
                source_url="https://example.com/contact?page=99",
                source_domain="example.com",
                campaign_id=campaign_id,
                niche="agency",
                city="Seattle",
                country="USA",
            )

        client = app.test_client()
        response = client.get("/emails?foo=landing+page%3D1&page=2", follow_redirects=True)
        html = response.get_data(as_text=True)

        self.assertIn('href="?foo=landing+page%3D1&amp;page=1"', html)
        self.assertIn('href="?foo=landing+page%3D1&amp;page=3"', html)

    def test_dns_fallback_does_not_mark_known_provider_valid(self):
        result = verifier._dns_based_verify("user@gmail.com", "gmail.com", "mx.google.com")

        self.assertEqual(result["verification"], "risky")
        self.assertEqual(result["verification_method"], "dns_provider")
        self.assertEqual(result["domain_confidence"], "high")

    def test_catch_all_is_downgraded_from_valid(self):
        with patch("verification.verifier._get_mx_cached", return_value=(True, "mx.example.com")), \
             patch("verification.verifier._test_smtp_availability", new=AsyncMock(return_value=True)), \
             patch("verification.verifier.check_smtp", new=AsyncMock(side_effect=["valid", "valid"])):
            result = asyncio.run(verifier.verify_email("user@example.com"))

        self.assertEqual(result["verification"], "risky")
        self.assertEqual(result["verification_method"], "smtp_catch_all")
        self.assertEqual(result["is_catch_all"], 1)

    def test_safe_role_inbox_is_not_hard_flagged_as_spam_trap(self):
        self.assertIsNone(verifier.check_spam_trap("abuse@example.com", "example.com"))

    def test_export_can_include_verification_metadata(self):
        app = create_app()
        app.testing = True
        client = app.test_client()
        campaign_id = database.insert_campaign("Export", ["agency"], ["USA"], ["Seattle"])

        database.insert_email(
            email="user@example.com",
            domain="example.com",
            source_url="https://example.com/contact",
            source_domain="example.com",
            campaign_id=campaign_id,
            niche="agency",
            city="Seattle",
            country="USA",
        )
        email_row = database.get_all_emails_filtered()[0]
        database.update_email_verification(
            email_id=email_row["id"],
            verification="risky",
            mx_valid=1,
            smtp_valid=None,
            verification_method="dns_provider",
            mailbox_confidence="unknown",
            domain_confidence="high",
            is_catch_all=0,
        )

        response = client.get(
            "/emails/export?columns=email,verification_method,mailbox_confidence,domain_confidence,is_catch_all"
        )
        reader = csv.reader(io.StringIO(response.get_data(as_text=True)))
        rows = list(reader)

        self.assertEqual(
            rows[0],
            ["Email", "Verification Method", "Mailbox Confidence", "Domain Confidence", "Catch-All"],
        )
        self.assertEqual(rows[1], ["user@example.com", "dns_provider", "unknown", "high", "0"])

    def test_login_session_survives_across_app_instances(self):
        config.save_settings({"app_password": "letmein", "app_password_hash": ""})

        app_one = create_app()
        app_one.testing = True
        client_one = app_one.test_client()

        login_response = client_one.post("/login", data={"password": "letmein"})
        self.assertEqual(login_response.status_code, 302)

        session_cookie = client_one.get_cookie(app_one.config["SESSION_COOKIE_NAME"])
        self.assertIsNotNone(session_cookie)

        app_two = create_app()
        app_two.testing = True
        client_two = app_two.test_client()
        client_two.set_cookie(
            key=session_cookie.key,
            value=session_cookie.value,
            domain=session_cookie.domain or "localhost",
            path=session_cookie.path or "/",
        )

        response = client_two.get("/", follow_redirects=False)

        self.assertEqual(response.status_code, 200)

    def test_campaign_run_redirects_with_task_for_detail_polling(self):
        app = create_app()
        app.testing = True
        client = app.test_client()
        campaign_id = database.insert_campaign("Live progress", ["agency"], ["USA"], ["Seattle"])

        with patch("web.routes.campaigns.tasks.run_in_background") as run_in_background:
            response = client.post(f"/campaigns/{campaign_id}/run", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        location = response.headers["Location"]
        self.assertIn(f"/campaigns/{campaign_id}", location)
        self.assertIn("campaign_task=", location)
        run_in_background.assert_called_once()

    def test_running_task_survives_fast_app_restart(self):
        now = datetime.now().isoformat()
        database.upsert_task(
            task_id="fresh-task",
            task_type="campaign",
            campaign_id=1,
            status="running",
            progress=40,
            total=100,
            message="Crawling...",
            started_at=now,
            updated_at=now,
        )

        tasks._tasks.clear()
        tasks.init_tasks()

        task = tasks.get_task("fresh-task")
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "running")
        self.assertEqual(task.error, "")

    def test_stale_running_task_fails_after_heartbeat_window(self):
        old = (datetime.now() - timedelta(seconds=tasks.STALE_TASK_SECONDS + 60)).isoformat()
        database.upsert_task(
            task_id="stale-task",
            task_type="campaign",
            campaign_id=1,
            status="running",
            progress=40,
            total=100,
            message="Crawling...",
            started_at=old,
            updated_at=old,
        )

        tasks._tasks.clear()
        tasks.init_tasks()

        task = tasks.get_task("stale-task")
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.error, "Server restarted during task")

    def test_heartbeat_keeps_long_task_alive(self):
        task_id = tasks.create_task("campaign", campaign_id=1)
        before = tasks.get_task(task_id).updated_at

        with patch("tasks._now_iso", return_value=(datetime.now() + timedelta(seconds=30)).isoformat()):
            tasks.heartbeat_task(task_id)

        task = tasks.get_task(task_id)
        self.assertEqual(task.status, "running")
        self.assertGreater(task.updated_at, before)

    def test_completed_task_reports_full_progress(self):
        task_id = tasks.create_task("campaign", campaign_id=1)
        tasks.update_task(task_id, progress=75, total=100, message="Almost done")
        tasks.complete_task(task_id, "Done")

        task = tasks.get_task(task_id)
        self.assertEqual(task.status, "completed")
        self.assertEqual(task.progress, 100)
        self.assertEqual(task.to_dict()["percent"], 100)

    def test_campaign_is_queued_when_running_limit_is_full(self):
        config.save_settings({"max_running_campaigns": 1})
        active_id = database.insert_campaign("Active", ["agency"], ["USA"], ["Seattle"])
        queued_id = database.insert_campaign("Queued", ["plumber"], ["USA"], ["Seattle"])
        database.update_campaign_status(active_id, "generating")

        task_id, started = campaign_queue.enqueue_campaign(queued_id)

        campaign = database.get_campaign(queued_id)
        task = tasks.get_task(task_id)
        self.assertFalse(started)
        self.assertEqual(campaign["status"], "queued")
        self.assertEqual(task.status, "queued")

    def test_queued_campaign_starts_when_slot_opens(self):
        config.save_settings({"max_running_campaigns": 1})
        active_id = database.insert_campaign("Active", ["agency"], ["USA"], ["Seattle"])
        queued_id = database.insert_campaign("Queued", ["plumber"], ["USA"], ["Seattle"])
        database.update_campaign_status(active_id, "generating")
        task_id, started = campaign_queue.enqueue_campaign(queued_id)
        self.assertFalse(started)

        database.update_campaign_status(active_id, "done")
        with patch("campaign_queue.tasks.run_in_background") as run_in_background:
            campaign_queue.start_queued_campaigns()

        campaign = database.get_campaign(queued_id)
        task = tasks.get_task(task_id)
        self.assertEqual(campaign["status"], "generating")
        self.assertEqual(task.status, "running")
        run_in_background.assert_called_once()

    def test_smtp_probe_tries_multiple_hosts_before_reporting_unavailable(self):
        attempts = []

        class _ConnectionStub:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_create_connection(address, timeout=None):
            attempts.append((address, timeout))
            if address[0] == "mx1.example.com":
                raise OSError("first host failed")
            return _ConnectionStub()

        with patch("verification.verifier.socket.create_connection", side_effect=fake_create_connection):
            reachable = verifier._probe_smtp_connectivity(("mx1.example.com", "mx2.example.com"), timeout=3.0)

        self.assertTrue(reachable)
        self.assertEqual(
            attempts,
            [(("mx1.example.com", 25), 3.0), (("mx2.example.com", 25), 3.0)],
        )


if __name__ == "__main__":
    unittest.main()
