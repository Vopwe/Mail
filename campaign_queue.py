"""
Campaign queue coordinator.

Keeps campaign concurrency bounded so long jobs do not overload the VPS.
"""
import logging
import threading

import config
import database
import tasks
from web.routes._campaign_runner import run_campaign

logger = logging.getLogger(__name__)

_queue_lock = threading.Lock()
ACTIVE_STATUSES = ("generating", "crawling")


def _max_running_campaigns() -> int:
    try:
        value = int(config.get_setting("max_running_campaigns", config.MAX_RUNNING_CAMPAIGNS))
    except (TypeError, ValueError):
        value = config.MAX_RUNNING_CAMPAIGNS
    return max(1, min(value, 10))


def _active_campaign_count() -> int:
    return sum(len(database.get_campaigns(status=status)) for status in ACTIVE_STATUSES)


def _queued_campaigns() -> list[dict]:
    queued = database.get_campaigns(status="queued")
    return sorted(queued, key=lambda campaign: campaign.get("created_at", ""))


async def _run_and_drain_queue(task_id: str, campaign_id: int):
    try:
        await run_campaign(task_id, campaign_id)
    finally:
        start_queued_campaigns()


def _start_campaign_locked(campaign_id: int, task_id: str):
    database.update_campaign_status(campaign_id, "generating")
    tasks.update_task(
        task_id,
        status="running",
        progress=0,
        total=0,
        error="",
        message="Waiting for worker...",
    )
    tasks.run_in_background(_run_and_drain_queue, task_id, campaign_id)


def enqueue_campaign(campaign_id: int) -> tuple[str, bool]:
    """Queue or start a campaign. Returns (task_id, started_now)."""
    with _queue_lock:
        current = database.get_campaign(campaign_id)
        if current and current["status"] in ("queued", *ACTIVE_STATUSES):
            existing = tasks.find_latest_task(
                task_type="campaign",
                campaign_id=campaign_id,
                statuses=("queued", "running"),
            )
            if existing:
                return existing.task_id, current["status"] in ACTIVE_STATUSES

        task_id = tasks.create_task(
            task_type="campaign",
            campaign_id=campaign_id,
            status="queued",
            message="Queued. Waiting for an open campaign slot...",
        )

        if _active_campaign_count() < _max_running_campaigns():
            _start_campaign_locked(campaign_id, task_id)
            return task_id, True

        database.update_campaign_status(campaign_id, "queued")
        return task_id, False


def start_queued_campaigns():
    """Start queued campaigns until available slots are full."""
    with _queue_lock:
        available = _max_running_campaigns() - _active_campaign_count()
        if available <= 0:
            return

        for campaign in _queued_campaigns()[:available]:
            task = tasks.find_latest_task(
                task_type="campaign",
                campaign_id=campaign["id"],
                statuses=("queued", "running"),
            )
            if task is None:
                task_id = tasks.create_task(
                    task_type="campaign",
                    campaign_id=campaign["id"],
                    status="queued",
                    message="Queued. Waiting for an open campaign slot...",
                )
            else:
                task_id = task.task_id

            logger.info("Starting queued campaign %s with task %s", campaign["id"], task_id)
            _start_campaign_locked(campaign["id"], task_id)
