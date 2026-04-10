"""
Background task runner — threading + asyncio for single-user local app.
"""
import threading
import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TaskStatus:
    task_id: str
    task_type: str = ""
    status: str = "running"   # running | completed | failed
    progress: int = 0
    total: int = 0
    message: str = ""
    error: str = ""
    started_at: str = ""
    completed_at: str = ""

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status,
            "progress": self.progress,
            "total": self.total,
            "message": self.message,
            "error": self.error,
            "percent": round((self.progress / self.total * 100) if self.total > 0 else 0),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


# Global in-memory task registry
_tasks: dict[str, TaskStatus] = {}
_lock = threading.Lock()


def create_task(task_type: str = "") -> str:
    task_id = uuid.uuid4().hex[:12]
    with _lock:
        _tasks[task_id] = TaskStatus(
            task_id=task_id,
            task_type=task_type,
            started_at=datetime.now().isoformat(),
        )
    return task_id


def get_task(task_id: str) -> TaskStatus | None:
    return _tasks.get(task_id)


def update_task(task_id: str, **kwargs):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            for k, v in kwargs.items():
                setattr(task, k, v)


def complete_task(task_id: str, message: str = "Done"):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            task.status = "completed"
            task.message = message
            task.completed_at = datetime.now().isoformat()


def fail_task(task_id: str, error: str):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            task.status = "failed"
            task.error = error
            task.completed_at = datetime.now().isoformat()


def run_in_background(async_func, task_id: str, *args, **kwargs):
    """Run an async function in a background thread with its own event loop."""
    def wrapper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(async_func(task_id, *args, **kwargs))
        except Exception as e:
            fail_task(task_id, str(e))
        finally:
            try:
                import database
                database.close_db()
            except Exception:
                pass
            loop.close()

    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    return task_id


def get_all_tasks() -> list[dict]:
    with _lock:
        return [t.to_dict() for t in _tasks.values()]
