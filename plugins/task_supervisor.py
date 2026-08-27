"""Application-wide ownership, conflict control, and shutdown for tasks."""

import asyncio
import logging
from dataclasses import dataclass


logger = logging.getLogger(__name__)


class TaskConflict(RuntimeError):
    pass


@dataclass
class _TaskEntry:
    task: asyncio.Task
    owner: str
    resources: frozenset[str]
    drain_on_shutdown: bool


class TaskSupervisor:
    def __init__(self):
        self._tasks: dict[str, _TaskEntry] = {}
        self._resources: dict[str, str] = {}
        self._accepting = True

    def start_accepting(self):
        self._accepting = True

    def stop_accepting(self):
        self._accepting = False

    def spawn(self, coroutine, *, key: str, owner: str,
              resources=(), drain_on_shutdown=False) -> asyncio.Task:
        resource_set = frozenset(str(resource) for resource in resources)
        conflict = self._tasks.get(key)
        blocked_resource = next(
            (resource for resource in resource_set if resource in self._resources),
            None,
        )
        if not self._accepting or (conflict and not conflict.task.done()) or blocked_resource:
            coroutine.close()
            if not self._accepting:
                raise TaskConflict("Application is shutting down; new tasks are disabled")
            if blocked_resource:
                holder = self._resources[blocked_resource]
                raise TaskConflict(
                    f"Resource {blocked_resource} is already owned by task {holder}"
                )
            raise TaskConflict(f"Task {key} is already running")

        task = asyncio.create_task(coroutine, name=key)
        entry = _TaskEntry(task, owner, resource_set, bool(drain_on_shutdown))
        self._tasks[key] = entry
        for resource in resource_set:
            self._resources[resource] = key
        task.add_done_callback(lambda completed, task_key=key: self._done(task_key, completed))
        return task

    def _done(self, key: str, task: asyncio.Task):
        entry = self._tasks.get(key)
        if entry is None or entry.task is not task:
            return
        self._tasks.pop(key, None)
        for resource in entry.resources:
            if self._resources.get(resource) == key:
                self._resources.pop(resource, None)
        if task.cancelled():
            logger.info("task_cancelled key=%s owner=%s", key, entry.owner)
            return
        exception = task.exception()
        if exception is not None:
            logger.error(
                "task_failed key=%s owner=%s error_type=%s error=%s",
                key,
                entry.owner,
                type(exception).__name__,
                exception,
            )

    def snapshot(self) -> dict:
        return {
            key: {
                "owner": entry.owner,
                "resources": sorted(entry.resources),
                "drain_on_shutdown": entry.drain_on_shutdown,
            }
            for key, entry in self._tasks.items()
            if not entry.task.done()
        }

    async def shutdown(self, drain_timeout=10.0, cancel_timeout=10.0):
        """Stop intake, briefly drain finite work, then cancel and await all."""
        self.stop_accepting()
        drain_tasks = [
            entry.task for entry in self._tasks.values()
            if entry.drain_on_shutdown and not entry.task.done()
        ]
        if drain_tasks:
            _done, pending = await asyncio.wait(
                drain_tasks, timeout=max(0.0, float(drain_timeout))
            )
            if pending:
                logger.warning(
                    "task_drain_deadline pending=%s", len(pending)
                )

        remaining = [
            entry.task for entry in self._tasks.values() if not entry.task.done()
        ]
        for task in remaining:
            task.cancel()
        if remaining:
            done, pending = await asyncio.wait(
                remaining, timeout=max(0.0, float(cancel_timeout))
            )
            if pending:
                logger.error("task_cancel_deadline pending=%s", len(pending))
            if done:
                await asyncio.gather(*done, return_exceptions=True)


supervisor = TaskSupervisor()
