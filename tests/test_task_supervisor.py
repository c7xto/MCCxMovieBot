import asyncio
import unittest
from pathlib import Path

from plugins.task_supervisor import TaskConflict, TaskSupervisor


ROOT = Path(__file__).resolve().parents[1]


class TaskSupervisorTests(unittest.TestCase):
    def test_resource_conflicts_and_completion_release_ownership(self):
        async def run():
            supervisor = TaskSupervisor()
            release = asyncio.Event()

            async def held():
                await release.wait()

            first = supervisor.spawn(
                held(), key="migration:1:2", owner="admin:1",
                resources=("cluster:1", "cluster:2"),
            )
            with self.assertRaisesRegex(TaskConflict, "cluster:2"):
                supervisor.spawn(
                    held(), key="migration:2:3", owner="admin:2",
                    resources=("cluster:2", "cluster:3"),
                )
            release.set()
            await first
            await asyncio.sleep(0)
            self.assertEqual(supervisor.snapshot(), {})

        asyncio.run(run())

    def test_shutdown_drains_finite_work_then_cancels_worker(self):
        async def run():
            supervisor = TaskSupervisor()
            finite_done = asyncio.Event()
            worker_cancelled = asyncio.Event()

            async def finite():
                await asyncio.sleep(0)
                finite_done.set()

            async def worker():
                try:
                    await asyncio.Event().wait()
                finally:
                    worker_cancelled.set()

            supervisor.spawn(
                finite(), key="finite", owner="test", drain_on_shutdown=True
            )
            supervisor.spawn(worker(), key="worker", owner="test")
            await supervisor.shutdown(drain_timeout=1, cancel_timeout=1)
            self.assertTrue(finite_done.is_set())
            self.assertTrue(worker_cancelled.is_set())
            with self.assertRaisesRegex(TaskConflict, "shutting down"):
                supervisor.spawn(finite(), key="late", owner="test")

        asyncio.run(run())

    def test_all_application_task_creation_routes_through_supervisor(self):
        offenders = []
        for path in [ROOT / "bot.py", *(ROOT / "plugins").glob("*.py")]:
            if path.name == "task_supervisor.py":
                continue
            source = path.read_text(encoding="utf-8")
            if "asyncio.create_task(" in source:
                offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(offenders, [])
        bot_source = (ROOT / "bot.py").read_text(encoding="utf-8")
        self.assertIn("await supervisor.shutdown", bot_source)


if __name__ == "__main__":
    unittest.main()
