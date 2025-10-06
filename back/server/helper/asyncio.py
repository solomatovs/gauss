import signal
import asyncio


class CancellableTaskGroup(asyncio.TaskGroup):
    def __init__(self, stop_event: asyncio.Event):
        super().__init__()
        self._stop_event = stop_event

    async def __aenter__(self):
        res = await super().__aenter__()
        self.create_task(self._cancel_task())
        return res

    async def _cancel_task(self):
        await self._stop_event.wait()
        self._abort()


class AsyncioHelper:
    @staticmethod
    def stop_signal():
        """Следит за сигналами или условиями завершения"""
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        # Реакция на Ctrl+C
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

        return stop_event

    @staticmethod
    def task_group(stop_signal: asyncio.Event):
        return CancellableTaskGroup(stop_signal)
