import asyncio
import signal


class CancellableTaskGroup(asyncio.TaskGroup):
    def __init__(self, stop_event: asyncio.Event, return_when: str):
        super().__init__()
        self._stop_event = stop_event
        self._return_when = return_when
        self._tasks: set[asyncio.Task] = set()
        self._waiter: asyncio.Task | None = None

    async def __aenter__(self):
        res = await super().__aenter__()
        self.create_task(self._cancel_task())
        return res

    def create_task(self, coro, *, name=None, context=None):
        """Создать задачу и отслеживать её"""
        task = super().create_task(coro, name=name, context=context)
        self._tasks.add(task)

        # Если включён режим FIRST_COMPLETED — следим за окончанием любой задачи
        if self._return_when == asyncio.FIRST_COMPLETED:
            if not self._waiter:
                self._waiter = asyncio.create_task(self._watch_first_completed())

        return task

    async def _cancel_task(self):
        """Завершить группу при срабатывании stop_event"""
        await self._stop_event.wait()
        self._abort()

    async def _watch_first_completed(self):
        """Следим, чтобы при завершении одной задачи остальные отменялись"""
        try:
            done, pending = await asyncio.wait(
                self._tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if pending:
                for p in pending:
                    p.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        except asyncio.CancelledError:
            pass


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
    def cancellable_task_group(
        stop_signal: asyncio.Event, return_when: str = asyncio.ALL_COMPLETED
    ):
        return CancellableTaskGroup(stop_signal, return_when)

    @staticmethod
    def run_sync(coro):
        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            # нет активного event loop
            return asyncio.run(coro)
        else:
            # есть активный event loop → выполняем в отдельном потоке с новым loop
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
