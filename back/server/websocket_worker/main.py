import asyncio

from server.helper.os import OsHelper
from server.helper.config import ConfigHelper
from server.helper.logging import LoggingHelper
from server.websocket_worker.worker import (
    WebSocketWorker,
    WebsocketConfig,
)


async def simple_run() -> None:
    """Простой запуск WebSocket воркера"""
    config = ConfigHelper.load(WebsocketConfig,
        config_files=ConfigHelper.typical_config_files(),
    )

    # настройка логирования
    LoggingHelper.basicConfig(config.log_level)

    logger = LoggingHelper.getLogger("main")

    worker = WebSocketWorker.create(config)

    exit_code = 0
    
    try:
        # Используем контекстный менеджер для автоматической очистки
        async with WebSocketWorker.create(config) as worker:
            await worker.start()
    except KeyboardInterrupt:
        logger.info("received interrupt signal")
        exit_code = 0
    except Exception as _:
        logger.exception("critical error")
        exit_code = 1

    OsHelper.exit(exit_code)

def main() -> None:
    """Главная функция для запуска WebSocket воркера"""
    asyncio.run(simple_run())

if __name__ == "__main__":
    main()
