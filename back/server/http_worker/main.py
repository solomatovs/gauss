import asyncio

from server.helper.os import OsHelper
from server.helper.config import ConfigHelper
from server.helper.logging import LoggingHelper
from server.http_worker.worker import (
    HttpConfig,
    HttpWorker,
)

async def simple_run() -> None:
    """Простой запуск HTTP воркера"""
    config = ConfigHelper.load(HttpConfig,
        config_files=ConfigHelper.typical_config_files(),
    )

    # настройка логирования
    LoggingHelper.basicConfig(config.log_level)

    logger = LoggingHelper.getLogger("main")

    worker = HttpWorker(config)

    exit_code = 0
    
    try:
        # Запускаем созданный сервер
        await worker.start_serving()
        
        # Ждем завершения
        await worker.wait_closed()

    except KeyboardInterrupt:
        logger.info("received interrupt signal")
        exit_code = 0
    except Exception as _:
        logger.exception("critical error")
        exit_code = 1
    finally:
        await worker.shutdown()

    OsHelper.exit(exit_code)

def main():
    """Главная функция для запуска HTTP воркера"""
    asyncio.run(simple_run())

if __name__ == "__main__":
    main()
