import logging
import asyncio

from server.balancer.l4 import (
    LoadBalancer,
    BalancerConfig,
)
from server.helper.os import OsHelper
from server.helper.config import ConfigHelper

# настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_simple() -> None:
    """Простой запуск HTTP воркера"""
    config = ConfigHelper.load(
        BalancerConfig,
        config_files=ConfigHelper.typical_config_files(),
    )

    worker = LoadBalancer(config)

    try:
        # Запускаем созданный сервер
        await worker.start_serving()

        # Ждем завершения
        await worker.wait_closed()

    except KeyboardInterrupt:
        # logger.info(f"Worker {worker.cx.worker_id}: Received interrupt signal")
        pass
    except Exception as e:
        # logger.error(f"Worker {worker.cx.worker_id}: Critical error: {str(e)}")
        pass
    finally:
        # await worker.shutdown()
        pass


def main():
    """Главная функция для запуска HTTP воркера"""

    try:
        asyncio.run(run_simple())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    except Exception as e:
        logger.error(f"Worker failed: {str(e)}")
        OsHelper.exit(1)


if __name__ == "__main__":
    asyncio.run(run_simple())
