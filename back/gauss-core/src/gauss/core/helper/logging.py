import logging
import time


class CustomFormatter(logging.Formatter):
    """Форматтер с UTC временем, миллисекундами и lowercase уровня"""

    converter = time.gmtime  # всегда использовать UTC

    RESET = "\033[0m"  # обычный
    BOLD = "\033[1m"  # тусклый
    DIM = "\033[2m"  # жирный

    COLORS = {
        logging.DEBUG: "\033[37m",  # серый
        logging.INFO: "\033[36m",  # голубой
        logging.WARNING: "\033[33m",  # жёлтый
        logging.ERROR: "\033[31m",  # красный
        logging.CRITICAL: "\033[41m",  # белый текст на красном фоне
    }
    STYLE = {
        logging.DEBUG: DIM,  # тусклый
        logging.INFO: RESET,  # стандартный
        logging.WARNING: RESET,  # стандартный
        logging.ERROR: BOLD,  # жирный
        logging.CRITICAL: BOLD,  # жирный
    }

    def get_color_level(self, levelno: int):
        return self.COLORS.get(levelno, self.RESET)

    def get_style_level(self, levelno: int):
        return self.STYLE.get(levelno, self.RESET)

    def format(self, record: logging.LogRecord) -> str:
        color = self.get_color_level(record.levelno)
        style = self.get_style_level(record.levelno)
        # выравнивание по 8 символам
        level = record.levelname.lower()
        repeat = " " * (8 - len(level))
        record.levelname = f"{repeat}{style}{color}{level}{self.RESET}"

        return super().format(record)


class PrefixSuffixMessageFilter(logging.Filter):
    def __init__(self, prefix="", suffix=""):
        super().__init__()
        self.prefix = prefix
        self.suffix = suffix

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = f"{self.prefix}{record.msg}{self.suffix}"
        return True


class LoggingHelper:
    @staticmethod
    def _get_formatter():
        return CustomFormatter(
            fmt="%(asctime)s.%(msecs)03d %(levelname)s: [%(threadName)s %(name)s] %(message)s",
            # fmt="%(asctime)s.%(msecs)03d %(levelname)s: [%(threadName)s %(name)s %(pathname)s %(lineno)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    @staticmethod
    def basicConfig(level: str = "info"):
        numeric_level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)

        logging.basicConfig(level=numeric_level, encoding="utf-8", errors="ignore")

        # Применяем кастомный форматтер ко всем хэндлерам
        for handler in logging.getLogger().handlers:
            handler.setFormatter(LoggingHelper._get_formatter())

    @staticmethod
    def getLogger(name: str, prefix: str = "", suffix: str = ""):
        logger = logging.getLogger(name)

        if prefix or suffix:
            logger.addFilter(PrefixSuffixMessageFilter(prefix, suffix))

        return logger
