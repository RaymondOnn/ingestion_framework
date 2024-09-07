import os
import sys
from loguru import logger as lg

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)


def get_logger():
    """
    Create and configure a Logger instance from loguru.

    Returns:
        Logger: A Logger instance with the configured settings.
    """
    lg.remove()
    # Add a new handler that logs to sys.stderr
    lg.add(
        sys.stderr,
        # logger level
        level=os.getenv("LOG_LEVEL", "DEBUG"),
        # logger format
        format=logger_format,
        # Colorize the output
        colorize=True,
        # Do not serialize the log records
        serialize=False,
    )
    print(lg)
    return lg


# logger = get_logger()
