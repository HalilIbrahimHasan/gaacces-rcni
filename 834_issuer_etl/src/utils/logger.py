"""
Logging utilities for the 834 Issuer ETL framework.

Provides a consistent logger factory so every module writes structured,
timestamped messages that aid debugging and audit trails.
"""

import logging
import sys

from config import LOG_FORMAT, LOG_LEVEL


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A logger with console handler and shared format/level from config.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        logger.propagate = False
    return logger
