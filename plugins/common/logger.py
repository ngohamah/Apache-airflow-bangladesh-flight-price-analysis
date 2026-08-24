"""Shared logging setup: console output plus a rotating log file.

Every task module should call get_logger(__name__) rather than configuring
logging itself, so all task output lands in the same log file for traceability.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from plugins.common.config import LOG_DIR, LOG_FILE

_CONFIGURED_LOGGERS: set[str] = set()

_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per file
_BACKUP_COUNT = 5


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with console + rotating file handlers.

    Idempotent: repeated calls for the same name reuse the existing handlers
    instead of attaching duplicates.
    """
    logger = logging.getLogger(name)
    if name in _CONFIGURED_LOGGERS:
        return logger

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    _CONFIGURED_LOGGERS.add(name)
    return logger
