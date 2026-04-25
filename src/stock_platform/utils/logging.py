"""
Centralized logging setup.

Usage (from anywhere in the app):

    from stock_platform.utils.logging import get_logger
    log = get_logger(__name__)
    log.info("Starting ingestion for {}", "RELIANCE.NS")

Writes to:
- Console (colorized, INFO by default, DEBUG in development)
- logs/app.log          — rolling main log
- logs/data_quality.log — only DQ-tagged records
"""

from __future__ import annotations

import sys

from loguru import logger

from stock_platform.config import LOGS_DIR, get_settings

_CONFIGURED = False


def _ensure_dirs() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def _setup() -> None:
    """Configure loguru once per process."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    _ensure_dirs()
    settings = get_settings()

    logger.remove()  # drop default handler

    # Console
    logger.add(
        sys.stderr,
        level=settings.app_log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # Rolling app log
    logger.add(
        LOGS_DIR / "app.log",
        level="DEBUG",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=False,  # do not leak variable values into shipped logs
    )

    # Data-quality log — only records tagged with extra={"dq": True}
    logger.add(
        LOGS_DIR / "data_quality.log",
        level="INFO",
        rotation="10 MB",
        retention="90 days",
        enqueue=True,
        filter=lambda record: record["extra"].get("dq") is True,
    )

    logger.add(
        LOGS_DIR / "backtests.log",
        level="INFO",
        rotation="10 MB",
        retention="180 days",
        enqueue=True,
        filter=lambda record: record["extra"].get("backtest") is True,
    )

    _CONFIGURED = True
    logger.debug("Logger configured. Logs directory: {}", LOGS_DIR)


def get_logger(name: str | None = None):
    """Return a configured logger. `name` is conventionally __name__."""
    _setup()
    return logger.bind(component=name or "stock_platform")


def get_dq_logger(name: str | None = None):
    """Return a logger that writes to logs/data_quality.log."""
    _setup()
    return logger.bind(component=name or "stock_platform", dq=True)


def get_backtest_logger(name: str | None = None):
    """Return a logger that writes to logs/backtests.log."""
    _setup()
    return logger.bind(component=name or "stock_platform", backtest=True)
