"""Provides backoff functionality."""
import time
from functools import wraps
from typing import Any

from log import logger


def backoff(func):
    """Provide backoff functionality."""

    @wraps(func)
    def inner(
        *args: Any, sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10, **kwargs: Any,
    ):
        while sleep_time < border_sleep_time:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.exception('Failed to execute func with backoff;', e, exc_info=True)
                time.sleep(sleep_time)
                sleep_time *= 2 ** factor
    return inner
