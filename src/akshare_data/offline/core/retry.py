"""统一重试装饰器"""

import time
import logging
from functools import wraps
from typing import Callable, Optional, Tuple, Type

from akshare_data.offline.core.errors import RetryExhaustedError

logger = logging.getLogger("akshare_data")


class RetryConfig:
    """重试配置"""

    def __init__(
        self,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
    ):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        self.exceptions = exceptions


def retry(config: Optional[RetryConfig] = None):
    """重试装饰器"""
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = config.delay
            last_error = None

            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except config.exceptions as e:
                    last_error = e
                    if attempt == config.max_retries:
                        raise RetryExhaustedError(
                            f"Function {func.__name__} failed after {config.max_retries} retries",
                            last_error,
                        )
                    logger.warning(
                        f"Attempt {attempt + 1}/{config.max_retries} failed for {func.__name__}: {e}"
                    )
                    time.sleep(current_delay)
                    current_delay *= config.backoff

            raise last_error

        return wrapper

    return decorator
