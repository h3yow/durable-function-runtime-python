"""退避与重试策略。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """固定/指数退避重试策略。

    attempt 从 1 起算；第 ``attempt`` 次失败后的等待时间为
    ``min(base_delay * multiplier**(attempt-1), max_delay)``。
    """

    max_attempts: int = 3
    base_delay: float = 0.05
    multiplier: float = 2.0
    max_delay: float = 2.0

    def delay_for(self, attempt: int) -> float:
        """返回第 attempt 次失败后应等待的秒数。"""
        if attempt < 1:
            raise ValueError("attempt 从 1 起算")
        delay = self.base_delay * (self.multiplier ** (attempt - 1))
        return min(delay, self.max_delay)

    def should_retry(self, attempt: int, *, retryable: bool) -> bool:
        """判断第 attempt 次失败后是否还应重试。

        只有可重试错误、且尚未达到最大尝试次数时才重试。
        attempt=1 表示第 1 次执行刚失败；max_attempts=3 表示最多执行 3 次
        （即失败后最多再重试 2 次）。
        """
        return retryable and attempt < self.max_attempts
