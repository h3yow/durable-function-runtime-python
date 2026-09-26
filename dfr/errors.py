"""错误类型与可区分的拒绝/失败原因码。

所有跨层传递的业务异常都带一个稳定的 ``code`` 字符串，
调用方与测试可据此区分失败原因，而不是依赖异常文本匹配。
"""

from __future__ import annotations


class DFRError(Exception):
    """平台错误基类。"""

    code: str = "dfr_error"

    def __init__(self, message: str = "", *, code: str | None = None) -> None:
        super().__init__(message or self.__class__.__doc__ or self.code)
        if code is not None:
            self.code = code


class RetryableError(DFRError):
    """函数步骤抛出的可重试错误（例如临时网络故障）。"""

    code = "retryable_error"


class NonRetryableError(DFRError):
    """函数步骤抛出的不可重试错误，直接进入失败终态。"""

    code = "non_retryable_error"


class StepTimeoutError(RetryableError):
    """单步执行超时，归类为可重试错误。"""

    code = "step_timeout"


class CheckpointMissingError(DFRError):
    """恢复时找不到检查点，明确拒绝恢复。"""

    code = "checkpoint_missing"


class CheckpointVersionError(DFRError):
    """检查点版本与当前运行时不兼容，明确拒绝恢复。"""

    code = "checkpoint_version_incompatible"


class CheckpointCorruptError(DFRError):
    """检查点内容损坏/无法解析，明确拒绝恢复。"""

    code = "checkpoint_corrupt"


class QueueFullError(DFRError):
    """有界等待队列已满，调用被拒绝。"""

    code = "queue_full"


class ConcurrencyLimitRejectedError(DFRError):
    """并发槽位与队列均不可用，调用被有界拒绝。"""

    code = "concurrency_limit_rejected"


class QuotaExceededError(DFRError):
    """调用超出配额，被有界拒绝。"""

    code = "quota_exceeded"


class ExecutionFailedError(DFRError):
    """执行已进入失败终态（重试耗尽或不可重试错误）。"""

    code = "execution_failed"


class TimedOutFinalError(DFRError):
    """执行超过整体截止时间，进入超时终态。"""

    code = "execution_timed_out"
