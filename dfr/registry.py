"""函数注册表与 DurableFunction 定义。

一个函数被建模为**有序的步骤列表**。运行时逐步骤执行，每完成一步就做一次
检查点；恢复时已完成的步骤不会被再次调用。步骤函数抛出
``RetryableError`` / ``NonRetryableError`` 以表达可重试/不可重试语义。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from dfr.errors import DFRError

StepFn = Callable[["StepContext"], Any]


@dataclass
class StepContext:
    """传给每个步骤的上下文：调用入参与上一步结果。"""

    payload: Any = None
    previous: Any = None


class DurableFunction(Protocol):
    """可注册函数协议：返回带名字的有序步骤列表。"""

    name: str

    def steps(self) -> list[StepFn]:
        ...


@dataclass
class StepsFunction:
    """最常用的函数定义：一个名字 + 一组有序步骤。"""

    name: str
    _steps: list[StepFn]

    def steps(self) -> list[StepFn]:
        return list(self._steps)


class FunctionNotFoundError(DFRError):
    """引用了未注册的函数名。"""

    code = "function_not_found"


class FunctionAlreadyRegisteredError(DFRError):
    """同名函数重复注册。"""

    code = "function_already_registered"


class FunctionRegistry:
    """按名字注册/查找函数定义。"""

    def __init__(self) -> None:
        self._functions: dict[str, DurableFunction] = {}

    def register(self, fn: DurableFunction, *, overwrite: bool = False) -> DurableFunction:
        existing = self._functions.get(fn.name)
        if existing is not None and not overwrite:
            raise FunctionAlreadyRegisteredError(fn.name)
        self._functions[fn.name] = fn
        return fn

    def define(self, name: str, steps: list[StepFn]) -> StepsFunction:
        """便捷注册：直接给定名字与步骤列表。"""
        fn = StepsFunction(name=name, _steps=list(steps))
        return self.register(fn)  # type: ignore[return-value]

    def get(self, name: str) -> DurableFunction:
        try:
            return self._functions[name]
        except KeyError:
            raise FunctionNotFoundError(name) from None

    def contains(self, name: str) -> bool:
        return name in self._functions

    def names(self) -> list[str]:
        return sorted(self._functions)
