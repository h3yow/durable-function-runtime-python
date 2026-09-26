"""基于本地 JSON 文件的持久化存储（单进程，原子写）。"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from dfr.errors import CheckpointCorruptError


class JsonStore:
    """每个执行一个 JSON 文件，写入采用临时文件 + os.replace 原子替换。

    同进程内以一把全局 ``threading.Lock`` 串行化写；读取为只读打开。
    文件名由幂等键做 URL 转义得到，保证键的可逆映射与文件系统安全。
    """

    SUFFIX = ".json"

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def path_for(self, key: str) -> Path:
        return self.directory / f"{quote(key, safe='')}{self.SUFFIX}"

    def exists(self, key: str) -> bool:
        return self.path_for(key).exists()

    def write(self, key: str, data: dict[str, Any]) -> None:
        """原子写入：同目录临时文件 + os.replace，避免崩溃产生半截文件。"""
        target = self.path_for(key)
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        with self._lock:
            fd, tmp_name = tempfile.mkstemp(
                prefix=target.name + ".", suffix=".tmp", dir=self.directory
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_name, target)
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
                raise

    def read(self, key: str) -> dict[str, Any]:
        path = self.path_for(key)
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError as exc:
            raise CheckpointCorruptError(
                f"检查点文件无法解析: {path.name}: {exc}"
            ) from exc

    def iter_records(self) -> list[dict[str, Any]]:
        """读取目录下全部可解析的执行记录（恢复时使用）。

        无法解析的文件会被跳过（键名仍可经 :meth:`iter_keys` 枚举），
        以保证单条损坏不影响其他执行恢复。
        """
        return [data for _key, data in self.iter_keyed_records()]

    def iter_keys(self) -> list[str]:
        """枚举目录下所有幂等键（无论文件是否可解析）。"""
        return [
            self.key_from_filename(p.name)
            for p in sorted(self.directory.glob(f"*{self.SUFFIX}"))
        ]

    def iter_keyed_records(
        self,
    ) -> list[tuple[str, dict[str, Any]]]:
        """枚举 (幂等键, 记录)；损坏文件以 CheckpointCorruptError 抛出项表示。

        返回列表的第二项是 dict 或异常实例，由调用方逐条处理。
        """
        items: list[tuple[str, Any]] = []
        for path in sorted(self.directory.glob(f"*{self.SUFFIX}")):
            key = self.key_from_filename(path.name)
            try:
                with open(path, encoding="utf-8") as fh:
                    items.append((key, json.load(fh)))
            except json.JSONDecodeError as exc:
                items.append((key, CheckpointCorruptError(
                    f"检查点文件无法解析: {path.name}: {exc}"
                )))
        return items

    @staticmethod
    def key_from_filename(name: str) -> str:
        """文件名反解为幂等键（排障/日志用）。"""
        return unquote(name.removesuffix(JsonStore.SUFFIX))
