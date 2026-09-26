#!/usr/bin/env bash
# 一键本地验证：幂等/重试/失败终态/并发背压/重启恢复
set -euo pipefail
cd "$(dirname "$0")/.."
uv run python scripts/demo.py
