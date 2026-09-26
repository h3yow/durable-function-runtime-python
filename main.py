"""本地启动入口：uvicorn main:app。

启动时自动从 data/ 目录恢复未完成的执行；
控制台打印调用标识、状态迁移与重试记录（dfr.runtime logger）。
"""

import logging

from dfr.api import build_default_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = build_default_app(data_dir="data")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
