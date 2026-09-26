"""Entry point: run the durable function runtime HTTP server."""
import uvicorn

from durable_runtime.api import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
