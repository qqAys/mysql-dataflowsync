import asyncio

import persistqueue
import tailer
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.websockets import WebSocket

from utils import Path, QUEUE_PATH, LOG_PATH

app = FastAPI(docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory="monitor/static"), name="static")


@app.get("/cdc")
async def root():
    return FileResponse(path="monitor/cdc_index.html", status_code=200)


@app.get("/dpu")
async def root():
    return FileResponse(path="monitor/dpu_index.html", status_code=200)


@app.websocket("/ws/cdc/queue-length")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_bytes(b"")
    try:
        while True:
            q = persistqueue.SQLiteQueue(QUEUE_PATH, auto_commit=True)
            await websocket.send_text(str(q.qsize()))
            await asyncio.sleep(1)
    except Exception as e:
        print(e)
        pass


@app.websocket("/ws/cdc/print-log")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_bytes(b"")
    try:
        with open(Path(LOG_PATH, "cdc.log"), "r") as f:
            for line in tailer.follow(f):
                await websocket.send_text(line)
                await asyncio.sleep(0.1)
    except Exception as e:
        print(e)
        pass


@app.websocket("/ws/dpu/queue-length")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_bytes(b"")
    try:
        while True:
            q = persistqueue.SQLiteQueue(QUEUE_PATH, auto_commit=True)
            await websocket.send_text(str(q.qsize()))
            await asyncio.sleep(1)
    except Exception as e:
        print(e)
        pass


@app.websocket("/ws/dpu/print-log")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_bytes(b"")
    try:
        with open(Path(LOG_PATH, "dpu.log"), "r") as f:
            for line in tailer.follow(f):
                await websocket.send_text(line)
                await asyncio.sleep(0.1)
    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    pass
