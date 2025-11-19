#!/usr/bin/env bash
# Local demo pipeline without Docker, uses asyncio queue
python3 - <<'PY'
import asyncio
import random
import signal
import time
from datetime import datetime
import os

import pandas as pd

PARQUET_DIR = "./data/parquet"
os.makedirs(PARQUET_DIR, exist_ok=True)

QUEUE = asyncio.Queue()
RUNNING = True

def handle_shutdown():
    global RUNNING
    RUNNING = False
    print("Shutdown requested... finishing remaining tasks.")

try:
    signal.signal(signal.SIGINT, lambda *_: handle_shutdown())
    signal.signal(signal.SIGTERM, lambda *_: handle_shutdown())
except Exception:
    pass

async def producer(queue: asyncio.Queue):
    symbols = ["AAPL", "MSFT", "GOOG"]
    print("Starting producer...")
    while RUNNING:
        tick = {
            "symbol": random.choice(symbols),
            "price": round(random.uniform(100, 500), 4),
            "size": random.randint(1, 1000),
            "exchange": "SIM",
            "ts": datetime.utcnow().isoformat() + "Z",
        }
        await queue.put(tick)
        await asyncio.sleep(0.02)
    print("Producer stopped.")

async def write_parquet(batch):
    df = pd.DataFrame(batch)
    df = df.rename(columns={"ts": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["price"] = pd.to_numeric(df["price"])
    df["size"] = pd.to_numeric(df["size"])
    df = df.sort_values("timestamp")
    df["return"] = df.groupby("symbol")["price"].pct_change().fillna(0.0)
    filename = f"local_{int(time.time() * 1000)}.parquet"
    path = os.path.join(PARQUET_DIR, filename)
    df.to_parquet(path)
    print(f"Wrote {len(df)} rows -> {path}")

async def ingest(queue: asyncio.Queue):
    print("Starting ingest...")
    batch = []
    while RUNNING or not queue.empty():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        batch.append(item)
        queue.task_done()
        if len(batch) >= 100:
            await write_parquet(batch)
            batch = []
    if batch:
        await write_parquet(batch)
    print("Ingest stopped.")

async def main():
    await asyncio.gather(producer(QUEUE), ingest(QUEUE))

print("Running local async pipeline — Ctrl-C to stop")
asyncio.run(main())
PY
