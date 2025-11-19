"""Ingest: Kafka consumer -> normalization -> feature engineering -> Parquet + Redis + Prometheus"""

import os
import asyncio
import json
from datetime import datetime
import signal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redis
from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server
from fastapi import FastAPI

from utils import setup_logging, TICKS_PROCESSED, PARQUET_WRITES, INGEST_LATENCY

# Setup directories
os.makedirs("./logs", exist_ok=True)
PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)

logger = setup_logging(log_file="./logs/ingest.log")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "market.ticks")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

app = FastAPI(title="Market Ingest")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/latest/{symbol}")
def latest(symbol: str):
    key = f"sym:{symbol}:latest"
    return redis_client.hgetall(key)


async def process_batch(batch):
    df = pd.DataFrame(batch)
    df = df.rename(columns={"ts": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["price"] = pd.to_numeric(df["price"])
    df["size"] = pd.to_numeric(df["size"])
    df = df.sort_values("timestamp")
    df["return"] = df.groupby("symbol")["price"].pct_change().fillna(0.0)
    df["price_ema_5"] = df.groupby("symbol")["price"].transform(lambda s: s.ewm(span=5, adjust=False).mean())

    # Write Parquet
    date_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = f"{PARQUET_DIR}/ticks_{date_str}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    PARQUET_WRITES.inc()

    # Update Redis
    pipe = redis_client.pipeline()
    for _, row in df.groupby("symbol").tail(1).iterrows():
        key = f"sym:{row['symbol']}:latest"
        pipe.hset(key, mapping={
            "symbol": row["symbol"],
            "price": float(row["price"]),
            "size": int(row["size"]),
            "timestamp": row["timestamp"].isoformat(),
            "return": float(row["return"]),
            "price_ema_5": float(row["price_ema_5"])
        })
    pipe.execute()
    return path


async def consume(stop_event: asyncio.Event):
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="ingest-group",
        auto_offset_reset="latest"
    )
    await consumer.start()
    logger.info("Consumer started")
    batch = []

    try:
        async for msg in consumer:
            if stop_event.is_set():
                break
            TICKS_PROCESSED.inc()
            try:
                raw = json.loads(msg.value.decode("utf-8"))
            except Exception as e:
                logger.error(f"Failed to decode message: {e}")
                continue

            normalized = {
                "symbol": raw.get("symbol"),
                "price": raw.get("price"),
                "size": raw.get("size"),
                "exchange": raw.get("exchange"),
                "timestamp": raw.get("ts") or raw.get("timestamp")
            }
            batch.append(normalized)

            if len(batch) >= BATCH_SIZE:
                try:
                    with INGEST_LATENCY.time():
                        path = await process_batch(batch)
                        logger.info(f"Wrote {path} ({len(batch)} rows)")
                    batch = []
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")

    finally:
        await consumer.stop()
        logger.info("Consumer stopped")


# --- FastAPI startup hook to run consumer ---
@app.on_event("startup")
async def start_consumer():
    stop_event = asyncio.Event()
    asyncio.create_task(consume(stop_event))


if __name__ == "__main__":
    start_http_server(8000)  # Prometheus metrics
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
