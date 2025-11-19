"""Producer: Simulated market tick generator -> Kafka (async)"""

import asyncio
import json
import os
import random
import time
import socket
from datetime import datetime

from aiokafka import AIOKafkaProducer
from utils import setup_logging, TICKS_PROCESSED

logger = setup_logging()

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "market.ticks")
SYMBOLS = os.environ.get("SYMBOLS", "AAPL,MSFT,GOOG,TSLA,EURUSD,BTCUSD").split(",")
TICK_RATE = float(os.environ.get("TICK_RATE", "0.05"))  # seconds per tick


def wait_for_kafka(host: str, port: int, timeout: int = 60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info(f"Kafka at {host}:{port} is ready!")
                return
        except OSError:
            logger.info("Waiting for Kafka...")
            time.sleep(2)
    raise RuntimeError(f"Kafka not available after {timeout}s")


def generate_tick() -> dict:
    symbol = random.choice(SYMBOLS)
    price = round(random.uniform(100, 2000), 4) if any(c.isalpha() for c in symbol) else round(random.uniform(1.0, 1.5), 6)
    tick = {
        "symbol": symbol,
        "price": price,
        "size": random.randint(1, 1000),
        "exchange": "SIM",
        "ts": datetime.utcnow().isoformat() + "Z",
    }
    return tick


async def produce():
    host, port = KAFKA_BOOTSTRAP.split(":")
    wait_for_kafka(host, int(port))

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type="gzip",
        request_timeout_ms=15000,
        retry_backoff_ms=200,
        acks="all",
    )

    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    await producer.start()
    logger.info(f"Producer started. Sending ticks to topic '{TOPIC}'")

    try:
        while True:
            tick = generate_tick()
            try:
                await producer.send_and_wait(TOPIC, json.dumps(tick).encode("utf-8"))
                TICKS_PROCESSED.inc()
                logger.debug(f"Produced: {tick}")
            except Exception as e:
                logger.error(f"Failed to send tick: {e}")
            await asyncio.sleep(TICK_RATE)
    except asyncio.CancelledError:
        logger.warning("Producer cancelled (shutdown).")
    finally:
        await producer.stop()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(produce())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down...")
