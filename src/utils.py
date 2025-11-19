"""Utility module for logging setup and Prometheus metrics."""

import logging
import sys
from prometheus_client import Counter, Histogram

def setup_logging(log_file: str | None = None, level: int = logging.INFO):
    logger = logging.getLogger("market_pipeline")
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

# Prometheus metrics
TICKS_PROCESSED = Counter(
    "ticks_processed_total",
    "Total number of raw ticks processed by the producer or ingest stage",
)

PARQUET_WRITES = Counter(
    "parquet_files_written_total",
    "Total number of parquet files successfully written",
)

INGEST_LATENCY = Histogram(
    "ingest_latency_seconds",
    "Latency distribution for processing and storing a batch of ticks (in seconds)",
)
