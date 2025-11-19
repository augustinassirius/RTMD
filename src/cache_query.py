"""Helpers to query Redis for latest ticks or fall back to the newest Parquet batch."""

import os
import glob
from typing import Optional, Dict, Any

import pandas as pd
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PARQUET_DIR_DEFAULT = os.environ.get("PARQUET_DIR", "./data/parquet")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True,
)

def get_latest_from_redis(symbol: str) -> Dict[str, Any]:
    key = f"sym:{symbol}:latest"
    data = redis_client.hgetall(key)
    return data or {}

def get_latest_from_parquet(parquet_dir: str = PARQUET_DIR_DEFAULT, limit: int = 50) -> Optional[pd.DataFrame]:
    files = sorted(
        glob.glob(f"{parquet_dir}/*.parquet"),
        key=os.path.getmtime,
        reverse=True,
    )
    if not files:
        return None
    try:
        df = pd.read_parquet(files[0])
        return df.tail(limit)
    except Exception as e:
        print(f"Failed to read Parquet file: {e}")
        return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2:
        symbol = sys.argv[1]
        print(get_latest_from_redis(symbol))
    else:
        df = get_latest_from_parquet()
        if df is None:
            print("No Parquet files found.")
        else:
            print(df)
