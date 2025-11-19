# **Real-Time-Low-Latency-Market-Data-Ingestion-and-Processing-System**

### **Components**

* **`src/producer.py`** — Async Kafka producer (simulated market ticks)

* **`src/ingest.py`** — Async Kafka consumer: normalization, feature engineering, Parquet writes, Redis updates, Prometheus metrics, FastAPI endpoints

* **`src/cache_query.py`** — Helpers to fetch latest ticks from Redis or fallback to the newest Parquet

* **`src/utils.py`** — Logging setup and Prometheus metrics

* **`requirements.txt`** — Python dependencies

* **`docker-compose.yml`** — Full development stack: Zookeeper, Kafka, Redis, Prometheus, Grafana, producer, ingest

* **`Dockerfile.producer` & `Dockerfile.ingest`** — Docker build instructions

* **`prometheus.yml`** — Prometheus scrape config

* **`grafana/dashboard.json`** — Example dashboard

---

## **Local Setup (without Docker)**

1. Ensure Python 3.10+ is installed:

`brew install python   # macOS`

2. Install dependencies:

`python3 -m pip install -r requirements.txt`

3. Run producer (requires a running Kafka):

`python3 src/producer.py`

4. Run ingest service (requires Kafka \+ Redis):

`python3 src/ingest.py`

5. Access:

* FastAPI: `http://localhost:8001/latest/AAPL`

* Prometheus metrics: `http://localhost:8000`

---

## **Docker Compose Setup (recommended)**

1. Build and start all services:

`docker compose build`  
`docker compose up -d`

2. Verify services:

* Kafka: `docker compose logs -f kafka`

* Producer: `docker compose logs -f producer`

* Ingest: `docker compose logs -f ingest`

* Prometheus: `http://localhost:9090`

* Grafana: `http://localhost:3000` (default login: admin/admin)

* Redis: `localhost:6379`

3. The producer feeds ticks into Kafka. The ingest service consumes them, updates Redis, writes Parquet files, and exposes metrics.

---

## **Notes & Fixes Applied**

* Fixed Kafka connection issues by using internal Docker network hostnames (`kafka:9092`)

* Explicitly created Kafka topic `market.ticks` to avoid auto-create errors

* Updated timestamps to be UTC-aware (`datetime.now(timezone.utc)`)

* Ensured ingest consumer waits for Kafka to be ready before consuming

* Exposed Prometheus metrics (`/metrics`) and FastAPI endpoints (`/latest/{symbol}`)

* Dockerfiles and docker-compose are fully configured for local dev testing

* Logs and metrics can be accessed via Grafana dashboards and Prometheus queries

---

## **Useful Commands**

* Tail logs:

`docker compose logs -f producer`  
`docker compose logs -f ingest`

* Execute Kafka CLI inside container:

`docker compose exec kafka bash`  
`kafka-topics --bootstrap-server kafka:9092 --list`

* Check Redis contents:

`docker compose exec redis redis-cli`  
`keys *`  
`hgetall sym:AAPL:latest`  
