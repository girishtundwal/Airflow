# 14 — Airflow Performance Optimization

## Scheduler Performance

The scheduler is the most performance-sensitive component. Bottlenecks here cause task scheduling delays (scheduler lag).

```
Scheduler lag = time between when a task SHOULD start and when it ACTUALLY starts
Target: < 30 seconds for most workloads
```

### Key Scheduler Config

```ini
[scheduler]
scheduler_heartbeat_sec = 5         # main loop interval (lower = more responsive, more load)
min_file_process_interval = 30      # re-parse DAG files every 30s (increase to reduce load)
dag_dir_list_interval = 300         # scan dags/ folder every 5min (can increase for stable setups)
parsing_processes = 4               # parallel DAG file parsers (set to CPU count)
max_dagruns_to_create_per_loop = 10 # batching for high-volume DAG creation
max_callbacks_per_loop = 20         # SLA/failure callback processing
use_job_schedule = True
```

---

## DAG Parsing Optimization

DAG files are re-parsed every `min_file_process_interval` seconds. Slow parsing = delayed scheduling.

### Anti-patterns vs Best Practices

| Anti-pattern | Problem | Fix |
|-------------|---------|-----|
| `import pandas as pd` at top level | pandas imports on every parse | Move imports inside callables |
| `Variable.get("x")` at top level | DB query on every parse | Move inside task callables |
| DB queries in DAG body | Executes on every parse | Move to task callables |
| 1000+ files in dags/ folder | Parser overwhelmed | Split into subdirectories, increase `parsing_processes` |
| Large DAG factory that queries DB | DB hammered by scheduler | Cache results or use static config files |
| Complex class instantiation at import | Slow parse | Use lazy initialization |

```python
# BAD — runs on every DAG parse
import pandas as pd                    # slow import
config = Variable.get("config")        # DB query on parse
conn = psycopg2.connect(...)           # connection on parse

# GOOD — runs only when task executes
def my_task():
    import pandas as pd               # import inside callable
    config = Variable.get("config")   # inside callable
```

### Measuring Parse Time

```bash
# Check DAG import errors and parse times
airflow dags list-import-errors

# Time a specific DAG file parse
time python -c "from airflow.models import DagBag; DagBag('/opt/airflow/dags/my_dag.py')"

# Check parsing stats in UI: Admin → DAG File Processing
```

---

## Worker Optimization

```ini
[celery]
worker_concurrency = 16       # tasks per worker (CPU-bound: = num CPUs; IO-bound: 2-4x CPUs)
worker_prefetch_multiplier = 1 # fetch only 1 task at a time (better distribution)
acks_late = True              # acknowledge task only after completion (safer retries on crash)

[core]
parallelism = 32              # global max concurrent tasks
max_active_tasks_per_dag = 16 # per-DAG limit
```

```bash
# Scale Celery workers horizontally
# On each worker machine:
airflow celery worker --concurrency 16 --queues default,high_priority

# Check worker status
airflow celery flower    # open http://localhost:5555
celery -A airflow.executors.celery_executor.app inspect active
```

---

## Parallelism Tuning

```
Global parallelism hierarchy:
1. parallelism (global ceiling, all tasks)
2. max_active_tasks_per_dag (per-DAG ceiling)
3. pool slots (resource-based ceiling)
4. max_active_runs_per_dag (concurrent DAG runs)
5. Worker concurrency (per-worker ceiling)
```

```ini
[core]
parallelism = 32                   # total concurrent tasks (all DAGs combined)
max_active_tasks_per_dag = 16      # max concurrent tasks per DAG
max_active_runs_per_dag = 5        # max concurrent DAG runs per DAG
```

```python
# Override at DAG level
with DAG(
    dag_id="high_load_dag",
    max_active_tasks=32,           # allow more for this specific DAG
    max_active_runs=3,
):
    pass
```

---

## Pools

Pools limit the number of concurrently running tasks that access a shared resource.

```
Without pools:
  DB server gets 50 simultaneous connections → crashes

With pool "db_connections" (10 slots):
  Max 10 tasks query the DB at once, others wait
```

```bash
# Create pool via CLI
airflow pools set db_connections 10 "Database connection slots"
airflow pools set snowflake_load 5 "Snowflake loading slots"
airflow pools set api_calls 20 "External API rate limit"

# List pools
airflow pools list

# Get pool info
airflow pools get db_connections

# Delete pool
airflow pools delete old_pool
```

```python
# Assign task to a pool
heavy_query = PostgresOperator(
    task_id="heavy_query",
    pool="db_connections",         # will wait if pool is full
    pool_slots=2,                  # this task occupies 2 slots (for heavy tasks)
    postgres_conn_id="my_db",
    sql="SELECT * FROM huge_table",
)
```

---

## Queues

Route tasks to specific workers (Celery only):

```python
# Assign task to specific queue
gpu_task = PythonOperator(
    task_id="train_model",
    queue="gpu_workers",           # only workers consuming this queue will pick it up
    python_callable=train,
)

scraper = PythonOperator(
    task_id="scrape_data",
    queue="scraper_workers",
    python_callable=scrape,
)
```

```bash
# Start workers for specific queues
airflow celery worker --queues gpu_workers
airflow celery worker --queues scraper_workers,default
```

---

## Deferrable Operators

Switch to deferrable operators for I/O-bound waiting tasks to free worker slots:

```python
# OLD: blocks a worker slot while waiting 2 hours for S3 file
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait = S3KeySensor(
    task_id="wait",
    bucket_name="my-bucket",
    bucket_key="data/file.parquet",
    mode="reschedule",  # better, but still uses reschedule mechanism
)

# NEW: deferrable — zero worker slots while waiting
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensorAsync

wait = S3KeySensorAsync(
    task_id="wait",
    bucket_name="my-bucket",
    bucket_key="data/file.parquet",
    poke_interval=60,
)
```

**Impact**: With 100 long-running sensors:
- Reschedule mode: 0 permanent worker slots, but TaskInstance writes every `poke_interval`
- Deferrable: 0 worker slots, 1 asyncio coroutine in Triggerer (very cheap)

---

## Database Optimization

```ini
[database]
sql_alchemy_pool_size = 5           # base pool size
sql_alchemy_max_overflow = 10       # extra connections during spikes
sql_alchemy_pool_timeout = 30       # wait time for pool connection
sql_alchemy_pool_recycle = 1800     # recycle connections every 30 min
```

```bash
# Use PgBouncer for PostgreSQL connection pooling
# Without: each scheduler/webserver/worker opens its own connections
# With PgBouncer: all share a connection pool

# Regular cleanup (run as maintenance DAG)
airflow db clean --clean-before-timestamp "2024-01-01T00:00:00" --yes
```

---

## DAG Design Optimization

| Practice | Performance Impact |
|----------|------------------|
| Keep task count reasonable (< 100 per DAG) | Reduces scheduler complexity |
| Use TaskGroups for visual organization (not task splitting) | No perf impact, better UX |
| Use dynamic task mapping instead of 100 static tasks | Better scheduler scaling |
| Minimize XCom payload size | Reduce DB I/O |
| Use deferrable operators for waits | Free worker slots |
| Set appropriate `execution_timeout` | Prevent hung tasks from blocking slots |
| Use appropriate `pool` | Prevent resource contention |
| Set `depends_on_past=False` unless needed | Avoid pipeline stalls |
| Use `catchup=False` | Avoid backfill floods on new DAGs |

---

## Performance Benchmarking

```bash
# Airflow internal benchmark tool
airflow performance-checks

# Test DAG parsing time
time airflow dags list

# Check scheduler stats in UI
# Admin → DAG File Processing → shows parse time per file

# Monitor key metrics via StatsD/Prometheus:
# - airflow.scheduler.tasks.starving (tasks waiting for slots)
# - airflow.executor.open_slots (available slots)
# - airflow.dag_processing.total_parse_time (total parse time per cycle)
```

---

## Key Config Summary

| Config | Default | Recommended Prod |
|--------|---------|-----------------|
| `parallelism` | 32 | 32-256 (based on workers) |
| `max_active_tasks_per_dag` | 16 | 16-64 |
| `max_active_runs_per_dag` | 16 | 3-10 |
| `parsing_processes` | 2 | 4-8 |
| `min_file_process_interval` | 30 | 30-60s |
| `scheduler_heartbeat_sec` | 5 | 5s |
| `worker_concurrency` (Celery) | 16 | 8-32 (tune per workload) |
| `sql_alchemy_pool_size` | 5 | 5-20 (with PgBouncer) |
