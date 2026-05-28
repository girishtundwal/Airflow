# 10 — Airflow Sensors & Event-driven Pipelines

## What are Sensors?

Sensors are special operators that **wait for a condition to become True** before allowing downstream tasks to run. They repeatedly check (poll) the condition at a configurable interval.

All sensors inherit from `BaseSensorOperator` and implement the `poke()` method which returns `True` (condition met) or `False` (not yet).

---

## Sensor Modes

### poke Mode (default)
```
Worker slot OCCUPIED while sensor polls
t=0    poke → False  (waiting)
t=30   poke → False  (waiting)   ← worker slot held the entire time
t=60   poke → False  (waiting)
t=90   poke → True   (done!)
```

```python
sensor = FileSensor(
    task_id="wait_for_file",
    filepath="/data/input.csv",
    mode="poke",           # default
    poke_interval=30,      # check every 30 seconds
    timeout=3600,          # fail after 1 hour
)
```

### reschedule Mode (recommended for long waits)
```
Worker slot RELEASED between checks
t=0    poke → False  → release worker → reschedule in 30s
t=30   poke → False  → release worker → reschedule in 30s
t=60   poke → True   → task succeeds
```

```python
sensor = FileSensor(
    task_id="wait_for_file",
    filepath="/data/input.csv",
    mode="reschedule",     # free worker slot between checks
    poke_interval=60,
    timeout=7200,
)
```

**Rule of thumb**: Use `poke` for short waits (< 5 minutes), `reschedule` for longer waits.

---

## Deferrable Sensors (Airflow 2.2+)

Deferrable sensors are the most resource-efficient — they use the **Triggerer** component and don't use any worker slot while waiting.

```
Normal sensor (poke/reschedule): uses worker slot (even if just sleeping)
Deferrable sensor:
  1. Task starts on worker
  2. Task defers → worker slot FREED, trigger registered in DB
  3. Triggerer (asyncio) polls externally — zero worker usage
  4. Trigger fires → task resumes on a NEW worker slot
```

```python
# Use deferrable variant when available
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensorAsync  # deferrable

wait = S3KeySensorAsync(
    task_id="wait_for_s3",
    bucket_name="my-bucket",
    bucket_key="data/{{ ds }}/file.parquet",
    aws_conn_id="aws_default",
    poke_interval=60,
    timeout=3600,
)
```

---

## FileSensor

```python
from airflow.sensors.filesystem import FileSensor

wait_file = FileSensor(
    task_id="wait_for_input",
    filepath="/data/input_{{ ds_nodash }}.csv",  # Jinja-templated
    fs_conn_id="fs_default",    # connection of type "File System"
    poke_interval=30,
    timeout=3600,
    mode="reschedule",
    soft_fail=False,            # True = skip instead of fail on timeout
)
```

---

## S3KeySensor

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_s3 = S3KeySensor(
    task_id="wait_for_s3_file",
    bucket_name="my-data-bucket",
    bucket_key="raw/orders/{{ ds }}/data.parquet",
    wildcard_match=False,   # True = wildcard glob matching
    aws_conn_id="aws_default",
    poke_interval=60,
    timeout=7200,
    mode="reschedule",
)

# Wildcard example: wait for any file in a prefix
wait_any = S3KeySensor(
    task_id="wait_any_file",
    bucket_name="my-bucket",
    bucket_key="raw/orders/{{ ds }}/*.parquet",
    wildcard_match=True,
    mode="reschedule",
)
```

---

## ExternalTaskSensor

```python
from airflow.sensors.external_task import ExternalTaskSensor

# Wait for specific task in another DAG
wait = ExternalTaskSensor(
    task_id="wait_upstream",
    external_dag_id="upstream_pipeline",
    external_task_id="final_load_task",       # None = wait for full DAG
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    execution_date_fn=lambda dt: dt,          # same logical_date
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
    check_existence=True,                     # fail if upstream DAG doesn't exist
)

# Upstream hourly → this DAG is daily
# Need to wait for all 24 hourly runs
from datetime import timedelta

def get_hourly_dates(dt):
    return [dt + timedelta(hours=h) for h in range(24)]

wait_all_hours = ExternalTaskSensor(
    task_id="wait_all_hourly",
    external_dag_id="hourly_ingestion",
    external_task_id="load_task",
    execution_date_fn=get_hourly_dates,
    mode="reschedule",
)
```

---

## SqlSensor

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

# Wait until query returns non-null/non-empty result
wait_for_data = SqlSensor(
    task_id="wait_for_data",
    conn_id="my_postgres",
    sql="""
        SELECT COUNT(*) 
        FROM orders 
        WHERE order_date = '{{ ds }}'
        HAVING COUNT(*) > 0
    """,
    poke_interval=300,      # check every 5 minutes
    timeout=7200,
    mode="reschedule",
    success=lambda result: result and result[0][0] > 100,  # custom success condition
)
```

---

## HttpSensor

```python
from airflow.providers.http.sensors.http import HttpSensor

# Wait for HTTP endpoint to return success
wait_api = HttpSensor(
    task_id="wait_for_api",
    http_conn_id="my_api",
    endpoint="api/v1/status",
    request_params={"date": "{{ ds }}"},
    response_check=lambda response: response.json()["status"] == "ready",
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
)
```

---

## Custom Sensor

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataReadySensor(BaseSensorOperator):
    """Wait until enough records exist in a table for a given date."""
    
    template_fields = ["table", "date_col", "target_date"]
    
    def __init__(
        self,
        table: str,
        date_col: str,
        target_date: str,
        min_records: int = 1000,
        postgres_conn_id: str = "postgres_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.date_col = date_col
        self.target_date = target_date
        self.min_records = min_records
        self.postgres_conn_id = postgres_conn_id
    
    def poke(self, context) -> bool:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = f"SELECT COUNT(*) FROM {self.table} WHERE {self.date_col} = '{self.target_date}'"
        count = hook.get_first(sql)[0]
        
        self.log.info(f"Found {count} records, need {self.min_records}")
        return count >= self.min_records


# Usage
wait = DataReadySensor(
    task_id="wait_for_data",
    table="raw.orders",
    date_col="order_date",
    target_date="{{ ds }}",
    min_records=5000,
    mode="reschedule",
    poke_interval=300,
    timeout=7200,
)
```

---

## Sensor Optimization

| Scenario | Recommendation |
|----------|---------------|
| Waiting < 5 minutes | `mode="poke"`, poke_interval=10-30s |
| Waiting 5min - 1hr | `mode="reschedule"`, poke_interval=60s |
| Waiting > 1 hour | Deferrable sensor (if available) |
| Thousands of concurrent sensors | Deferrable sensors only |
| Sensor waits for another DAG | `ExternalTaskSensor(mode="reschedule")` |

---

## Common Sensor Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| Sensor holds worker for hours | `mode="poke"` with long wait | Switch to `mode="reschedule"` |
| All workers occupied by sensors | Many poke-mode sensors | Use reschedule or deferrable |
| Sensor runs forever | No `timeout` set | Always set `timeout` |
| Sensor fails despite condition being true | Race condition, wrong execution_date | Check `execution_date_fn` in ExternalTaskSensor |
| `soft_fail=True` unexpected behavior | Downstream tasks get `upstream_failed` | Use `trigger_rule="none_failed"` on downstream |

---

## Dataset-based Event-driven Pipelines (Airflow 2.4+)

The modern way to create event-driven workflows — no sensors needed:

```python
from airflow.datasets import Dataset
from airflow.decorators import dag, task
import pendulum

orders_dataset = Dataset("s3://my-bucket/orders/")
customers_dataset = Dataset("s3://my-bucket/customers/")

# Producer DAG
@dag(schedule_interval="@hourly", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def load_raw_data():
    
    @task(outlets=[orders_dataset])     # declares output dataset
    def load_orders():
        # load data...
        pass
    
    @task(outlets=[customers_dataset])
    def load_customers():
        # load data...
        pass
    
    load_orders()
    load_customers()

# Consumer DAG — triggered when BOTH datasets are updated
@dag(schedule=[orders_dataset, customers_dataset])
def transform_data():
    
    @task(inlets=[orders_dataset, customers_dataset])
    def join_and_transform():
        # process
        pass
    
    join_and_transform()

producer = load_raw_data()
consumer = transform_data()
```
