# 09 — Airflow XCom & Data Passing

## What is XCom?

**XCom** (Cross-Communication) allows tasks to exchange small amounts of metadata. Values are stored in the `xcom` table in the metadata database.

**Key constraint**: XCom is for **small metadata** (task IDs, record counts, status flags, file paths), NOT for large data (DataFrames, large files).

Default size limit: depends on DB column type — effectively keep under **48KB** for safe cross-DB compatibility.

---

## XCom Architecture

```
Task A executes
     │
     │  xcom_push(key="result", value={"count": 1000})
     ▼
┌─────────────────────────────────────────────────┐
│  xcom table (Metadata DB)                       │
│  dag_id: my_dag                                 │
│  task_id: task_a                                │
│  run_id: scheduled__2024-01-15                  │
│  key: result                                    │
│  value: {"count": 1000}  (JSON)                 │
│  timestamp: 2024-01-15T06:00:00                 │
└─────────────────────────────────────────────────┘
     │
     │  xcom_pull(task_ids="task_a", key="result")
     ▼
Task B executes with {"count": 1000}
```

---

## Traditional push() and pull()

```python
from airflow.operators.python import PythonOperator

def push_data(**context):
    ti = context["ti"]
    # Push with default key "return_value"
    ti.xcom_push(key="return_value", value={"count": 500, "status": "ok"})
    # Push with custom key
    ti.xcom_push(key="file_path", value="s3://bucket/data/2024-01-15.parquet")

def pull_data(**context):
    ti = context["ti"]
    # Pull with default key
    result = ti.xcom_pull(task_ids="push_task")
    # Pull with specific key
    file_path = ti.xcom_pull(task_ids="push_task", key="file_path")
    # Pull from multiple tasks
    results = ti.xcom_pull(task_ids=["task_a", "task_b"])  # returns list
    
    print(f"Count: {result['count']}, File: {file_path}")

push_task = PythonOperator(task_id="push_task", python_callable=push_data)
pull_task = PythonOperator(task_id="pull_task", python_callable=pull_data)

push_task >> pull_task
```

---

## Automatic XComs

### PythonOperator return value

```python
def extract_count() -> int:
    return 1000  # automatically pushed as key="return_value"

def use_count(**context):
    count = context["ti"].xcom_pull(task_ids="extract")
    print(f"Got {count} records")

extract = PythonOperator(task_id="extract", python_callable=extract_count)
```

### BashOperator stdout

```python
from airflow.operators.bash import BashOperator

count_task = BashOperator(
    task_id="count_lines",
    bash_command="wc -l < /data/file.csv",
    # Last stdout line pushed as XCom return_value
)

def use_count(**context):
    count = context["ti"].xcom_pull(task_ids="count_lines")
    print(f"Line count: {count}")
```

---

## TaskFlow API: Automatic XCom

The cleanest pattern — no explicit push/pull:

```python
from airflow.decorators import dag, task
import pendulum

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def xcom_taskflow():
    
    @task
    def step1() -> dict:
        return {"count": 500, "source": "api"}
    
    @task
    def step2(data: dict) -> str:
        # data is automatically pulled from step1's XCom
        return f"Processed {data['count']} records from {data['source']}"
    
    @task
    def step3(summary: str):
        print(summary)
    
    result = step1()
    msg = step2(result)    # result is passed as XCom
    step3(msg)

pipeline = xcom_taskflow()
```

### Multiple outputs (TaskFlow)

```python
@task(multiple_outputs=True)
def get_config() -> dict:
    return {
        "host": "prod.db.com",
        "port": 5432,
        "db": "analytics"
    }
    # Creates THREE separate XCom keys: "host", "port", "db"
    # Access as config["host"], config["port"], etc.

@dag(...)
def pipeline():
    config = get_config()
    process(config["host"], config["port"])
```

---

## XCom in Jinja Templates

```python
PostgresOperator(
    task_id="use_xcom",
    postgres_conn_id="my_db",
    sql="""
        INSERT INTO results
        SELECT * FROM raw_data
        WHERE batch_id = {{ ti.xcom_pull(task_ids='get_batch_id') }}
    """,
)
```

---

## Custom XCom Backends

For large data, store the actual data externally and use XCom to store the **reference** (URI).

### Built-in S3 XCom Backend

```python
# In requirements.txt / providers
# apache-airflow-providers-amazon

# airflow.cfg
[core]
xcom_backend = airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend

# Environment variables
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend
AIRFLOW__AWS_S3_XCOM_BACKEND__BUCKET_NAME=my-airflow-xcom-bucket
AIRFLOW__AWS_S3_XCOM_BACKEND__KEY_PREFIX=xcom/
```

### Custom XCom Backend Implementation

```python
from airflow.models.xcom import BaseXCom
import pandas as pd
import io
import boto3

class S3XComBackend(BaseXCom):
    PREFIX = "s3_xcom://"
    BUCKET = "my-airflow-bucket"
    
    @staticmethod
    def serialize_value(value, *, dag_id, task_id, run_id, map_index, key):
        # If it's a DataFrame, store in S3
        if isinstance(value, pd.DataFrame):
            s3_key = f"xcom/{dag_id}/{run_id}/{task_id}/{key}.parquet"
            buffer = io.BytesIO()
            value.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            s3 = boto3.client("s3")
            s3.put_object(Body=buffer.getvalue(), Bucket=S3XComBackend.BUCKET, Key=s3_key)
            
            # Store the S3 reference in DB (small!)
            return BaseXCom.serialize_value(
                value=S3XComBackend.PREFIX + s3_key,
                dag_id=dag_id, task_id=task_id,
                run_id=run_id, map_index=map_index, key=key,
            )
        
        # For other types, use default serialization
        return BaseXCom.serialize_value(
            value=value, dag_id=dag_id, task_id=task_id,
            run_id=run_id, map_index=map_index, key=key,
        )
    
    @staticmethod
    def deserialize_value(result):
        value = BaseXCom.deserialize_value(result)
        
        if isinstance(value, str) and value.startswith(S3XComBackend.PREFIX):
            s3_key = value[len(S3XComBackend.PREFIX):]
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=S3XComBackend.BUCKET, Key=s3_key)
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))
        
        return value
```

---

## XCom Anti-patterns vs Best Practices

| Anti-pattern | Problem | Solution |
|-------------|---------|---------|
| Store large DataFrame in XCom | Bloats metadata DB, slow queries | Store in S3/GCS, pass file path via XCom |
| Store binary data in XCom | Can exceed DB column limit | Use S3 XCom backend or external storage |
| Pull XCom from many task_ids | Tight coupling between tasks | Design DAG to pass results as function arguments (TaskFlow) |
| Use XCom for inter-DAG communication | Cross-DAG XCom pulls are fragile | Use Datasets, ExternalTaskSensor, or shared storage |
| Never clean XCom | DB grows indefinitely | Set up periodic cleanup DAG |

---

## XCom Size Guidelines

| Data Type | Appropriate? | Alternative |
|-----------|-------------|------------|
| Task status (`"success"`, `"skipped"`) | Yes | — |
| Record count (`1000`) | Yes | — |
| File path (`"s3://bucket/key"`) | Yes | — |
| Small dict (`{"key": "val"}`) | Yes (< 48KB) | — |
| Large dict (> 48KB) | No | S3 XCom backend |
| pandas DataFrame | No | S3/GCS + pass path |
| List of 1M records | No | S3/GCS + pass path |

---

## XCom Cleanup

```python
# Maintenance DAG to clean old XCom values
from airflow.decorators import dag, task
from airflow.utils.session import create_session
from airflow.models import XCom
import pendulum

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance"],
)
def cleanup_xcom():
    
    @task
    def delete_old_xcoms():
        cutoff = pendulum.now("UTC").subtract(days=7)
        with create_session() as session:
            deleted = (
                session.query(XCom)
                .filter(XCom.timestamp < cutoff)
                .delete()
            )
            session.commit()
        return deleted

cleanup = cleanup_xcom()
```
