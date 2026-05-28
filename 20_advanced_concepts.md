# 20 — Airflow Advanced Concepts

## DAG Serialization Internals

Since Airflow 2.0, DAGs are serialized to JSON and stored in the `serialized_dag` table.

**Why serialization was introduced**:
- Webserver doesn't need access to DAG Python files
- Supports multiple schedulers without file conflicts
- Faster DAG loading in the webserver
- Enables DAG versioning

```python
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import create_session

with create_session() as session:
    # Get serialized DAG
    serialized = session.query(SerializedDagModel).filter_by(dag_id="my_dag").first()
    print(serialized.data)  # JSON representation

# Configuration
# airflow.cfg:
# [core]
# min_serialized_dag_update_interval = 30   # re-serialize only if file changed more than 30s ago
# min_serialized_dag_fetch_interval = 10    # webserver re-reads from DB every 10s
```

---

## Scheduler Internals (Deep Dive)

```python
# Simplified scheduler loop pseudocode
class SchedulerJob:
    
    def _run_scheduler_loop(self):
        while True:
            # Phase 1: Create DAG runs for due schedules
            self._create_dag_runs(dag_models, session)
            
            # Phase 2: Schedule task instances
            self._schedule_dag_runs(dag_runs, session)
            
            # Phase 3: Execute eligible task instances
            # (critical section — uses DB locking for HA)
            with self._executor_lock:
                self._critical_section_execute_task_instances(session)
            
            # Phase 4: Update executor state
            self.executor.heartbeat()
            
            # Phase 5: Process callbacks
            self._process_executor_events(session)
            
            time.sleep(self._heartbeat_sec)
    
    def _critical_section_execute_task_instances(self, session):
        # Get task instances in 'scheduled' state
        tis = session.query(TaskInstance).filter(
            TaskInstance.state == State.SCHEDULED,
            TaskInstance.queue_at < timezone.utcnow(),
        ).limit(self.max_tis_per_query).all()
        
        for ti in tis:
            # Check pool slots
            # Check executor slots
            # Submit to executor
            self.executor.queue_command(ti, command, ...)
```

---

## Triggerer Internals

```python
# Triggerer runs asyncio event loop
# Each deferred task registers a BaseTrigger

class BaseTrigger:
    
    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Async generator yielding TriggerEvent when condition met."""
        raise NotImplementedError
    
    def serialize(self) -> tuple[str, dict]:
        """Return (classpath, init_kwargs) for serialization to DB."""
        raise NotImplementedError

# Example: S3 trigger
class S3KeyTrigger(BaseTrigger):
    def __init__(self, bucket: str, key: str, poll_interval: float = 30.0):
        self.bucket = bucket
        self.key = key
        self.poll_interval = poll_interval
    
    def serialize(self):
        return ("airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger", {
            "bucket": self.bucket,
            "key": self.key,
            "poll_interval": self.poll_interval,
        })
    
    async def run(self):
        import aiobotocore.session
        session = aiobotocore.session.get_session()
        
        while True:
            async with session.create_client("s3") as client:
                try:
                    await client.head_object(Bucket=self.bucket, Key=self.key)
                    yield TriggerEvent({"status": "found", "bucket": self.bucket, "key": self.key})
                    return
                except client.exceptions.ClientError:
                    pass  # not found yet
            
            await asyncio.sleep(self.poll_interval)
```

---

## Deferrable Operator Full Example

```python
from airflow.models import BaseOperator
from airflow.exceptions import TaskDeferred
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio

class AsyncHttpCheckTrigger(BaseTrigger):
    def __init__(self, url: str, expected_status: int = 200, poll_interval: float = 30.0):
        super().__init__()
        self.url = url
        self.expected_status = expected_status
        self.poll_interval = poll_interval
    
    def serialize(self):
        return ("my_module.AsyncHttpCheckTrigger", {
            "url": self.url,
            "expected_status": self.expected_status,
            "poll_interval": self.poll_interval,
        })
    
    async def run(self):
        import aiohttp
        while True:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as resp:
                    if resp.status == self.expected_status:
                        yield TriggerEvent({"status": resp.status, "url": self.url})
                        return
            await asyncio.sleep(self.poll_interval)


class DeferrableHttpSensor(BaseOperator):
    
    def __init__(self, url: str, expected_status: int = 200, **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.expected_status = expected_status
    
    def execute(self, context) -> None:
        # Start the check; if not ready, defer
        if not self._is_ready():
            raise TaskDeferred(
                trigger=AsyncHttpCheckTrigger(
                    url=self.url,
                    expected_status=self.expected_status,
                ),
                method_name="execute_complete",
            )
        return self._process()
    
    def execute_complete(self, context, event: dict) -> None:
        # Called when trigger fires
        self.log.info(f"HTTP check complete: {event}")
        return self._process()
    
    def _is_ready(self) -> bool:
        import requests
        try:
            return requests.get(self.url, timeout=5).status_code == self.expected_status
        except Exception:
            return False
    
    def _process(self):
        self.log.info("Processing response")
        return "done"
```

---

## Airflow REST API

```bash
# Base URL: http://airflow:8080/api/v1

# List DAGs
curl http://airflow:8080/api/v1/dags --user admin:password

# Pause/unpause DAG
curl -X PATCH http://airflow:8080/api/v1/dags/my_dag \
    -H "Content-Type: application/json" \
    -d '{"is_paused": false}' \
    --user admin:password

# Trigger DAG run
curl -X POST http://airflow:8080/api/v1/dags/my_dag/dagRuns \
    -H "Content-Type: application/json" \
    -d '{"conf": {"env": "prod"}, "logical_date": "2024-01-15T00:00:00Z"}' \
    --user admin:password

# Get DAG runs
curl "http://airflow:8080/api/v1/dags/my_dag/dagRuns?limit=10&state=failed" \
    --user admin:password

# Get task instances
curl "http://airflow:8080/api/v1/dags/my_dag/dagRuns/run_id/taskInstances" \
    --user admin:password

# Clear task instance (rerun)
curl -X POST http://airflow:8080/api/v1/dags/my_dag/clearTaskInstances \
    -H "Content-Type: application/json" \
    -d '{"dry_run": false, "task_ids": ["my_task"], "start_date": "2024-01-15T00:00:00Z"}' \
    --user admin:password

# Get variables
curl http://airflow:8080/api/v1/variables --user admin:password

# Set variable
curl -X POST http://airflow:8080/api/v1/variables \
    -H "Content-Type: application/json" \
    -d '{"key": "my_var", "value": "my_value"}' \
    --user admin:password

# Get connections
curl http://airflow:8080/api/v1/connections --user admin:password

# Health check
curl http://airflow:8080/health
```

---

## Custom Plugin Example

```python
# plugins/my_plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

class MyCustomHook(BaseHook):
    conn_name_attr = "my_conn_id"
    default_conn_name = "my_conn_default"
    
    def __init__(self, my_conn_id: str = "my_conn_default"):
        super().__init__()
        self.my_conn_id = my_conn_id
    
    def get_conn(self):
        conn = self.get_connection(self.my_conn_id)
        # return connection object
        return {"host": conn.host, "token": conn.password}

class MyCustomOperator(BaseOperator):
    template_fields = ["endpoint"]
    
    def __init__(self, endpoint: str, conn_id: str = "my_conn_default", **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.conn_id = conn_id
    
    def execute(self, context):
        hook = MyCustomHook(my_conn_id=self.conn_id)
        conn = hook.get_conn()
        self.log.info(f"Calling {conn['host']}/{self.endpoint}")
        return f"called {self.endpoint}"


# Register the plugin
class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    hooks = [MyCustomHook]
    operators = [MyCustomOperator]
    sensors = []
    macros = []
    listeners = []
```

---

## Dataset-based Cross-DAG Dependencies

```python
from airflow.datasets import Dataset
from airflow.decorators import dag, task
import pendulum

# Define shared datasets
orders_silver = Dataset("delta://orders/silver")
customers_silver = Dataset("delta://customers/silver")
analytics_gold = Dataset("delta://analytics/gold")

# Team A: ingestion pipeline
@dag(schedule_interval="@hourly", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def ingest_orders():
    
    @task(outlets=[orders_silver])
    def load_orders():
        # load and clean orders data
        pass
    
    load_orders()

# Team B: customer pipeline  
@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def ingest_customers():
    
    @task(outlets=[customers_silver])
    def load_customers():
        pass
    
    load_customers()

# Data Analytics: triggered when BOTH upstream datasets updated
@dag(schedule=[orders_silver, customers_silver])  # event-driven!
def analytics_transform():
    
    @task(
        inlets=[orders_silver, customers_silver],
        outlets=[analytics_gold],
    )
    def build_analytics():
        # joins orders and customers
        pass
    
    build_analytics()

# Instantiate all
ingest = ingest_orders()
customers = ingest_customers()
analytics = analytics_transform()
```

---

## Multiple Scheduler HA

```ini
# airflow.cfg
[scheduler]
# No special config needed — just run multiple schedulers!
# They use optimistic locking on the DB to coordinate

# Health check threshold — how long before a scheduler is considered dead
scheduler_health_check_threshold = 30
```

```bash
# Start multiple schedulers (they auto-coordinate via DB)
# Machine 1:
airflow scheduler

# Machine 2:
airflow scheduler

# Both are active — no primary/secondary distinction
# If one dies, the other continues without interruption
```

```sql
-- Check active schedulers
SELECT id, state, latest_heartbeat, hostname
FROM job
WHERE job_type = 'SchedulerJob'
  AND state = 'running';
```

---

## Key CLI Commands

```bash
# DAGs
airflow dags list
airflow dags list-import-errors
airflow dags trigger my_dag --conf '{"key": "val"}'
airflow dags pause my_dag
airflow dags unpause my_dag
airflow dags delete my_dag
airflow dags backfill my_dag -s 2024-01-01 -e 2024-01-31
airflow dags show my_dag              # display DAG structure
airflow dags test my_dag 2024-01-15   # test DAG without metadata DB

# Tasks
airflow tasks list my_dag
airflow tasks test my_dag my_task 2024-01-15     # run task without metadata DB
airflow tasks run my_dag my_task run_id           # run with metadata DB
airflow tasks clear my_dag -s 2024-01-01 -e 2024-01-31
airflow tasks states-for-dag-run my_dag run_id

# DB
airflow db init
airflow db upgrade
airflow db check
airflow db clean --clean-before-timestamp 2024-01-01T00:00:00

# Users / Roles
airflow users create --username admin --role Admin ...
airflow users list
airflow roles create my_role
airflow roles list

# Connections
airflow connections list
airflow connections add my_conn --conn-type postgres --conn-host localhost
airflow connections delete my_conn

# Variables
airflow variables list
airflow variables set key value
airflow variables get key
airflow variables delete key

# Pools
airflow pools list
airflow pools set my_pool 10 "description"
airflow pools delete my_pool
```
