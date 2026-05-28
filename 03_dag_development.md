# 03 — DAG Development Fundamentals

## What is a DAG?

A **DAG (Directed Acyclic Graph)** is the core workflow definition in Airflow:
- **Directed**: dependencies flow one way (A → B, not B → A)
- **Acyclic**: no circular dependencies (no loops)
- **Graph**: nodes = tasks, edges = dependencies

A DAG is a Python file. It **describes** the workflow — it does NOT run the work itself. Operators define what each task does.

---

## DAG Parameters

```python
from airflow import DAG
from datetime import datetime, timedelta
import pendulum

with DAG(
    dag_id="example_dag",                    # unique identifier (required)
    description="My example pipeline",       # shown in UI
    schedule_interval="0 6 * * *",           # when to run: cron, timedelta, preset, None
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # must be static, timezone-aware
    end_date=None,                           # optional end date
    catchup=False,                           # don't backfill past runs
    max_active_runs=1,                       # max concurrent DAG runs
    max_active_tasks=16,                     # max concurrent tasks in this DAG
    concurrency=16,                          # alias for max_active_tasks
    dagrun_timeout=timedelta(hours=2),       # DAG run times out after 2h
    tags=["etl", "team-data"],              # for filtering in UI
    doc_md="""
    ## My DAG
    Loads data from S3 to Snowflake daily.
    """,
    default_args={                           # default params applied to all tasks
        "owner": "data-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": False,
        "max_retry_delay": timedelta(hours=1),
        "email": ["alerts@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "on_failure_callback": None,
        "on_success_callback": None,
        "sla": timedelta(hours=1),
    },
    on_failure_callback=lambda ctx: print("DAG failed!"),
    on_success_callback=None,
    sla_miss_callback=None,
    render_template_as_native_obj=False,     # render templates as Python objects (not strings)
    params={"env": "prod", "threshold": 100},  # runtime parameters with defaults
) as dag:
    pass
```

---

## Scheduling

### Presets

| Preset | Equivalent Cron | Runs at |
|--------|----------------|---------|
| `@once` | — | Run once only |
| `@hourly` | `0 * * * *` | Every hour |
| `@daily` | `0 0 * * *` | Midnight daily |
| `@weekly` | `0 0 * * 0` | Sunday midnight |
| `@monthly` | `0 0 1 * *` | 1st of month |
| `@yearly` | `0 0 1 1 *` | Jan 1st |
| `None` | — | Manual trigger only |

### schedule_interval options

```python
schedule_interval="@daily"                    # preset
schedule_interval="0 6 * * 1-5"              # cron: 6am weekdays
schedule_interval=timedelta(hours=6)          # every 6 hours
schedule_interval=None                        # manual only
schedule_interval=[Dataset("s3://bucket/")]   # dataset-triggered (2.4+)
```

---

## start_date Rules

```python
# CORRECT — static, timezone-aware
start_date=pendulum.datetime(2024, 1, 1, tz="UTC")
start_date=datetime(2024, 1, 1)  # naive datetime, uses default timezone

# WRONG — dynamic start_date causes new DAG run every parse!
start_date=datetime.now()             # BUG: changes on every parse
start_date=datetime.now() - timedelta(days=1)  # BUG
```

**Rule**: `start_date` must be **static** (hardcoded). The first DAG run covers the data interval `start_date → start_date + schedule_interval`.

---

## catchup

```python
# catchup=True (default): Airflow creates all missed DAG runs since start_date
# DANGER: if start_date is 2 years ago with @hourly, will create 17,520 runs!

# catchup=False: only runs for the current period
catchup=False  # recommended for new DAGs
```

```bash
# Manual backfill (preferred over catchup=True)
airflow dags backfill --start-date 2024-01-01 --end-date 2024-03-01 my_dag
```

---

## depends_on_past

```python
# If True: a task waits for the SAME task in the PREVIOUS DAG run to succeed
# Use case: sequential data processing where order matters
default_args={"depends_on_past": True}
```

Example: If task `transform` failed on Jan 2, and `depends_on_past=True`, then Jan 3's `transform` will wait (won't run) until Jan 2's `transform` succeeds.

---

## Task Dependencies

```python
# Set dependencies with >> and <<
task_a >> task_b          # a must complete before b
task_b << task_a          # same as above

# Multiple dependencies
[task_a, task_b] >> task_c   # both a and b must complete before c
task_a >> [task_b, task_c]   # a feeds into both b and c

# Explicit methods
task_a.set_downstream(task_b)
task_b.set_upstream(task_a)

# Complex DAG
extract >> [transform_a, transform_b] >> merge >> load
```

### ASCII: Dependency Chain
```
           ┌──► transform_a ──┐
extract ───┤                   ├──► merge ──► load
           └──► transform_b ──┘
```

---

## Default Arguments

```python
default_args = {
    "owner": "data-team",           # shown in UI, used for access control
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1),
    "email": ["alert@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # 5m, 10m, 20m, ...
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=1),  # task-level timeout
    "on_failure_callback": notify_slack,
    "pool": "default_pool",
    "priority_weight": 1,
    "queue": "default",
}
```

---

## Dynamic DAGs

### Pattern 1: Loop in one file

```python
# Creates 3 DAGs in one file
configs = [
    {"dag_id": "pipeline_customers", "table": "customers"},
    {"dag_id": "pipeline_orders", "table": "orders"},
    {"dag_id": "pipeline_products", "table": "products"},
]

for config in configs:
    with DAG(
        dag_id=config["dag_id"],
        schedule_interval="@daily",
        start_date=pendulum.datetime(2024, 1, 1),
        catchup=False,
    ) as dag:
        
        @task
        def load(table=config["table"]):
            print(f"Loading {table}")
        
        load()
    
    globals()[config["dag_id"]] = dag  # register in global scope — required!
```

### Pattern 2: DAG Factory from YAML

```python
# config/pipelines.yaml
# pipelines:
#   - name: pipeline_a
#     schedule: "@daily"
#     source_table: raw.orders
#     target_table: processed.orders

import yaml
from pathlib import Path

def create_dag(config):
    with DAG(
        dag_id=f"pipeline_{config['name']}",
        schedule_interval=config["schedule"],
        start_date=pendulum.datetime(2024, 1, 1),
        catchup=False,
    ) as dag:
        # build tasks from config
        pass
    return dag

config_path = Path(__file__).parent / "config" / "pipelines.yaml"
with open(config_path) as f:
    configs = yaml.safe_load(f)

for cfg in configs["pipelines"]:
    dag_obj = create_dag(cfg)
    globals()[dag_obj.dag_id] = dag_obj
```

---

## Airflow Variables

```python
from airflow.models import Variable

# Get variable (raises KeyError if not found)
value = Variable.get("my_variable")

# Get with default
value = Variable.get("my_variable", default_var="default_value")

# Get JSON variable
config = Variable.get("my_config", deserialize_json=True)
# Returns dict/list directly

# Set
Variable.set("my_variable", "new_value")
Variable.set("my_config", {"key": "value"}, serialize_json=True)

# Delete
Variable.delete("my_variable")

# In Jinja templates
# {{ var.value.my_variable }}           — string value
# {{ var.json.my_config.some_key }}     — JSON field access
```

**Important**: Avoid calling `Variable.get()` at the DAG file's top level — it runs on every parse and hammers the DB. Use it inside task callables only.

---

## Airflow Connections

```python
from airflow.hooks.base import BaseHook

# Get connection object
conn = BaseHook.get_connection("my_postgres_conn")
print(conn.host, conn.port, conn.schema, conn.login, conn.password)

# Set connection via CLI
airflow connections add my_conn \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-schema mydb \
  --conn-login user \
  --conn-password pass

# Set via environment variable (takes priority over DB)
export AIRFLOW_CONN_MY_POSTGRES_CONN='postgresql://user:pass@localhost:5432/mydb'
# Or JSON format:
export AIRFLOW_CONN_MY_CONN='{"conn_type": "postgres", "host": "localhost", "login": "user", "password": "pass", "schema": "mydb", "port": 5432}'
```

---

## Airflow Macros & Jinja Templating

Macros are available in operator `template_fields`:

```python
BashOperator(
    task_id="run_script",
    bash_command="""
        echo "Processing date: {{ ds }}"
        echo "Logical date: {{ logical_date }}"
        echo "Next ds: {{ next_ds }}"
        echo "Yesterday: {{ yesterday_ds }}"
        python process.py --date {{ ds }} --run-id {{ run_id }}
    """,
)
```

### Key Template Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{{ ds }}` | Data interval start date (YYYY-MM-DD) | `2024-01-15` |
| `{{ ts }}` | Data interval start timestamp (ISO) | `2024-01-15T00:00:00+00:00` |
| `{{ next_ds }}` | Data interval end date | `2024-01-16` |
| `{{ prev_ds }}` | Previous data interval start | `2024-01-14` |
| `{{ logical_date }}` | Pendulum datetime object | `DateTime(2024, 1, 15, ...)` |
| `{{ data_interval_start }}` | Interval start as Pendulum datetime | — |
| `{{ data_interval_end }}` | Interval end as Pendulum datetime | — |
| `{{ run_id }}` | DAG run identifier | `scheduled__2024-01-15...` |
| `{{ dag.dag_id }}` | The DAG ID | `my_dag` |
| `{{ dag_run.conf }}` | Manual trigger config dict | `{"env": "prod"}` |
| `{{ var.value.KEY }}` | Airflow Variable value | `prod` |
| `{{ conn.CONN_ID.host }}` | Connection attribute | `localhost` |
| `{{ macros.ds_add(ds, 7) }}` | Date math macro | `2024-01-22` |
| `{{ ti.xcom_pull(...) }}` | Pull XCom value | — |

### Template Fields

```python
class MyOperator(BaseOperator):
    template_fields = ["my_param", "sql_query"]  # these fields get Jinja-rendered
    template_ext = [".sql"]                       # file extensions that get rendered
    
    def __init__(self, my_param, sql_query, **kwargs):
        super().__init__(**kwargs)
        self.my_param = my_param
        self.sql_query = sql_query
```

---

## Airflow Params

```python
from airflow.models.param import Param

with DAG(
    dag_id="parameterized_dag",
    params={
        "environment": Param("prod", type="string", enum=["dev", "staging", "prod"]),
        "batch_size": Param(1000, type="integer", minimum=1, maximum=10000),
        "dry_run": Param(False, type="boolean"),
    },
) as dag:
    
    @task
    def run_with_params(**context):
        env = context["params"]["environment"]
        batch_size = context["params"]["batch_size"]
        print(f"Running in {env} with batch_size={batch_size}")
```

Trigger with custom params via UI or API:
```bash
airflow dags trigger my_dag --conf '{"environment": "staging", "batch_size": 500}'
```

---

## DAG Best Practices

| Practice | Why |
|----------|-----|
| Static `start_date` | Dynamic dates cause new DAG runs on every parse |
| `catchup=False` for new DAGs | Avoids accidental backfill flood |
| No heavy imports at module level | Slows DAG parsing, runs on every re-parse |
| Idempotent tasks | Safe to re-run without side effects |
| Small focused tasks | Easier to retry, better parallelism |
| Use `default_args` | Consistent config across all tasks |
| Add `tags` | Easy filtering in UI |
| Add `doc_md` | Self-documenting pipelines |
| Avoid `Variable.get()` at top level | Each parse hits the DB |
| Use `pendulum` over `datetime` | Timezone-aware dates |
| Set `execution_timeout` on tasks | Prevent hung tasks |
| Use `retries` + `retry_delay` | Resilient against transient failures |
