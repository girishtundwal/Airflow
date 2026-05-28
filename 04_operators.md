# 04 — Airflow Operators

## What are Operators?

Operators are **templates that define what a task does**. When you create a task in a DAG, you instantiate an Operator. The Operator's `execute()` method is called when the task runs.

```
Operator class → Task instance when placed in a DAG
BaseOperator → all operators inherit from this
```

---

## BashOperator

```python
from airflow.operators.bash import BashOperator

run_script = BashOperator(
    task_id="run_script",
    bash_command="python /scripts/process.py --date {{ ds }}",
    env={"MY_VAR": "value", "PATH": "/usr/bin:$PATH"},
    append_env=True,           # append to existing env, don't replace
    output_encoding="utf-8",
    skip_on_exit_code=99,      # exit code 99 → skip (not fail)
    cwd="/opt/scripts",        # working directory
    retries=2,
)

# Multi-line command
run_complex = BashOperator(
    task_id="complex",
    bash_command="""
        set -e
        echo "Start: {{ ds }}"
        python process.py --date {{ ds }} --env {{ dag_run.conf.get('env', 'prod') }}
        echo "Done"
    """,
)
```

**Note**: BashOperator pushes the **last line of stdout** to XCom as `return_value`.

---

## PythonOperator

```python
from airflow.operators.python import PythonOperator

def my_function(arg1, arg2, **context):
    ds = context["ds"]
    ti = context["ti"]
    run_id = context["run_id"]
    return f"processed {arg1} on {ds}"

task = PythonOperator(
    task_id="my_python_task",
    python_callable=my_function,
    op_args=["value1"],            # positional args
    op_kwargs={"arg2": "value2"},  # keyword args
    templates_dict={"date": "{{ ds }}"},  # Jinja-rendered dict passed as templates_dict
    provide_context=True,          # deprecated in 2.x — context always provided via **kwargs
)
```

---

## BranchPythonOperator

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def choose_branch(**context):
    source = context["dag_run"].conf.get("source", "db")
    if source == "db":
        return "load_from_db"      # return task_id to follow
    elif source == "api":
        return "load_from_api"
    else:
        return ["load_from_db", "load_from_api"]  # can return list

decide = BranchPythonOperator(
    task_id="decide",
    python_callable=choose_branch,
)

load_db = EmptyOperator(task_id="load_from_db")
load_api = EmptyOperator(task_id="load_from_api")

# Must handle "skipped" state with trigger_rule
merge = EmptyOperator(
    task_id="merge",
    trigger_rule="none_failed_min_one_success",  # run if at least one path succeeded
)

decide >> [load_db, load_api] >> merge
```

---

## ShortCircuitOperator

```python
from airflow.operators.python import ShortCircuitOperator

def check_data_exists(**context):
    # Return True to continue, False to skip all downstream tasks
    count = get_row_count(context["ds"])
    return count > 0

check = ShortCircuitOperator(
    task_id="check_data",
    python_callable=check_data_exists,
    ignore_downstream_trigger_rules=True,  # propagate skip through branches
)
```

---

## EmptyOperator (DummyOperator)

```python
from airflow.operators.empty import EmptyOperator

# Use for structure: start/end markers, join points
start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end", trigger_rule="none_failed")

start >> [task_a, task_b, task_c] >> end
```

---

## TriggerDagRunOperator

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger = TriggerDagRunOperator(
    task_id="trigger_downstream",
    trigger_dag_id="downstream_dag",
    conf={"source": "upstream_pipeline", "date": "{{ ds }}"},
    wait_for_completion=True,           # wait for triggered DAG to finish
    poke_interval=30,                   # check every 30s if wait_for_completion=True
    allowed_states=["success"],         # fail if downstream DAG fails
    reset_dag_run=True,                 # re-run if DAG run already exists
    execution_date="{{ data_interval_start.isoformat() }}",
)
```

---

## ExternalTaskSensor (cross-DAG dependency)

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_dag",
    external_task_id="final_task",      # None = wait for entire DAG run
    allowed_states=["success"],
    execution_date_fn=lambda dt: dt,    # map current logical_date to upstream logical_date
    timeout=3600,                       # fail after 1 hour
    poke_interval=60,
    mode="reschedule",                  # don't hold worker slot while waiting
)
```

---

## SQL Operators

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="my_postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS orders_{{ ds_nodash }} (
            id SERIAL PRIMARY KEY,
            order_date DATE,
            amount NUMERIC
        );
    """,
    autocommit=True,
)

# Run SQL from file
run_query = PostgresOperator(
    task_id="run_query",
    postgres_conn_id="my_postgres",
    sql="sql/transform_orders.sql",    # file path relative to dags folder
    parameters={"date": "{{ ds }}"},
)
```

---

## KubernetesPodOperator

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

run_in_pod = KubernetesPodOperator(
    task_id="run_spark_job",
    name="spark-job",
    namespace="airflow",
    image="my-spark-image:latest",
    cmds=["spark-submit"],
    arguments=["--master", "k8s://...", "app.py", "--date", "{{ ds }}"],
    env_vars=[
        k8s.V1EnvVar(name="ENV", value="prod"),
        k8s.V1EnvVar(
            name="DB_PASSWORD",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(name="db-secret", key="password")
            ),
        ),
    ],
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "1", "memory": "2Gi"},
        limits={"cpu": "2", "memory": "4Gi"},
    ),
    volumes=[
        k8s.V1Volume(
            name="data-vol",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="data-pvc"),
        )
    ],
    volume_mounts=[k8s.V1VolumeMount(name="data-vol", mount_path="/data")],
    in_cluster=True,               # running inside K8s cluster
    get_logs=True,                 # stream pod logs
    is_delete_operator_pod=True,   # delete pod after completion
    service_account_name="airflow-worker",
)
```

---

## DockerOperator

```python
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

run_container = DockerOperator(
    task_id="run_in_docker",
    image="my-pipeline:latest",
    command="python /app/process.py --date {{ ds }}",
    environment={"ENV": "prod", "DATE": "{{ ds }}"},
    mounts=[
        Mount(source="/host/data", target="/app/data", type="bind")
    ],
    network_mode="bridge",
    auto_remove="success",         # remove container on success
    docker_url="unix://var/run/docker.sock",
    api_version="auto",
    mem_limit="2g",
)
```

---

## Custom Operator

```python
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SnowflakeToS3Operator(BaseOperator):
    
    template_fields = ["sql", "s3_key"]   # fields that accept Jinja templates
    template_ext = [".sql"]
    ui_color = "#e07c1a"                  # task box color in UI
    
    def __init__(
        self,
        sql: str,
        s3_bucket: str,
        s3_key: str,
        snowflake_conn_id: str = "snowflake_default",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.snowflake_conn_id = snowflake_conn_id
        self.aws_conn_id = aws_conn_id
    
    def execute(self, context):
        self.log.info(f"Running query: {self.sql}")
        # Use hooks to connect to systems
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        sf_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        df = sf_hook.get_pandas_df(self.sql)
        
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        csv_buffer = df.to_csv(index=False)
        s3_hook.load_string(
            string_data=csv_buffer,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True,
        )
        
        self.log.info(f"Exported {len(df)} rows to s3://{self.s3_bucket}/{self.s3_key}")
        return len(df)
```

---

## Deferrable Operator

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models import BaseOperator
from airflow.exceptions import TaskDeferred
import asyncio

class MyAsyncTrigger(BaseTrigger):
    def __init__(self, file_path: str, poll_interval: int = 30):
        super().__init__()
        self.file_path = file_path
        self.poll_interval = poll_interval
    
    def serialize(self):
        return ("my_module.MyAsyncTrigger", {
            "file_path": self.file_path,
            "poll_interval": self.poll_interval,
        })
    
    async def run(self):
        while True:
            if await self._check_file():
                yield TriggerEvent({"file_path": self.file_path, "status": "found"})
                return
            await asyncio.sleep(self.poll_interval)
    
    async def _check_file(self):
        import aiofiles.os
        return await aiofiles.os.path.exists(self.file_path)


class DeferrableFileOperator(BaseOperator):
    
    def execute(self, context):
        if not self._file_exists():
            raise TaskDeferred(
                trigger=MyAsyncTrigger(file_path=self.file_path),
                method_name="execute_complete",  # called when trigger fires
            )
        return self._process_file()
    
    def execute_complete(self, context, event):
        # Called by Triggerer when condition is met
        self.log.info(f"File found: {event['file_path']}")
        return self._process_file()
```

---

## Operator Best Practices

| Practice | Detail |
|----------|--------|
| Set `retries` | Default to 2-3 retries for transient failures |
| Set `execution_timeout` | Prevent tasks from running indefinitely |
| Set `sla` | Alert when task exceeds expected duration |
| Use `on_failure_callback` | Send alerts (Slack, PagerDuty) on failure |
| Set `pool` | Prevent overloading shared resources |
| Use `priority_weight` | Critical tasks run first |
| Idempotent `execute()` | Safe to re-run multiple times |
| Use hooks in operators | Don't hardcode credentials |
| Avoid long-running sensors in poke mode | Use `mode="reschedule"` |
