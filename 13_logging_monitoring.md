# 13 — Airflow Logging & Monitoring

## Airflow Logging Architecture

```
Task executes on worker
    │
    ├── stdout/stderr captured
    ├── Airflow's TaskHandler writes to log file
    │   Default: $AIRFLOW_HOME/logs/dag_id=X/run_id=Y/task_id=Z/attempt=N.log
    │
    └── If remote logging configured: uploaded to S3/GCS/Elasticsearch
```

Log files are fetched by the webserver via the worker's REST API when viewing in UI.

---

## Task Log Configuration

```ini
[logging]
base_log_folder = /opt/airflow/logs

# Template for log file path
log_filename_template = dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log

# Log level
logging_level = INFO
fab_logging_level = WARNING

# Remote logging
remote_logging = False
remote_base_log_folder = s3://my-airflow-logs/
remote_log_conn_id = aws_default
encrypt_s3_logs = False
```

---

## Remote Logging

### S3 Logging

```ini
[logging]
remote_logging = True
remote_base_log_folder = s3://my-airflow-bucket/logs/
remote_log_conn_id = aws_default
encrypt_s3_logs = False
```

```python
# Required provider
# apache-airflow-providers-amazon
```

### GCS Logging

```ini
[logging]
remote_logging = True
remote_base_log_folder = gs://my-airflow-bucket/logs/
remote_log_conn_id = google_cloud_default
```

### Elasticsearch Logging

```ini
[elasticsearch]
host = http://elasticsearch:9200
log_id_template = {dag_id}-{task_id}-{run_id}-{map_index}-{try_number}
end_of_log_mark = end_of_log
frontend = https://kibana.example.com/app/kibana#/discover?_a=(columns:!(message),query:(language:kuery,query:'log_id:{{ log_id }}'))
write_stdout = True
json_format = True
json_fields = asctime,filename,lineno,levelname,message
```

---

## Logging in DAG Code

```python
from airflow.decorators import task
import logging

logger = logging.getLogger(__name__)

@task
def my_task(**context):
    # These appear in the task log
    logger.info("Starting task")
    logger.warning("High memory usage detected")
    logger.error("Something went wrong")
    
    # Or use self.log in operator
    # self.log.info("In operator execute()")

# In BaseOperator subclass
class MyOp(BaseOperator):
    def execute(self, context):
        self.log.info(f"Processing date: {context['ds']}")
        self.log.warning("Data quality check failed")
```

---

## Monitoring with StatsD

StatsD emits metrics from Airflow to any StatsD-compatible backend (Telegraf, Graphite, etc.).

```ini
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
statsd_allow_list =          # optional: filter metrics
statsd_datadog_enabled = False  # enable Datadog tags format
```

### Key StatsD Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `airflow.dag.loading-duration.*` | Timer | Time to load DAG files |
| `airflow.dag_processing.total_parse_time` | Gauge | Total DAG parsing time |
| `airflow.scheduler.tasks.starving` | Gauge | Tasks that can't start (no executor slots) |
| `airflow.scheduler.tasks.executable` | Gauge | Tasks ready to run |
| `airflow.executor.open_slots` | Gauge | Available executor slots |
| `airflow.executor.queued_tasks` | Gauge | Tasks in queue |
| `airflow.executor.running_tasks` | Gauge | Tasks currently running |
| `airflow.ti.start.*` | Counter | Task instance starts |
| `airflow.ti.finish.*` | Counter | Task instance completions |
| `airflow.dagrun.duration.success.*` | Timer | Successful DAG run duration |
| `airflow.dagrun.duration.failed.*` | Timer | Failed DAG run duration |
| `airflow.pool.open_slots.*` | Gauge | Available pool slots |
| `airflow.pool.queued_slots.*` | Gauge | Queued pool slots |

---

## Prometheus Integration

```bash
# Install exporter
pip install airflow-exporter
# Or use: apache-airflow-providers-prometheus (built-in since 2.x)
```

```ini
[metrics]
statsd_on = False  # StatsD and prometheus are alternatives

# Built-in Prometheus metrics endpoint (requires extra package)
# Access: http://airflow-webserver:8080/metrics
```

### Key Prometheus Metrics

```
# Scheduler lag
airflow_scheduler_heartbeat          # seconds since last scheduler heartbeat

# Task metrics
airflow_task_instance_created_*      # counter by state
airflow_task_running_count           # currently running tasks

# DAG run metrics
airflow_dag_run_duration             # histogram of DAG run durations
airflow_dag_run_ongoing              # current running DAG runs

# Executor
airflow_executor_running_tasks
airflow_executor_queued_tasks
airflow_executor_open_slots
```

---

## Grafana Dashboard Panels

Recommended panels for Airflow monitoring dashboard:

| Panel | Metric | Alert Condition |
|-------|--------|----------------|
| Scheduler heartbeat age | `airflow_scheduler_heartbeat` | > 30s |
| Running tasks | `airflow_executor_running_tasks` | < 0 (shouldn't happen) |
| Queued tasks | `airflow_executor_queued_tasks` | > 50 sustained |
| DAG run failures | `airflow_dagrun_failure_count` | > 0 |
| Task failure rate | `rate(airflow_ti_finish_failed[5m])` | > 5/min |
| Executor open slots | `airflow_executor_open_slots` | = 0 (bottleneck) |
| Pool saturation | `airflow_pool_open_slots` | = 0 |
| DAG parsing duration | `airflow_dag_processing_duration` | > 30s |

---

## Alerting via Callbacks

```python
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from typing import Any
import json
import urllib.request

def slack_alert(context: dict) -> None:
    """Send task failure alert to Slack."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    
    message = {
        "text": (
            f":red_circle: *DAG Failed*\n"
            f"*DAG*: {dag_id}\n"
            f"*Task*: {task_id}\n"
            f"*Execution Date*: {execution_date}\n"
            f"*Error*: {str(exception)[:200]}\n"
            f"*Log*: http://airflow:8080/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"
        )
    }
    
    webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(message).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)


def pagerduty_alert(context: dict) -> None:
    """Trigger PagerDuty incident on task failure."""
    import requests
    
    requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json={
            "routing_key": "YOUR_INTEGRATION_KEY",
            "event_action": "trigger",
            "payload": {
                "summary": f"Airflow task failed: {context['dag'].dag_id}.{context['task_instance'].task_id}",
                "severity": "critical",
                "source": "airflow",
            },
        },
    )


# Apply to DAG
with DAG(
    dag_id="production_pipeline",
    default_args={
        "on_failure_callback": slack_alert,
        "on_retry_callback": None,
    },
    on_failure_callback=slack_alert,       # DAG-level failure
    sla_miss_callback=slack_alert,         # SLA miss
) as dag:
    pass
```

---

## SLA Monitoring

```python
from datetime import timedelta

def sla_miss_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    task_names = ", ".join(t.task_id for t in task_list)
    print(f"SLA missed for: {task_names} in DAG {dag.dag_id}")
    # Send to Slack, PagerDuty, etc.

with DAG(
    dag_id="sla_monitored_dag",
    sla_miss_callback=sla_miss_handler,
    schedule_interval="@daily",
    ...
):
    extract = PythonOperator(
        task_id="extract",
        sla=timedelta(hours=1),    # must complete within 1h of DAG start
        python_callable=do_extract,
    )
    
    transform = PythonOperator(
        task_id="transform",
        sla=timedelta(hours=2),    # must complete within 2h of DAG start
        python_callable=do_transform,
    )
```

---

## Airflow UI Monitoring Views

| View | Purpose |
|------|---------|
| **Grid** (formerly Tree) | Task state history across DAG runs — spot patterns |
| **Graph** | DAG structure + current run state |
| **Gantt** | Task timing + parallelism visualization |
| **Task Duration** | Historical task duration trends |
| **Landing Times** | How late each task finishes vs expected |
| **Calendar** | Run success/failure per day |
| **Audit Log** | User actions and system events |
| **Browse → DAG Runs** | Filter/search all DAG runs |
| **Browse → Task Instances** | Filter by state, date, task |

---

## Audit Logging

```python
# Query audit log table
from airflow.utils.session import create_session
from airflow.models import Log

with create_session() as session:
    logs = (
        session.query(Log)
        .filter(Log.dag_id == "my_dag")
        .order_by(Log.dttm.desc())
        .limit(100)
        .all()
    )
    
    for log in logs:
        print(f"{log.dttm}: {log.event} by {log.owner} — {log.extra}")
```

---

## Observability Best Practices

1. **Centralize logs** to S3/GCS/Elasticsearch — local logs are lost when containers restart
2. **Set up StatsD/Prometheus** before going to production
3. **Create Grafana dashboard** with scheduler health, task failure rate, queue depth
4. **Alert on scheduler lag** — if heartbeat > 30s, investigate immediately
5. **Alert on pool saturation** — if open_slots = 0, tasks will queue indefinitely
6. **Use `on_failure_callback`** on all production DAGs
7. **Set SLAs** on critical tasks
8. **Monitor XCom table size** — large XCom values cause DB slowdowns
9. **Archive/clean logs** regularly — log directories grow indefinitely by default
