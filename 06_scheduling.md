# 06 — Scheduling & Time Management

## Scheduling Fundamentals

**Critical concept**: Airflow schedules based on **data intervals**, not execution time.

```
schedule_interval = "@daily" (midnight)
start_date = 2024-01-01

Data Interval: [2024-01-01 00:00, 2024-01-02 00:00)
Logical Date:  2024-01-01 00:00
Actual run time: AFTER 2024-01-02 00:00 (at end of interval)
```

The DAG run for "January 1st data" starts running **on January 2nd at midnight** — after the interval closes.

```
Timeline:
Jan 1 00:00  ──────────── Jan 2 00:00 ──────────► (run triggered here)
│←────── data interval ──────────►│
│     logical_date = Jan 1        │
```

---

## Cron Expression Reference

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 7, 0=Sunday)
│ │ │ │ │
* * * * *
```

| Expression | Meaning |
|-----------|---------|
| `0 * * * *` | Every hour at :00 |
| `*/15 * * * *` | Every 15 minutes |
| `0 6 * * *` | Daily at 6:00 AM |
| `0 6 * * 1-5` | Weekdays at 6:00 AM |
| `0 0 1 * *` | First of every month at midnight |
| `0 0 * * 0` | Every Sunday at midnight |
| `0 2 * * 1` | Every Monday at 2:00 AM |
| `0 6,18 * * *` | Daily at 6 AM and 6 PM |
| `30 9 1,15 * *` | 9:30 AM on the 1st and 15th of each month |

### Cron Presets

| Preset | Cron Equivalent | Notes |
|--------|----------------|-------|
| `@once` | — | Run one time only |
| `@hourly` | `0 * * * *` | Top of every hour |
| `@daily` | `0 0 * * *` | Midnight UTC |
| `@weekly` | `0 0 * * 0` | Sunday midnight |
| `@monthly` | `0 0 1 * *` | 1st of month |
| `@yearly` / `@annually` | `0 0 1 1 *` | January 1st |
| `None` | — | Manual trigger only |

---

## Timetables (Airflow 2.2+)

Custom timetables replace `schedule_interval` for complex scheduling needs.

```python
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum

class BusinessHoursTimetable(Timetable):
    """Run every hour only during business hours: Mon-Fri, 9am-5pm UTC."""
    
    def infer_manual_data_interval(self, run_after: pendulum.DateTime) -> DataInterval:
        start = run_after.subtract(hours=1)
        return DataInterval(start=start, end=run_after)
    
    def next_dagrun_info(
        self, *, last_automated_data_interval, restriction
    ) -> DagRunInfo | None:
        if last_automated_data_interval is None:
            next_start = restriction.earliest
        else:
            next_start = last_automated_data_interval.end
        
        # Skip non-business hours
        while True:
            if next_start.day_of_week in [5, 6]:  # Saturday, Sunday
                next_start = next_start.next(pendulum.MONDAY).set(hour=9, minute=0, second=0)
                continue
            if not (9 <= next_start.hour < 17):
                if next_start.hour < 9:
                    next_start = next_start.set(hour=9, minute=0, second=0)
                else:
                    next_start = next_start.add(days=1).set(hour=9, minute=0, second=0)
                continue
            break
        
        end = next_start.add(hours=1)
        if restriction.latest is not None and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(start=next_start, end=end)

# Register timetable via plugin
from airflow.plugins_manager import AirflowPlugin

class MyPlugin(AirflowPlugin):
    name = "my_timetables"
    timetables = [BusinessHoursTimetable]

# Use in DAG
@dag(timetable=BusinessHoursTimetable(), ...)
def business_dag(): ...
```

---

## Data Intervals

| Concept | Definition |
|---------|-----------|
| `data_interval_start` | Start of the time window this run processes |
| `data_interval_end` | End of the time window |
| `logical_date` | Same as `data_interval_start` (replaces `execution_date`) |
| `run_after` | When Airflow actually triggers the run (= `data_interval_end`) |

```python
# Access in templates
"{{ data_interval_start }}"    # pendulum datetime
"{{ data_interval_end }}"
"{{ ds }}"                     # data_interval_start as YYYY-MM-DD
"{{ ds_nodash }}"              # 20240115

# Access in Python
def my_task(**context):
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    logical_date = context["logical_date"]
```

---

## Catchup Mechanism

```python
# catchup=True (default)
# If start_date was 30 days ago with @daily schedule,
# Airflow creates 30 DAG runs immediately!

# catchup=False — only runs for the current/next interval
# Recommended for most production DAGs

@dag(
    start_date=pendulum.datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,  # prevents backfill flood
)
```

---

## Backfills

```bash
# Backfill date range manually
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    my_dag

# Run specific dates
airflow dags backfill \
    --start-date 2024-01-15 \
    --end-date 2024-01-15 \
    my_dag

# Dry run (show what would run without executing)
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --dry-run \
    my_dag

# Rerun failed tasks only
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --rerun-failed-tasks \
    my_dag
```

**Important**: Backfills respect `depends_on_past` — older runs complete before newer ones start.

---

## Manual Triggers

```bash
# Trigger via CLI
airflow dags trigger my_dag
airflow dags trigger my_dag --conf '{"env": "staging", "force": true}'

# Trigger specific logical date
airflow dags trigger my_dag --exec-date 2024-01-15T00:00:00
```

```python
# Trigger via REST API
import requests

resp = requests.post(
    "http://airflow:8080/api/v1/dags/my_dag/dagRuns",
    json={"conf": {"env": "staging"}, "logical_date": "2024-01-15T00:00:00Z"},
    auth=("admin", "admin"),
)
```

---

## Dataset Scheduling (Event-driven, Airflow 2.4+)

Datasets allow DAGs to be triggered when upstream DAGs produce data.

```python
from airflow.datasets import Dataset

# Define datasets
orders_dataset = Dataset("s3://bucket/orders/")
customers_dataset = Dataset("postgresql://host/db/customers")

# Producer DAG — declares it writes to a dataset
@dag(schedule_interval="@daily", ...)
def producer_dag():
    
    @task(outlets=[orders_dataset])    # marks this task as writing to dataset
    def load_orders():
        # load data to S3
        pass
    
    load_orders()

# Consumer DAG — triggered when dataset is updated
@dag(schedule=[orders_dataset, customers_dataset])  # triggered when BOTH are updated
def consumer_dag():
    
    @task(inlets=[orders_dataset])    # marks this task as reading from dataset
    def transform_orders():
        pass
    
    transform_orders()

producer = producer_dag()
consumer = consumer_dag()
```

---

## SLA Management

```python
from datetime import timedelta
from airflow.models import SlaMiss

def sla_miss_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA missed for tasks: {task_list}")
    # Send to Slack, PagerDuty, etc.

with DAG(
    dag_id="sla_dag",
    sla_miss_callback=sla_miss_handler,
    ...
):
    slow_task = PythonOperator(
        task_id="slow_task",
        python_callable=lambda: None,
        sla=timedelta(hours=1),    # task must complete within 1 hour of DAG run start
    )
```

---

## Trigger Rules

Controls when a task should run based on its upstream tasks' states.

| Trigger Rule | Condition to Run |
|-------------|-----------------|
| `all_success` | All upstreams succeeded (default) |
| `all_failed` | All upstreams failed |
| `all_done` | All upstreams done (success, fail, skip) |
| `all_skipped` | All upstreams skipped |
| `one_success` | At least one upstream succeeded |
| `one_failed` | At least one upstream failed |
| `one_done` | At least one upstream done |
| `none_failed` | No upstreams failed (success or skip is OK) |
| `none_failed_min_one_success` | No failures AND at least one success |
| `none_skipped` | No upstreams skipped |
| `always` | Always run regardless of upstream states |

```python
# Common patterns
final_task = EmptyOperator(
    task_id="final",
    trigger_rule="none_failed_min_one_success",  # works after branching
)

cleanup = PythonOperator(
    task_id="cleanup",
    trigger_rule="all_done",    # runs even if upstream failed
    python_callable=cleanup_fn,
)

alert = PythonOperator(
    task_id="alert_on_failure",
    trigger_rule="one_failed",  # runs only if something failed
    python_callable=send_alert,
)
```

---

## Timezones

```python
import pendulum

# Always use pendulum for timezone-aware datetimes
start_date = pendulum.datetime(2024, 1, 1, tz="America/New_York")
start_date = pendulum.datetime(2024, 1, 1, tz="UTC")

# Configure global default timezone
# airflow.cfg:
# [core]
# default_timezone = UTC

# In templates
# {{ execution_date.in_timezone('America/New_York').strftime('%Y-%m-%d') }}
```

---

## ExternalTaskSensor

```python
from airflow.sensors.external_task import ExternalTaskSensor

# Wait for exact same logical_date in upstream DAG
wait = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_dag",
    external_task_id="final_task",    # None = wait for entire DAG
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    execution_date_fn=lambda dt: dt,  # identity: same logical_date
    poke_interval=60,
    timeout=3600,
    mode="reschedule",                # release worker while waiting
)

# If upstream runs hourly but this runs daily
# Map daily date → list of hourly dates
from datetime import timedelta

def map_daily_to_hourly(execution_date):
    return [execution_date.add(hours=h) for h in range(24)]

wait_all_hours = ExternalTaskSensor(
    task_id="wait_all_hours",
    external_dag_id="hourly_dag",
    execution_date_fn=map_daily_to_hourly,
    mode="reschedule",
)
```

---

## Scheduling Best Practices

| Practice | Why |
|----------|-----|
| Use `pendulum` datetimes | Timezone-aware, avoids DST issues |
| Set `catchup=False` for new DAGs | Prevents flood of backfill runs |
| Never use `datetime.now()` as `start_date` | Changes every parse, causes new DAG runs |
| Use `None` schedule for manual-only DAGs | Explicit intent, no accidental runs |
| For event-driven: prefer Datasets over ExternalTaskSensor | Looser coupling between DAGs |
| Test SLA callbacks before production | Silent failures in callbacks hide SLA breaches |
| Use `mode="reschedule"` for long-running sensors | Free up worker slots |
| Set realistic `timeout` on sensors | Prevent sensors running forever |
