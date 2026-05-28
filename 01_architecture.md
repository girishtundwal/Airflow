# 01 — Airflow Architecture Deep Dive

## Full Airflow 2.x Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW 2.x ARCHITECTURE                         │
│                                                                          │
│   DAG Author                                                             │
│       │ writes Python files                                              │
│       ▼                                                                  │
│  ┌─────────┐    parse     ┌─────────────────────────────────────────┐   │
│  │DAG Files│──────────────►│           SCHEDULER                     │   │
│  │ /dags/  │              │  ┌──────────────┐  ┌─────────────────┐  │   │
│  └─────────┘              │  │DagFileProc.  │  │  SchedulerJob   │  │   │
│                           │  │  Manager     │  │  (main loop)    │  │   │
│                           │  └──────┬───────┘  └────────┬────────┘  │   │
│                           └─────────┼───────────────────┼───────────┘   │
│                                     │ serialized DAGs   │ queue tasks   │
│                                     ▼                   ▼               │
│                           ┌─────────────────────────────────────────┐   │
│                           │          METADATA DATABASE               │   │
│                           │  (PostgreSQL / MySQL)                    │   │
│                           │  - dag, dag_run, task_instance           │   │
│                           │  - xcom, log, connection, variable       │   │
│                           │  - slot_pool, job, trigger               │   │
│                           └───────────────────┬─────────────────────┘   │
│                                               │                         │
│              ┌────────────────────────────────┼───────────────────┐     │
│              │                                │                   │     │
│              ▼                                ▼                   ▼     │
│      ┌───────────────┐            ┌──────────────────┐   ┌─────────────┐│
│      │   WEBSERVER   │            │    EXECUTOR      │   │  TRIGGERER  ││
│      │  (Flask/FAB)  │            │                  │   │  (asyncio)  ││
│      │  Port 8080    │            │ Local / Celery / │   │ deferrable  ││
│      │  UI + REST API│            │   Kubernetes     │   │  operators  ││
│      └───────────────┘            └────────┬─────────┘   └─────────────┘│
│                                            │                             │
│                                   ┌────────▼──────────┐                 │
│                                   │     WORKERS       │                 │
│                                   │ (Celery workers / │                 │
│                                   │  K8s pods /       │                 │
│                                   │  local process)   │                 │
│                                   └───────────────────┘                 │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## DAG Processing Flow

1. DAG author writes a `.py` file to `$AIRFLOW_HOME/dags/`
2. **DagFileProcessorManager** scans the dags folder every `min_file_process_interval` seconds
3. Each DAG file is parsed in a subprocess with a timeout (`dagbag_import_timeout`)
4. Parsed DAG is **serialized to JSON** and stored in `serialized_dag` table
5. **SchedulerJob** reads serialized DAGs from DB — no direct file access
6. Webserver reads serialized DAGs from DB for UI rendering
7. Scheduler creates `DagRun` and `TaskInstance` records
8. Eligible task instances are submitted to the Executor

---

## Scheduler Internals

The Scheduler runs two interleaved loops:

### DAG Parsing Loop (`DagFileProcessorManager`)
```
while True:
    scan dags/ folder for new/modified files
    spawn subprocess for each file
    each subprocess imports the file, extracts DAG objects
    serialize DAGs to metadata DB
    sleep(min_file_process_interval)  # default: 30s
```

### Task Scheduling Loop (`SchedulerJob._run_scheduler_loop`)
```
while True:
    _create_dag_runs()       # create DagRun for due schedules
    _schedule_dag_runs()     # move task instances to SCHEDULED state
    _critical_section_execute_task_instances()  # submit to executor
    executor.heartbeat()     # check running tasks, update states
    sleep(scheduler_heartbeat_sec)  # default: 5s
```

Key config:
```ini
[scheduler]
scheduler_heartbeat_sec = 5
min_file_process_interval = 30
dag_file_processor_timeout = 50
parsing_processes = 2       # parallel DAG file parsers
```

---

## Task Lifecycle

```
NONE ──► SCHEDULED ──► QUEUED ──► RUNNING ──► SUCCESS
                          │                      │
                          │           ┌──────────┘
                          │           ▼
                          │        FAILED ──► UP_FOR_RETRY ──► QUEUED (retry)
                          │           │
                          │           ▼
                          │      UP_FOR_RESCHEDULE (sensors in reschedule mode)
                          │
                          ▼
                       SKIPPED (trigger rule not met, BranchOperator skipped)
                       DEFERRED (deferrable operators waiting on Triggerer)
                       REMOVED (task removed from DAG but DagRun still active)
                       UPSTREAM_FAILED (upstream task failed, trigger rule blocks)
```

### Task State Descriptions

| State | Description |
|-------|------------|
| `scheduled` | TI created, waiting for executor slot |
| `queued` | Submitted to executor, waiting for worker |
| `running` | Worker is actively executing the task |
| `success` | Task completed successfully |
| `failed` | Task raised an exception |
| `up_for_retry` | Failed but has retries remaining |
| `up_for_reschedule` | Sensor in reschedule mode, released worker slot |
| `skipped` | Skipped by BranchOperator or trigger rule |
| `deferred` | Deferrable operator waiting on Triggerer |
| `removed` | Task was removed from DAG definition |
| `upstream_failed` | Upstream task failed, trigger rule blocked execution |

---

## Metadata Database Internals

### Key Tables

```
dag                    — one row per DAG (dag_id, is_paused, schedule_interval, fileloc)
dag_run                — one row per DAG execution (dag_id, run_id, state, execution_date)
task_instance          — one row per task execution (dag_id, task_id, run_id, state, try_number)
serialized_dag         — JSON-serialized DAG (replaces direct file reads by webserver/scheduler)
xcom                   — key-value data passed between tasks
log                    — audit log of events (task events, user actions)
connection             — named connection credentials (conn_id, conn_type, host, login, password)
variable               — key-value configuration (key, val, is_encrypted)
slot_pool              — named resource pools (pool, slots)
job                    — heartbeat records for scheduler/worker jobs
trigger                — pending deferrable operator triggers
```

### Relationships
```
dag (1) ──── (N) dag_run (1) ──── (N) task_instance
                                          │
                                          └──── (N) xcom
                                          └──── (N) log
```

---

## Executor Architecture

### Sequential Executor
```
Scheduler ──► [Task 1] ──► [Task 2] ──► [Task 3]
                (serial, one at a time)
SQLite only, development use, no parallelism
```

### Local Executor
```
Scheduler ──► subprocess [Task 1]
           ├─► subprocess [Task 2]   (parallel subprocesses)
           └─► subprocess [Task 3]
PostgreSQL/MySQL required
```

### Celery Executor Architecture
```
┌────────────┐   submit   ┌─────────────────────┐
│ Scheduler  │──────────►│   Message Broker     │
│            │            │  (Redis / RabbitMQ)  │
└────────────┘            └──────────┬──────────┘
                                     │ consume
                         ┌───────────┼───────────┐
                         ▼           ▼            ▼
                    ┌─────────┐ ┌─────────┐ ┌─────────┐
                    │Worker 1 │ │Worker 2 │ │Worker 3 │
                    │(celery) │ │(celery) │ │(celery) │
                    └────┬────┘ └────┬────┘ └────┬────┘
                         │           │            │
                         └───────────┼────────────┘
                                     │ store results
                                     ▼
                          ┌──────────────────────┐
                          │   Result Backend     │
                          │   (Redis / DB)       │
                          └──────────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │   Metadata DB       │
                          │   (state updates)   │
                          └─────────────────────┘
```

### Kubernetes Executor Architecture
```
┌────────────┐  create pod  ┌────────────────────┐
│ Scheduler  │─────────────►│  Kubernetes API    │
│            │              └────────────────────┘
└────────────┘                       │ schedules
     ▲                               ▼
     │ watch pod                ┌──────────┐
     │ state                    │  Worker  │
     └──────────────────────────│   Pod    │
                                │ (ephemeral,
                                │  deleted after
                                │  task completes)
                                └──────────┘
```

---

## Triggerer Architecture (Deferrable Operators)

The Triggerer enables operators to **defer** execution — freeing the worker slot while waiting for an external condition.

```
Normal operator flow:
  Worker ──► [waits 2 hours for S3 file] ──► continues
             (worker slot OCCUPIED for 2 hours)

Deferrable operator flow:
  Worker ──► defer(trigger=S3KeyTrigger(...)) ──► worker slot FREED
                    │
                    ▼
              Triggerer (asyncio)
              runs BaseTrigger.run() coroutine
              polls S3 every 30s asynchronously
              fires TriggerEvent when condition met
                    │
                    ▼
              Worker (new slot) ──► resumes task from execute_complete()
```

Triggerer internals:
- Runs an asyncio event loop
- Each trigger = one Python coroutine (lightweight)
- Can handle thousands of concurrent deferred operators
- Stores pending triggers in `trigger` table

---

## Airflow State Management

State machine ensures tasks only transition through valid states:

```python
# Task can only go: scheduled → queued → running → success/failed
# Failed with retries: failed → up_for_retry → queued → running
# Sensor reschedule: running → up_for_reschedule → scheduled → running
# Deferrable: running → deferred → scheduled → running (new worker)
```

State is stored in `task_instance.state` in metadata DB. Multiple schedulers use **optimistic locking** to avoid race conditions.

---

## DAG Serialization

Since Airflow 2.0, DAGs are serialized to JSON and stored in the `serialized_dag` table:

- **Why**: Webserver no longer needs access to DAG files; faster loading; supports multi-scheduler HA
- **Format**: JSON representation of the DAG structure, task configs, operator params
- **When**: Serialized on each DAG parse (whenever file changes)
- **Access**: Webserver reads serialized DAGs; scheduler also uses them for task scheduling

```ini
[core]
store_serialized_dags = True  # default in 2.x
min_serialized_dag_update_interval = 30  # seconds
min_serialized_dag_fetch_interval = 10   # seconds
```

---

## Airflow Plugins Architecture

Plugins extend Airflow without modifying core code. Place in `$AIRFLOW_HOME/plugins/`.

```python
# plugins/my_plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

class MyCustomHook(BaseHook):
    def get_conn(self): ...

class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    hooks = [MyCustomHook]
    operators = []
    sensors = []
    macros = []
    listeners = []
```

Plugin types:
- **Operators** — custom task types
- **Hooks** — custom external system interfaces
- **Sensors** — custom waiting conditions
- **Macros** — custom Jinja template functions
- **Views/Blueprints** — custom web UI pages
- **Listeners** — react to task/DAG lifecycle events (2.x)

---

## High Availability Architecture

```
                    ┌─────────────────────────────┐
                    │     Load Balancer            │
                    └──────────┬──────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌────────────┐  ┌────────────┐  ┌────────────┐
       │ Webserver 1│  │ Webserver 2│  │ Webserver 3│
       └────────────┘  └────────────┘  └────────────┘

       ┌────────────┐  ┌────────────┐
       │Scheduler 1 │  │Scheduler 2 │  (active/active since 2.0)
       │(active)    │  │(active)    │
       └────────────┘  └────────────┘

       ┌────────────┐  ┌────────────┐  ┌────────────┐
       │  Worker 1  │  │  Worker 2  │  │  Worker 3  │
       └────────────┘  └────────────┘  └────────────┘

                    ┌─────────────────────────────┐
                    │  PostgreSQL HA               │
                    │  (Primary + Standby / RDS)  │
                    └─────────────────────────────┘
```

Multiple schedulers: use optimistic locking on task_instance rows to prevent duplicate task scheduling. Each scheduler independently runs its loop; the DB lock determines which one "wins" each task.

---

## Component Communication

| From → To | Mechanism |
|-----------|-----------|
| Scheduler → Workers (Celery) | Message broker (Redis/RabbitMQ) |
| Scheduler → Workers (K8s) | Kubernetes API |
| Workers → Scheduler | Metadata DB state updates |
| Webserver → Scheduler | Metadata DB |
| All → Metadata DB | SQLAlchemy ORM |
| Triggerer → Scheduler | Metadata DB trigger table |

---

## Heartbeats

- **SchedulerJob**: updates `job.latest_heartbeat` every `scheduler_heartbeat_sec` (default 5s)
- **CeleryWorker**: Celery's own heartbeat via broker
- **Health check**: `/health` endpoint on webserver checks scheduler heartbeat freshness
- If scheduler heartbeat is stale > `scheduler_health_check_threshold` (default 30s) — scheduler is considered dead

---

## Airflow Scaling Concepts

| Dimension | How to Scale |
|-----------|-------------|
| More tasks in parallel | Increase `parallelism` config, add workers |
| More DAGs | Increase `parsing_processes`, tune `min_file_process_interval` |
| Worker isolation | KubernetesExecutor (one pod per task) |
| Scheduler HA | Multiple schedulers (Airflow 2.0+) |
| DB load | PgBouncer connection pooler, read replicas |
| Worker autoscaling | KEDA (Celery), HPA (K8s) |
