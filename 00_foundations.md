# 00 — Apache Airflow Foundations

## What is Apache Airflow?

Apache Airflow is an open-source **workflow orchestration platform** for programmatically authoring, scheduling, and monitoring data pipelines. Workflows are defined as Python code (DAGs), giving you full power of the language for dynamic pipeline generation.

- Created by Maxime Beauchemin at **Airbnb in 2014**
- Open-sourced in 2015
- Joined **Apache Incubator in 2016**, became Apache top-level project in **2019**
- Current stable: Airflow 2.x (2.0 released Dec 2020 — major rewrite)

---

## Why Airflow Was Created

Airbnb needed a way to manage hundreds of complex data pipelines with dependencies, retries, monitoring, and scheduling — cron jobs alone were insufficient:

- No dependency management between jobs
- No visibility into failures
- No retry logic
- No way to parameterize runs
- No UI to monitor status

Airflow solved all of this by treating pipelines as **code**.

---

## Workflow Orchestration Basics

Orchestration = **defining, scheduling, and monitoring** workflows composed of tasks.

Core principles:
- **Idempotency** — re-running produces same result
- **Dependency management** — tasks run only when dependencies succeed
- **Retry logic** — automatic retries on failure
- **Observability** — logs, state tracking, alerting

---

## ETL vs ELT Orchestration

| Aspect | ETL | ELT |
|--------|-----|-----|
| Transform location | Before load (compute layer) | After load (in warehouse) |
| Tool | Spark, Python, pandas | dbt, SQL, Snowflake |
| Airflow role | Orchestrate transform → load | Orchestrate extract → load → trigger dbt |
| Use case | Complex transforms, sensitive data | Cloud warehouses (Snowflake, BigQuery, Redshift) |

**Airflow orchestrates both** — it doesn't do the transformation itself, it schedules and monitors the tools that do.

---

## Batch Processing Concepts

- **Batch**: process a fixed chunk of data at a scheduled time
- **Data interval**: the time window of data to process (e.g., one day's events)
- **Idempotent batch**: safe to re-run; same inputs → same output
- **Backfill**: processing historical data intervals retroactively

---

## Directed Acyclic Graphs (DAGs)

A **DAG** is the core abstraction in Airflow. It represents a workflow as a graph where:
- **Nodes** = Tasks
- **Edges** = Dependencies between tasks
- **Directed** = dependencies flow in one direction
- **Acyclic** = no circular dependencies (no loops)

```
ASCII: Simple DAG Structure

  [extract] ──► [transform] ──► [load]
                    │
                    ▼
               [validate]
```

```python
# DAG definition
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["example"],
) as dag:
    
    def extract():
        return {"data": [1, 2, 3]}
    
    def transform(**context):
        data = context["ti"].xcom_pull(task_ids="extract_task")
        return [x * 2 for x in data["data"]]
    
    extract_task = PythonOperator(task_id="extract_task", python_callable=extract)
    transform_task = PythonOperator(task_id="transform_task", python_callable=transform)
    
    extract_task >> transform_task
```

---

## Airflow Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                     AIRFLOW ARCHITECTURE                        │
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────────┐  │
│  │   Webserver  │     │  Scheduler   │     │    Executor    │  │
│  │  (Flask/FAB) │     │  (Scheduler  │     │ (Local/Celery/ │  │
│  │  Port: 8080  │     │   Job loop)  │     │  Kubernetes)   │  │
│  └──────┬───────┘     └──────┬───────┘     └───────┬────────┘  │
│         │                    │                      │           │
│         └────────────────────┼──────────────────────┘           │
│                              │                                  │
│                    ┌─────────▼──────────┐                       │
│                    │   Metadata DB      │                       │
│                    │  (PostgreSQL/MySQL) │                       │
│                    │  DAG state, XCom,  │                       │
│                    │  connections, vars │                       │
│                    └────────────────────┘                       │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    DAG Files (Python)                     │  │
│  │                   /opt/airflow/dags/                      │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Scheduler
- Reads DAG files, determines which tasks need to run
- Creates **DagRun** and **TaskInstance** records in DB
- Submits ready tasks to the Executor
- Runs as a persistent daemon process
- Since Airflow 2.0: supports **multiple active schedulers** (HA)

### Executor
- Decides **how** and **where** tasks run
- Types: SequentialExecutor, LocalExecutor, CeleryExecutor, KubernetesExecutor
- Not a separate process — runs inside the scheduler process (except Celery/K8s workers)

### Metadata Database
- **PostgreSQL** (recommended), MySQL, SQLite (dev only)
- Stores: DAG definitions, run history, task states, XCom values, connections, variables, logs
- The single source of truth for the entire system

### Webserver
- Flask + Flask-AppBuilder (FAB)
- Provides UI for monitoring, triggering, debugging DAGs
- Reads state from metadata DB — does NOT parse DAG files directly (uses serialized DAGs since 2.0)
- Port 8080 by default

### Workers
- Execute the actual task code
- Only relevant for CeleryExecutor and KubernetesExecutor
- For LocalExecutor: tasks run as subprocesses of the scheduler

---

## Airflow vs Cron Jobs

| Feature | Cron | Airflow |
|---------|------|---------|
| Dependencies | None | Full DAG dependency management |
| Retry logic | None | Built-in, configurable |
| Monitoring | None | Rich UI, logs, alerts |
| Parameterization | Limited | Full Python + Jinja templating |
| Backfill | Manual | Built-in catchup/backfill |
| Visibility | None | Gantt charts, task duration, history |
| Dynamic workflows | No | Yes (Python code) |
| Distributed execution | No | Yes (Celery, Kubernetes) |
| Community/providers | None | 70+ providers |

---

## Airflow vs Luigi

| Feature | Airflow | Luigi |
|---------|---------|-------|
| Creator | Airbnb | Spotify |
| Focus | Scheduling + monitoring | Task dependency |
| UI | Rich, built-in | Basic |
| Scheduling | Built-in | External (cron) |
| Community | Large, Apache | Smaller |
| Operators | 200+ built-in | Limited |
| Dynamic DAGs | Yes | Limited |

---

## Airflow vs Prefect

| Feature | Airflow | Prefect |
|---------|---------|---------|
| DAG definition | DAG + tasks explicit | Flow + tasks (more Pythonic) |
| Dynamic tasks | Task mapping | Native dynamic flows |
| Deployment | Self-hosted or managed | Prefect Cloud (SaaS) |
| Error handling | Retry params | Built-in, more flexible |
| Scheduling | Cron-based + timetables | Deployments + schedules |
| Local testing | Complex setup | Easier |
| Community | Largest | Growing |

---

## Airflow vs Dagster

| Feature | Airflow | Dagster |
|---------|---------|---------|
| Abstraction | Tasks in DAGs | Assets as first-class citizens |
| Type checking | None built-in | Strong typing |
| Testing | Manual | Built-in testing framework |
| Data lineage | Via plugins/OpenLineage | Native |
| Asset-based scheduling | Datasets (2.4+) | Core concept |
| Learning curve | Medium | Steeper |

---

## Event-driven vs Schedule-driven Pipelines

**Schedule-driven**: Run at fixed time intervals (cron, timedelta)
```python
schedule_interval="0 6 * * *"  # every day at 6am
```

**Event-driven**: Triggered by external events (data arrival, API webhook, another DAG completing)
```python
# Dataset-based (Airflow 2.4+)
from airflow.datasets import Dataset

my_dataset = Dataset("s3://bucket/data/")

# Triggered when upstream DAG writes to this dataset
@dag(schedule=[my_dataset])
def downstream_dag(): ...
```

---

## Airflow Terminology Glossary

| Term | Definition |
|------|-----------|
| **DAG** | Directed Acyclic Graph — defines the workflow |
| **Task** | A single unit of work within a DAG |
| **Operator** | Template/class that defines what a task does |
| **Task Instance (TI)** | A specific run of a task (DAG + task_id + execution_date) |
| **DAG Run** | A specific execution of a DAG at a given logical date |
| **Logical Date** | The data interval start (formerly execution_date) |
| **Data Interval** | The time window of data this run processes |
| **Schedule Interval** | How often the DAG runs (cron, timedelta, preset) |
| **Executor** | Component that runs tasks (Local, Celery, K8s) |
| **Operator** | Defines a type of task (Python, Bash, SQL, etc.) |
| **Hook** | Interface to an external service (DB, S3, API) |
| **Connection** | Stored credentials for an external system |
| **Variable** | Key-value configuration stored in metadata DB |
| **XCom** | Cross-communication: small data passed between tasks |
| **Pool** | Resource limit group for tasks |
| **Queue** | Routes tasks to specific workers (Celery) |
| **Trigger Rule** | Condition for when a task runs (all_success, one_failed, etc.) |
| **SLA** | Service Level Agreement — max time a task should take |
| **Catchup** | Run all missed DAG runs since start_date |
| **Backfill** | Manually trigger historical DAG runs |
| **Sensor** | Operator that waits for an external condition |
| **Provider** | Package extending Airflow with operators/hooks for a system |
| **Serialized DAG** | JSON representation of a DAG stored in metadata DB |
| **DAG Bag** | Collection of parsed DAG objects |
| **Triggerer** | Component that handles deferrable operators asynchronously |
| **Dataset** | Logical dataset that triggers DAGs when updated |

---

## Airflow Ecosystem Overview

```
Core Airflow
    ├── 70+ Provider Packages (apache-airflow-providers-*)
    │   ├── Cloud: AWS, GCP, Azure
    │   ├── Databases: Postgres, MySQL, Snowflake, BigQuery
    │   ├── Tools: dbt, Spark, Kubernetes, Docker
    │   └── APIs: HTTP, Slack, PagerDuty
    │
    ├── Managed Services
    │   ├── Amazon MWAA
    │   ├── Google Cloud Composer
    │   └── Astronomer (Astro)
    │
    └── Ecosystem Tools
        ├── OpenLineage (data lineage)
        ├── Great Expectations (data quality)
        ├── dbt (transformation)
        └── Cosmos (dbt + Airflow)
```
