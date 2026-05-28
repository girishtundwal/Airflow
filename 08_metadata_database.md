# 08 — Airflow Metadata Database

## Overview

The Metadata Database is the **single source of truth** for Airflow's state. Everything — DAG runs, task states, XCom, connections, variables — lives here.

| Database | Support Level | Use Case |
|----------|--------------|---------|
| PostgreSQL | Recommended | Production |
| MySQL | Supported | Production |
| SQLite | Dev only | Local testing, no parallelism |

---

## Key Tables

```
┌──────────────────────────────────────────────────────────────────┐
│                     METADATA DB SCHEMA                           │
│                                                                  │
│  dag ──────────────────────── dag_run ──────── task_instance     │
│  (dag_id, is_paused,          (dag_id,          (dag_id,         │
│   schedule_interval,           run_id,           task_id,        │
│   fileloc, is_active)          state,            run_id,         │
│                                execution_date,   state,          │
│                                conf)             try_number,     │
│                                                  hostname)       │
│                                      │                │          │
│                                      │         xcom ──┘          │
│                                      │         (key, value,      │
│                                      │          timestamp)       │
│                                      │                           │
│  connection                          │  variable                 │
│  (conn_id, conn_type,        log ────┘  (key, val,               │
│   host, port, login,         (event,     is_encrypted)           │
│   password, schema, extra)    dttm)                              │
│                                                                  │
│  slot_pool                    job                                │
│  (pool, slots,                (job_type, state,                  │
│   description)                latest_heartbeat)                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## Table Deep Dives

### `dag` table
```sql
-- Key columns
SELECT dag_id, is_paused, is_active, schedule_interval,
       fileloc, last_parsed_time, last_pickled
FROM dag;

-- Pause a DAG
UPDATE dag SET is_paused = TRUE WHERE dag_id = 'my_dag';
```

### `dag_run` table
```sql
-- Check DAG run states
SELECT dag_id, run_id, state, execution_date, start_date, end_date,
       run_type, conf
FROM dag_run
WHERE dag_id = 'my_dag'
ORDER BY execution_date DESC
LIMIT 10;

-- Run types: 'scheduled', 'manual', 'backfill', 'dataset_triggered'
```

### `task_instance` table
```sql
-- Find stuck tasks
SELECT dag_id, task_id, run_id, state, start_date, hostname,
       try_number, max_tries
FROM task_instance
WHERE state IN ('running', 'queued')
  AND start_date < NOW() - INTERVAL '1 hour';

-- Find recent failures
SELECT dag_id, task_id, run_id, state, end_date
FROM task_instance
WHERE state = 'failed'
ORDER BY end_date DESC
LIMIT 20;

-- Task execution stats
SELECT task_id,
       COUNT(*) as total_runs,
       SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successes,
       SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failures,
       AVG(EXTRACT(EPOCH FROM (end_date - start_date))) as avg_duration_sec
FROM task_instance
WHERE dag_id = 'my_dag'
GROUP BY task_id;
```

### `xcom` table
```sql
-- View XCom values
SELECT dag_id, task_id, run_id, key, value, timestamp
FROM xcom
WHERE dag_id = 'my_dag'
ORDER BY timestamp DESC;

-- XCom size check (large values are a problem)
SELECT dag_id, task_id, key,
       LENGTH(value::text) as value_bytes
FROM xcom
ORDER BY LENGTH(value::text) DESC
LIMIT 10;
```

### `connection` table
```sql
-- List connections (passwords are Fernet-encrypted if key is set)
SELECT conn_id, conn_type, host, port, schema, login
FROM connection;
```

### `variable` table
```sql
-- List variables
SELECT key, val, is_encrypted
FROM variable;
```

---

## Querying with Airflow ORM (SQLAlchemy)

```python
from airflow.utils.session import create_session
from airflow.models import DagRun, TaskInstance, DagModel

# Use create_session context manager
with create_session() as session:
    # Get last 10 failed runs for a DAG
    failed_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "my_dag",
            DagRun.state == "failed",
        )
        .order_by(DagRun.execution_date.desc())
        .limit(10)
        .all()
    )
    
    for run in failed_runs:
        print(f"{run.execution_date}: {run.run_id}")

# Get task instance state
with create_session() as session:
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == "my_dag",
            TaskInstance.task_id == "transform",
            TaskInstance.run_id == "manual__2024-01-15",
        )
        .first()
    )
    print(f"State: {ti.state}, Duration: {ti.duration}")
```

---

## Alembic Migrations

```bash
# Initialize DB (first time)
airflow db init

# Upgrade to latest version (after Airflow upgrade)
airflow db upgrade

# Check current DB version
airflow db check-migrations

# Downgrade (use carefully)
airflow db downgrade --to-revision abc123

# Show migration history
airflow db shell
# Then: SELECT * FROM alembic_version;
```

---

## Metadata Cleanup

```bash
# Clean old DAG runs, task instances, XCom (keeps last 30 days)
airflow db clean --clean-before-timestamp "2024-01-01T00:00:00" --yes

# Clean specific tables
airflow db clean \
    --clean-before-timestamp "2024-01-01T00:00:00" \
    --tables dag_run,task_instance,xcom,log \
    --yes

# Dry run to see what would be deleted
airflow db clean \
    --clean-before-timestamp "2024-01-01T00:00:00" \
    --dry-run
```

```python
# Programmatic cleanup in a maintenance DAG
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.models import XCom
import pendulum

def cleanup_xcom(**context):
    cutoff = pendulum.now("UTC").subtract(days=7)
    with create_session() as session:
        session.query(XCom).filter(XCom.timestamp < cutoff).delete()
        session.commit()
```

---

## Database Scaling

### Connection Pooling with PgBouncer

```ini
# Without PgBouncer: each Airflow component opens its own connections
# With PgBouncer: connections are pooled and reused

# airflow.cfg
[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:pass@pgbouncer:6432/airflow
sql_alchemy_pool_size = 5
sql_alchemy_max_overflow = 10
sql_alchemy_pool_timeout = 30
sql_alchemy_pool_recycle = 1800
```

### PostgreSQL Tuning

```sql
-- Add indexes for common queries (may already exist)
CREATE INDEX CONCURRENTLY idx_ti_dag_state 
    ON task_instance (dag_id, state, execution_date);

CREATE INDEX CONCURRENTLY idx_dagrun_dag_state 
    ON dag_run (dag_id, state);

-- Vacuum and analyze regularly
VACUUM ANALYZE task_instance;
VACUUM ANALYZE dag_run;
VACUUM ANALYZE xcom;

-- Check table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;
```

---

## Backup & Recovery

```bash
# PostgreSQL backup
pg_dump -U airflow -h localhost airflow > airflow_backup_$(date +%Y%m%d).sql

# Restore
psql -U airflow -h localhost airflow < airflow_backup_20240115.sql

# Point-in-time recovery: enable WAL archiving in PostgreSQL
# wal_level = replica
# archive_mode = on
# archive_command = 'cp %p /backup/wal/%f'
```

---

## Useful Diagnostic Queries

```sql
-- DAG success rate last 30 days
SELECT dag_id,
       COUNT(*) as total,
       SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as success,
       ROUND(100.0 * SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_pct
FROM dag_run
WHERE execution_date > NOW() - INTERVAL '30 days'
GROUP BY dag_id
ORDER BY success_pct;

-- Slowest tasks (average duration)
SELECT dag_id, task_id,
       COUNT(*) as runs,
       ROUND(AVG(EXTRACT(EPOCH FROM (end_date - start_date)))) as avg_sec,
       ROUND(MAX(EXTRACT(EPOCH FROM (end_date - start_date)))) as max_sec
FROM task_instance
WHERE state = 'success'
  AND start_date IS NOT NULL
  AND end_date IS NOT NULL
GROUP BY dag_id, task_id
ORDER BY avg_sec DESC
LIMIT 20;

-- Tasks currently running longer than 1 hour
SELECT dag_id, task_id, run_id, start_date, hostname,
       EXTRACT(EPOCH FROM (NOW() - start_date)) / 3600 as hours_running
FROM task_instance
WHERE state = 'running'
  AND start_date < NOW() - INTERVAL '1 hour'
ORDER BY start_date;

-- XCom storage usage by DAG
SELECT dag_id,
       COUNT(*) as xcom_count,
       SUM(octet_length(value)) / 1024 as total_kb
FROM xcom
GROUP BY dag_id
ORDER BY total_kb DESC;
```
