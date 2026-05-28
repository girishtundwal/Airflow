# Apache Airflow — Complete Data Engineering Reference

> Single-repo reference covering Apache Airflow from foundations to production patterns.
> Interview-ready, with code examples and diagrams throughout.

---

## Quick Navigation

| # | File | Description |
|---|------|-------------|
| 00 | [Foundations](00_foundations.md) | Core concepts, DAG basics, Airflow vs alternatives, terminology |
| 01 | [Architecture](01_architecture.md) | Internal components, state machine, Celery/K8s architecture, HA |
| 02 | [Installation & Setup](02_installation.md) | pip, Docker Compose, Helm, database setup, config |
| 03 | [DAG Development](03_dag_development.md) | DAG params, scheduling, catchup, Jinja, Variables, Macros, Params |
| 04 | [Operators](04_operators.md) | All core operators with full code examples, custom operators |
| 05 | [TaskFlow API](05_taskflow_api.md) | @dag/@task decorators, expand(), partial(), task groups |
| 06 | [Scheduling](06_scheduling.md) | Cron, timetables, data intervals, datasets, trigger rules, SLA |
| 07 | [Executors](07_executors.md) | Sequential/Local/Celery/Kubernetes executor comparison + config |
| 08 | [Metadata Database](08_metadata_database.md) | DB schema, ORM queries, cleanup, performance, HA |
| 09 | [XCom](09_xcom.md) | push/pull, TaskFlow auto-XCom, custom S3 backend, anti-patterns |
| 10 | [Sensors](10_sensors.md) | poke/reschedule/deferrable modes, all sensor types, custom sensors |
| 11 | [Hooks & Connections](11_hooks_connections.md) | PostgresHook, S3Hook, custom hooks, secrets backends |
| 12 | [Variables & Secrets](12_variables_secrets.md) | Variables, Fernet, Vault/AWS SM/GCP SM integration, Params |
| 13 | [Logging & Monitoring](13_logging_monitoring.md) | Remote logging, StatsD, Prometheus, Grafana, alerts, SLA |
| 14 | [Performance](14_performance.md) | Scheduler tuning, parsing optimization, pools, queues, deferrable |
| 15 | [Security](15_security.md) | RBAC, LDAP/OAuth, Fernet, TLS, Kubernetes security |
| 16 | [CI/CD & DevOps](16_cicd_devops.md) | Testing, GitHub Actions, Dockerfile, Helm, GitOps/ArgoCD |
| 17 | [Kubernetes](17_kubernetes.md) | KubernetesExecutor, KubernetesPodOperator, Helm, KEDA, EKS/GKE |
| 18 | [Managed Airflow](18_managed_airflow.md) | MWAA, Cloud Composer, Astro Cloud — setup, limits, cost |
| 19 | [Integrations](19_integrations.md) | Spark, Databricks, dbt, Snowflake, BigQuery, MLflow, Kafka |
| 20 | [Advanced Concepts](20_advanced_concepts.md) | DAG serialization, Triggerer, deferrable ops, REST API, HA |
| 21 | [Design Patterns](21_design_patterns.md) | Idempotency, incremental, DAG factory, fan-out/in, retry patterns |
| 22 | [Interview Prep](22_interview_prep.md) | 50+ Q&A covering all topics: scheduler, executor, XCom, security |
| 23 | [Real-world Patterns](23_realworld_patterns.md) | Lakehouse pipeline, ML pipeline, data quality, event-driven DAGs |
| 24 | [Astro & Ecosystem](24_astro_ecosystem.md) | Astro CLI, Cosmos/dbt, Airbyte, Great Expectations, OpenLineage |
| 25 | [Labs & Projects](25_labs_projects.md) | 8 complete runnable labs: ETL, incremental, factory, CI/CD, sensors |

---

## CLI Cheat Sheet

```bash
# ─── DAGs ──────────────────────────────────────────────────────
airflow dags list                                    # list all DAGs
airflow dags list-import-errors                      # show parse errors
airflow dags trigger my_dag --conf '{"key":"val"}'   # manual trigger
airflow dags pause my_dag                            # pause scheduling
airflow dags unpause my_dag
airflow dags backfill my_dag -s 2024-01-01 -e 2024-01-31
airflow dags show my_dag                             # print DAG structure
airflow dags test my_dag 2024-01-15                  # dry run

# ─── Tasks ─────────────────────────────────────────────────────
airflow tasks list my_dag
airflow tasks test my_dag task_id 2024-01-15        # run task (no metadata)
airflow tasks clear my_dag -s 2024-01-01 -e 2024-01-01
airflow tasks states-for-dag-run my_dag run_id
airflow tasks run my_dag task_id run_id             # run with metadata DB

# ─── Database ──────────────────────────────────────────────────
airflow db init                                     # first-time setup
airflow db upgrade                                  # after Airflow upgrade
airflow db check                                    # test connection
airflow db clean --clean-before-timestamp 2024-01-01T00:00:00 --yes

# ─── Users & Roles ─────────────────────────────────────────────
airflow users create --username admin --role Admin --firstname A --lastname B --email a@b.com --password pass
airflow users list
airflow roles list
airflow roles create my_role

# ─── Connections ───────────────────────────────────────────────
airflow connections list
airflow connections add my_conn --conn-type postgres --conn-host localhost --conn-port 5432
airflow connections delete my_conn
airflow connections export connections.json

# ─── Variables ─────────────────────────────────────────────────
airflow variables list
airflow variables set key value
airflow variables get key
airflow variables delete key
airflow variables export variables.json
airflow variables import variables.json

# ─── Pools ─────────────────────────────────────────────────────
airflow pools list
airflow pools set my_pool 10 "description"
airflow pools get my_pool
airflow pools delete my_pool

# ─── Celery ────────────────────────────────────────────────────
airflow celery worker --queues default,high_priority
airflow celery flower                                # monitoring UI :5555

# ─── Health ────────────────────────────────────────────────────
airflow jobs check --job-type SchedulerJob
airflow info
```

---

## Key Configuration Variables

| Config Key (AIRFLOW__SECTION__KEY) | Default | Description |
|-----------------------------------|---------|-------------|
| `CORE__EXECUTOR` | SequentialExecutor | Executor type |
| `CORE__FERNET_KEY` | (empty) | Encryption key for DB credentials |
| `CORE__DAGS_FOLDER` | ~/airflow/dags | Where to scan for DAG files |
| `CORE__LOAD_EXAMPLES` | True | Load example DAGs (set False in prod) |
| `CORE__PARALLELISM` | 32 | Global max concurrent tasks |
| `CORE__MAX_ACTIVE_TASKS_PER_DAG` | 16 | Per-DAG task concurrency |
| `CORE__MAX_ACTIVE_RUNS_PER_DAG` | 16 | Per-DAG run concurrency |
| `DATABASE__SQL_ALCHEMY_CONN` | sqlite:///... | Metadata DB connection string |
| `SCHEDULER__SCHEDULER_HEARTBEAT_SEC` | 5 | Scheduler loop interval |
| `SCHEDULER__MIN_FILE_PROCESS_INTERVAL` | 30 | DAG file re-parse interval |
| `SCHEDULER__PARSING_PROCESSES` | 2 | Parallel DAG file parsers |
| `WEBSERVER__WEB_SERVER_PORT` | 8080 | Webserver port |
| `WEBSERVER__SECRET_KEY` | (required) | Session signing key |
| `CELERY__BROKER_URL` | (required) | Redis/RabbitMQ URL |
| `CELERY__RESULT_BACKEND` | (required) | Result backend URL |
| `CELERY__WORKER_CONCURRENCY` | 16 | Tasks per Celery worker |

---

## Task States Quick Reference

| State | Icon | Description |
|-------|------|-------------|
| `none` | ⬜ | Not yet created |
| `scheduled` | 🟡 | Waiting for executor slot |
| `queued` | 🟠 | Sent to executor, not yet running |
| `running` | 🟢 | Actively executing |
| `success` | ✅ | Completed successfully |
| `failed` | ❌ | Raised an exception |
| `up_for_retry` | 🔄 | Failed, retries remaining |
| `up_for_reschedule` | ⏳ | Sensor released worker, waiting for next poke |
| `skipped` | ⏭️ | Branch not taken or ShortCircuit blocked |
| `deferred` | ⏸️ | Deferrable operator waiting on Triggerer |
| `removed` | 🗑️ | Task removed from DAG while run was active |
| `upstream_failed` | 🔺 | Upstream task failed, trigger rule blocked |

---

## Trigger Rules Reference

| Rule | Run when... |
|------|------------|
| `all_success` | All upstreams succeeded *(default)* |
| `all_failed` | All upstreams failed |
| `all_done` | All upstreams done (any state) |
| `all_skipped` | All upstreams skipped |
| `one_success` | At least one upstream succeeded |
| `one_failed` | At least one upstream failed |
| `one_done` | At least one upstream done |
| `none_failed` | No upstream failed (success/skip OK) |
| `none_failed_min_one_success` | No failures AND at least one success |
| `none_skipped` | No upstream skipped |
| `always` | Regardless of upstream state |

---

## Cron Presets

| Preset | Equivalent | Runs at |
|--------|-----------|---------|
| `@once` | — | One time only |
| `@hourly` | `0 * * * *` | :00 every hour |
| `@daily` | `0 0 * * *` | Midnight UTC |
| `@weekly` | `0 0 * * 0` | Sunday midnight |
| `@monthly` | `0 0 1 * *` | 1st of month |
| `@yearly` | `0 0 1 1 *` | January 1st |
| `None` | — | Manual trigger only |

---

## Common Template Variables

| Template | Value | Example |
|----------|-------|---------|
| `{{ ds }}` | Data interval start date | `2024-01-15` |
| `{{ ds_nodash }}` | Date without dashes | `20240115` |
| `{{ ts }}` | ISO timestamp | `2024-01-15T00:00:00+00:00` |
| `{{ next_ds }}` | Data interval end date | `2024-01-16` |
| `{{ prev_ds }}` | Previous interval start | `2024-01-14` |
| `{{ logical_date }}` | Pendulum datetime object | `DateTime(2024, 1, 15...)` |
| `{{ data_interval_start }}` | Interval start datetime | — |
| `{{ data_interval_end }}` | Interval end datetime | — |
| `{{ run_id }}` | DAG run identifier | `scheduled__2024-01-15...` |
| `{{ dag.dag_id }}` | Current DAG ID | `my_dag` |
| `{{ dag_run.conf }}` | Trigger config dict | `{"env": "prod"}` |
| `{{ var.value.KEY }}` | Airflow Variable | `prod` |
| `{{ var.json.KEY.field }}` | JSON Variable field | `1000` |
| `{{ conn.CONN.host }}` | Connection attribute | `db.internal` |
| `{{ macros.ds_add(ds, 7) }}` | Date math | `2024-01-22` |
| `{{ ti.xcom_pull(task_ids='X') }}` | Pull XCom | — |

---

## Most-used Operators Quick Reference

| Operator | Provider | Use case |
|----------|----------|---------|
| `PythonOperator` | core | Run any Python function |
| `BashOperator` | core | Run shell commands |
| `@task` decorator | core | TaskFlow API Python tasks |
| `EmptyOperator` | core | Structural markers, join points |
| `BranchPythonOperator` | core | Conditional workflow branching |
| `ShortCircuitOperator` | core | Skip downstream if condition False |
| `PostgresOperator` | postgres | Execute SQL against PostgreSQL |
| `SnowflakeOperator` | snowflake | Execute SQL in Snowflake |
| `BigQueryInsertJobOperator` | google | Run BigQuery SQL |
| `S3KeySensor` | amazon | Wait for S3 object |
| `FileSensor` | core | Wait for local file |
| `ExternalTaskSensor` | core | Wait for task in another DAG |
| `TriggerDagRunOperator` | core | Trigger another DAG |
| `KubernetesPodOperator` | cncf | Run any container as task |
| `SparkSubmitOperator` | apache.spark | Submit Spark job |
| `DatabricksRunNowOperator` | databricks | Trigger Databricks job |
| `DbtCloudRunJobOperator` | dbt | Run dbt Cloud job |

---

## Learning Path

### Beginner (New to Airflow)
1. [00 — Foundations](00_foundations.md) — understand core concepts
2. [02 — Installation](02_installation.md) — get Airflow running locally
3. [03 — DAG Development](03_dag_development.md) — write your first DAG
4. [04 — Operators](04_operators.md) — learn common operators
5. [06 — Scheduling](06_scheduling.md) — understand data intervals
6. [25 — Labs](25_labs_projects.md) — Lab 1 & Lab 2

### Intermediate (Writing Production DAGs)
1. [05 — TaskFlow API](05_taskflow_api.md) — modern Airflow patterns
2. [07 — Executors](07_executors.md) — understand Celery vs K8s
3. [09 — XCom](09_xcom.md) — inter-task communication
4. [10 — Sensors](10_sensors.md) — event-driven pipelines
5. [11 — Hooks & Connections](11_hooks_connections.md) — integrate external systems
6. [12 — Variables & Secrets](12_variables_secrets.md) — configuration management
7. [21 — Design Patterns](21_design_patterns.md) — production patterns

### Advanced / Production
1. [01 — Architecture](01_architecture.md) — deep internals
2. [08 — Metadata Database](08_metadata_database.md) — DB schema and maintenance
3. [13 — Logging & Monitoring](13_logging_monitoring.md) — observability stack
4. [14 — Performance](14_performance.md) — tuning and scaling
5. [15 — Security](15_security.md) — hardening for production
6. [16 — CI/CD](16_cicd_devops.md) — deployment automation
7. [17 — Kubernetes](17_kubernetes.md) — K8s-native deployment
8. [20 — Advanced Concepts](20_advanced_concepts.md) — internals and REST API
9. [22 — Interview Prep](22_interview_prep.md) — scenario questions
