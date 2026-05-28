# 22 — Airflow Interview Preparation

## DAG Design Questions

**Q: What is a DAG and what are the key parameters?**

**A:** A DAG (Directed Acyclic Graph) is Airflow's workflow definition — a Python file containing tasks and their dependencies. Key parameters: `dag_id` (unique name), `schedule_interval` (when to run), `start_date` (must be static), `catchup` (whether to backfill past runs), `default_args` (task defaults), `max_active_runs` (concurrent DAG run limit), `tags` (for UI filtering).

---

**Q: What is the difference between `schedule_interval` and a timetable?**

**A:** `schedule_interval` is the legacy way — accepts cron string, timedelta, or preset like `@daily`. Timetables (Airflow 2.2+) are Python classes implementing custom scheduling logic. Use timetables for business-hours-only schedules, skipping holidays, dataset-triggered runs, or any logic that can't be expressed as a cron. Under the hood, Airflow 2.x converts `schedule_interval` to a `CronDataIntervalTimetable`.

---

**Q: Why should `start_date` be static and never `datetime.now()`?**

**A:** DAG files are re-parsed every `min_file_process_interval` seconds (default 30s). If `start_date = datetime.now()`, it changes on every parse — Airflow creates a new DAG run each time it sees a new `start_date`. This causes hundreds of unexpected DAG runs. Always use a hardcoded date like `pendulum.datetime(2024, 1, 1)`.

---

**Q: What does `catchup=True` do and when should you set it to `False`?**

**A:** `catchup=True` (default) causes Airflow to run all missed DAG runs from `start_date` until now. If your `start_date` is 1 year ago with `@hourly` schedule and catchup=True, Airflow immediately creates ~8,760 DAG runs. Set `catchup=False` for: new DAGs, idempotent pipelines where historical data isn't needed, and any DAG where you'll manage backfills manually with `airflow dags backfill`.

---

**Q: What is `depends_on_past` and what problem does it solve?**

**A:** When `depends_on_past=True` on a task, that task waits for the same task in the **previous DAG run** to succeed before starting. Solves sequential data processing: if Jan 2's transform fails, Jan 3's transform won't start (preventing processing out-of-order). The downside: one failure cascades and blocks all future runs.

---

**Q: How do you design an idempotent DAG?**

**A:** 1) Use DELETE+INSERT or UPSERT instead of plain INSERT. 2) Partition data by `execution_date`/`ds` and overwrite the partition. 3) Never use sequences/autoincrement for business keys. 4) Use `{{ ds }}` as data window, not `datetime.now()`. 5) Write status to a tracking table before and after — skip if already processed. 6) Test by running the DAG twice with the same logical_date and verifying the output is identical.

---

**Q: How would you create 100 similar DAGs without code duplication?**

**A:** Use the **DAG Factory pattern**: iterate over a config list (from YAML/JSON/DB), create each DAG inside a function, and register it in `globals()`. Example:
```python
for config in load_configs():
    dag = create_dag(config)
    globals()[dag.dag_id] = dag
```
This creates all DAGs from a single Python file.

---

**Q: What are Airflow Params and how do they differ from Variables?**

**A:** Params are **per-run** configuration passed at trigger time (via UI, CLI `--conf`, or API). Variables are **global** key-value store used across DAGs. Use Params for runtime overrides of DAG behavior. Use Variables for shared configuration (like environment, API URLs) used across many DAGs. Params don't persist; Variables are stored in the DB.

---

**Q: How do you pass data between tasks in Airflow?**

**A:** Primary method: **XCom** — for small metadata (IDs, counts, file paths, status). TaskFlow API does this automatically via return values. For large data: store in external storage (S3, GCS, DB) and pass the **path/reference** via XCom. Never store DataFrames in XCom — use S3 XCom backend for that. Alternative: write to a shared database and read in downstream tasks.

---

## Scheduler Questions

**Q: How does the Airflow scheduler work internally?**

**A:** The scheduler runs two loops: (1) **DAG parsing loop** — `DagFileProcessorManager` spawns subprocesses to parse DAG files, serializes to DB. (2) **Task scheduling loop** — `SchedulerJob._run_scheduler_loop()` creates DagRuns for due schedules, moves task instances through states (scheduled → queued → running), submits ready tasks to the executor, and processes executor events. The loop runs every `scheduler_heartbeat_sec` (default 5s).

---

**Q: What is the data interval concept in Airflow?**

**A:** Each DAG run processes a **data interval** — a time window from `data_interval_start` to `data_interval_end`. For `@daily` DAGs, the interval is `[2024-01-01, 2024-01-02)`. The DAG run is triggered **at the end** of the interval (Jan 2 midnight), not the start. `logical_date` = `data_interval_start`. This replaces the old `execution_date` concept and clarifies what data the run processes.

---

**Q: Can you run multiple Airflow schedulers? How?**

**A:** Yes, since Airflow 2.0. Multiple schedulers use **optimistic locking** on the `task_instance` table to coordinate — only one scheduler "wins" the DB lock to schedule each task. Start as many schedulers as needed: `airflow scheduler` on each node. No special configuration needed. Benefit: HA (if one crashes, others continue) and throughput (multiple parsers, faster scheduling).

---

**Q: What causes scheduler lag and how do you fix it?**

**A:** Scheduler lag = delay between when a task should start and when it actually starts. Causes: (1) Too many DAG files parsing slowly — increase `parsing_processes`, move imports inside callables. (2) DB overloaded — use PgBouncer, optimize queries, regular cleanup. (3) Too many concurrent tasks — all executor slots full. (4) Slow heartbeat — reduce `scheduler_heartbeat_sec`. Monitor with `airflow.scheduler.tasks.starving` StatsD metric.

---

## Executor Questions

**Q: What is the difference between LocalExecutor, CeleryExecutor, and KubernetesExecutor?**

**A:**
- **LocalExecutor**: tasks run as subprocesses on the same machine as the scheduler. Simple, no extra infrastructure, limited to one machine's resources.
- **CeleryExecutor**: tasks sent to persistent Celery workers via Redis/RabbitMQ broker. Distributed, horizontal scaling, workers can be added/removed. Workers are always running (idle cost).
- **KubernetesExecutor**: each task gets its own K8s pod, created on demand and deleted after. Complete isolation, scales to zero when idle, but ~30s pod startup overhead.

---

**Q: When would you choose CeleryExecutor over KubernetesExecutor?**

**A:** Choose Celery when: (1) Tasks start in <5 seconds (no pod startup overhead). (2) Many short tasks (pod overhead would dominate). (3) You need worker queue routing with custom logic. (4) You're not on Kubernetes. Choose Kubernetes when: (1) Tasks need different Docker images or resource profiles. (2) Complete resource isolation between tasks. (3) You want zero idle cost (scale to zero). (4) Already on K8s infrastructure.

---

**Q: What happens when a Celery worker crashes?**

**A:** Tasks in "running" state on that worker become stuck (still show as "running" in DB but worker is gone). With `acks_late=True`, Celery will requeue unacknowledged tasks. The scheduler has health checks and can detect orphaned tasks. Recovery: manually clear stuck task instances (`airflow tasks clear`). Set `acks_late = True` in Celery config to enable automatic requeue on worker crash.

---

## XCom Questions

**Q: What are the size limitations of XCom?**

**A:** XCom values are stored in the `xcom` table in the metadata DB. The column type varies by DB: PostgreSQL uses `LargeBinary` (essentially unlimited but practically constrained by performance). Guideline: keep XCom payloads under **48KB** for safe cross-DB compatibility and good performance. Large XCom values bloat the DB, slow queries, and cause scheduler lag. Use S3/GCS XCom backends for large data.

---

**Q: How does TaskFlow API handle XCom differently from traditional operators?**

**A:** TaskFlow API makes XCom invisible. When an `@task`-decorated function returns a value, it's automatically pushed as XCom key `return_value`. When you pass the return value as an argument to another `@task`, it's automatically pulled. No explicit `ti.xcom_push()` or `ti.xcom_pull()` needed. This reduces boilerplate and makes data flow obvious from reading the code.

---

**Q: What are XCom anti-patterns?**

**A:** (1) Storing large DataFrames/files — causes DB bloat. (2) Storing binary data — exceeds column limits. (3) Using XCom for cross-DAG communication — fragile, use Datasets instead. (4) Calling `Variable.get()` or `xcom_pull()` at DAG module level — runs on every parse. (5) Storing secrets in XCom — logged and stored in DB unencrypted unless Fernet key is set.

---

## Sensor Questions

**Q: What is the difference between poke and reschedule mode in sensors?**

**A:** **poke mode**: sensor holds a worker slot while sleeping between checks. Simple but wasteful for long waits — 100 poke-mode sensors occupy 100 worker slots. **reschedule mode**: sensor checks the condition, and if not met, the task is moved to `up_for_reschedule` state and the worker slot is freed. Next check is scheduled based on `poke_interval`. Much more efficient for long waits. Use `mode="reschedule"` for anything expected to wait more than a few minutes.

---

**Q: What problem do deferrable sensors solve?**

**A:** Even reschedule-mode sensors write to the DB and wake up every `poke_interval` — with 1000 sensors pinging every 60s, that's 16 DB writes/sec. Deferrable sensors use the **Triggerer** — an asyncio process that runs triggers as coroutines. Zero worker slots, zero DB writes while waiting (just one asyncio coroutine per trigger). Can handle thousands of concurrent sensors efficiently. Requires `airflow triggerer` process to be running.

---

**Q: What happens if a sensor times out?**

**A:** The sensor task transitions to `failed` state with `AirflowSensorTimeout` exception. Downstream tasks are affected based on their `trigger_rule`. If `soft_fail=True`, the task transitions to `skipped` instead of `failed` — less disruptive but downstream tasks with `all_success` trigger rule also get skipped. Always set `timeout` on sensors — without it, a sensor can run for days waiting for a condition that will never come.

---

## Security Questions

**Q: What is a Fernet key and what does it encrypt?**

**A:** A Fernet key is a symmetric encryption key used by Airflow to encrypt passwords in the `connection` table and values in the `variable` table when stored in the metadata DB. Without it, credentials are stored in plaintext. Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. Set via `AIRFLOW__CORE__FERNET_KEY`. Rotate with `airflow rotate-fernet-key`.

---

**Q: How would you integrate Airflow with HashiCorp Vault?**

**A:** Configure the secrets backend in `airflow.cfg` or env vars:
```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"url": "http://vault:8200", "token": "...", "connections_path": "airflow/connections", "variables_path": "airflow/variables"}
```
Store connections as `vault kv put secret/airflow/connections/my_conn conn_uri="postgresql://..."`. Airflow resolves connections by checking Vault before the metadata DB. Use AppRole or Kubernetes auth instead of tokens in production.

---

## Performance Tuning Questions

**Q: What is the difference between `parallelism` and `max_active_tasks_per_dag`?**

**A:** `parallelism` is the **global ceiling** — total concurrent task instances across all DAGs. `max_active_tasks_per_dag` is a **per-DAG ceiling** — max concurrent task instances within a single DAG. A task must satisfy both limits to run. Example: `parallelism=32`, `max_active_tasks_per_dag=8` — maximum 32 total tasks, but any single DAG can run at most 8 simultaneously.

---

**Q: What are Airflow Pools and how do you use them?**

**A:** Pools are named resource slots that limit concurrency for tasks accessing shared resources. Example: if DB can handle 10 concurrent connections, create a pool with 10 slots. Assign tasks to the pool — once all 10 slots are taken, other tasks queue. Create with `airflow pools set my_pool 10 "description"`. Assign with `pool="my_pool"` on the operator. Tasks can occupy multiple slots with `pool_slots=2` for heavier operations.

---

**Q: How do you optimize DAG parsing performance?**

**A:** (1) Move heavy imports (`pandas`, `spark`) inside task callables — not at module level. (2) Move `Variable.get()` and DB queries inside callables. (3) Increase `parsing_processes` to match CPU count. (4) Increase `min_file_process_interval` (30-60s). (5) Use `dagbag_import_timeout` to kill slow parsing files. (6) Split very large DAG files. (7) Use YAML-based DAG factories instead of complex Python.

---

## Scenario Questions

**Q: Your DAG is stuck with tasks in "scheduled" state but not running. What do you check?**

**A:** 1) Check if the executor has available slots: `airflow.executor.open_slots` metric — if 0, all slots are taken. 2) Check pool: `airflow pools list` — if `open_slots=0`, pool is full. 3) Check if workers are running: `airflow celery flower` or `kubectl get pods`. 4) Check scheduler heartbeat: is it running? `airflow jobs check --job-type SchedulerJob`. 5) Check task dependencies: does it have `depends_on_past=True` with a failed predecessor?

---

**Q: Tasks are queuing but workers are idle. What's wrong?**

**A:** For CeleryExecutor: 1) Worker and scheduler connected to **different queues** — check `--queues` argument. 2) Broker (Redis/RabbitMQ) is down or not accessible from workers. 3) Task's `queue` attribute doesn't match worker's queue. 4) Celery worker has `worker_concurrency=0`. Check with `celery -A airflow.executors.celery_executor.app inspect active`.

---

**Q: You need to reprocess 3 months of data. How do you approach it?**

**A:** 1) Ensure DAG is idempotent (UPSERT, not INSERT). 2) Set `catchup=True` if not already, or use `airflow dags backfill --start-date 2024-01-01 --end-date 2024-03-31 my_dag`. 3) Consider `--reset-dagruns` flag if runs already exist. 4) Set appropriate `max_active_runs` to prevent overwhelming resources (e.g., 3-5 concurrent runs). 5) Monitor DB load during backfill. 6) After completion, verify row counts match expected.

---

**Q: How would you implement a circuit breaker in Airflow?**

**A:** Use a ShortCircuitOperator that checks health metrics before continuing:
```python
def check_upstream_health(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="target_db")
    # Check if target DB has capacity
    active_connections = hook.get_first("SELECT COUNT(*) FROM pg_stat_activity")[0]
    return active_connections < 80  # circuit open if DB is overwhelmed

circuit = ShortCircuitOperator(
    task_id="health_check",
    python_callable=check_upstream_health,
)
circuit >> actual_work
```

---

**Q: Task shows "no logs" in UI — what do you check?**

**A:** 1) Verify task actually ran — check `task_instance.state` and `start_date` in DB. 2) Check if log file exists: `$AIRFLOW_HOME/logs/dag_id=X/run_id=Y/task_id=Z/attempt=1.log`. 3) If using remote logging, check S3/GCS bucket. 4) Check webserver can reach the worker to fetch logs (network/firewall). 5) Check log file permissions. 6) For KubernetesExecutor: pod may have been deleted before logs were fetched — ensure `get_logs=True` and `is_delete_operator_pod=False` temporarily.

---

**Q: DAG not appearing in UI after adding it — why?**

**A:** 1) File parsing hasn't triggered yet — wait for `min_file_process_interval`. 2) Import error in DAG file: check `airflow dags list-import-errors`. 3) File not in `dags_folder`: verify `AIRFLOW__CORE__DAGS_FOLDER`. 4) DAG is paused: check `dag.is_paused`. 5) Scheduler not running: no parsing = no new DAGs. 6) File doesn't contain a `DAG` object in `globals()` — with `@dag` decorator, must instantiate the dag function at module level.

---

**Q: How do you deploy DAG changes with zero downtime?**

**A:** 1) DAG file changes are picked up automatically on next parse cycle — no restart needed. 2) For structural changes (adding/removing tasks), in-progress runs continue with the old structure (Airflow tracks run-level task set). 3) Use DAG versioning (`dag_id` includes version: `my_dag_v2`). 4) For breaking changes: pause the old DAG, deploy new one, migrate in-flight runs manually. 5) CI/CD pipeline: validate → test → push to S3/GCS/git-sync → auto-deployed to all schedulers.
