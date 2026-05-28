# 07 — Airflow Executors

## Executor Overview

An **executor** determines how and where tasks run. It's configured in `airflow.cfg` under `[core] executor` and runs inside the scheduler process (except for external workers).

```ini
[core]
executor = CeleryExecutor
# Options: SequentialExecutor, LocalExecutor, CeleryExecutor,
#          KubernetesExecutor, CeleryKubernetesExecutor
```

---

## Sequential Executor

```
Scheduler ──► [Task 1] → [Task 2] → [Task 3]
                (one at a time, in sequence)
```

- **Runs tasks serially** — no parallelism
- **SQLite only** — required by design
- Uses subprocess to run each task
- Default executor for fresh installs
- **Development/testing only**, never production

```ini
[core]
executor = SequentialExecutor
sql_alchemy_conn = sqlite:///airflow.db
```

---

## Local Executor

```
Scheduler ──► subprocess [Task A]
           ├─► subprocess [Task B]
           └─► subprocess [Task C]
```

- Tasks run as **subprocess of the scheduler** on the same machine
- **Requires PostgreSQL or MySQL** (SQLite doesn't support concurrent writes)
- Unlimited parallelism (limited by machine CPU/memory)
- **Good for small/medium workloads** on a single machine
- No separate worker processes needed

```ini
[core]
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
parallelism = 32        # max concurrent tasks
```

---

## Celery Executor

Full distributed execution with a message broker.

```
┌────────────┐  send task  ┌──────────────────────┐
│ Scheduler  │────────────►│   Message Broker     │
│            │             │  Redis or RabbitMQ   │
└────────────┘             └──────────┬───────────┘
                                      │ consume task
                         ┌────────────┼────────────┐
                         ▼            ▼             ▼
                   ┌──────────┐ ┌──────────┐ ┌──────────┐
                   │Worker 1  │ │Worker 2  │ │Worker 3  │
                   │(celery)  │ │(celery)  │ │(celery)  │
                   └────┬─────┘ └────┬─────┘ └────┬─────┘
                        │            │             │
                        └────────────┼─────────────┘
                                     │ results
                                     ▼
                           ┌──────────────────┐
                           │  Result Backend  │
                           │  Redis / DB      │
                           └────────┬─────────┘
                                    │ state updates
                                    ▼
                           ┌──────────────────┐
                           │  Metadata DB     │
                           └──────────────────┘
```

```ini
[core]
executor = CeleryExecutor

[celery]
broker_url = redis://localhost:6379/0
# Or RabbitMQ: amqp://user:pass@host:5672/vhost
result_backend = db+postgresql://airflow:airflow@localhost/airflow
worker_concurrency = 16      # tasks per worker process
worker_prefetch_multiplier = 1  # don't pre-fetch (better task distribution)
```

```bash
# Start worker
airflow celery worker

# Start worker on specific queue
airflow celery worker --queues high_priority,default

# Start flower (monitoring UI)
airflow celery flower
# Access: http://localhost:5555

# Scale workers
airflow celery worker --concurrency 16
```

---

## Kubernetes Executor

```
┌────────────┐  kubectl create pod  ┌──────────────┐
│ Scheduler  │─────────────────────►│  K8s API     │
│            │                      └──────┬───────┘
└────────────┘                             │ schedule
     ▲                                     ▼
     │ watch pod status          ┌──────────────────┐
     │                           │  Worker Pod      │
     └───────────────────────────│  (ephemeral)     │
                                 │  deleted after   │
                                 │  task completes  │
                                 └──────────────────┘
```

- **Each task = one Kubernetes Pod**
- Pods are created on demand, deleted after completion
- **Complete resource isolation** between tasks
- **No persistent workers** — scales to zero when idle
- Slower startup (pod creation ~30s) vs Celery

```ini
[core]
executor = KubernetesExecutor

[kubernetes]
namespace = airflow
worker_container_repository = apache/airflow
worker_container_tag = 2.9.2
delete_worker_pods = True
in_cluster = True
pod_template_file = /opt/airflow/pod_templates/worker.yaml
worker_pods_creation_batch_size = 16
```

---

## CeleryKubernetes Executor

Hybrid: tasks without `queue="kubernetes"` go to Celery workers; tasks with `queue="kubernetes"` go to K8s pods.

```python
# Force a task to run as K8s pod
heavy_job = PythonOperator(
    task_id="heavy_job",
    queue="kubernetes",    # routes to K8s executor
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": "4Gi", "cpu": "2"},
                        limits={"memory": "8Gi", "cpu": "4"},
                    ),
                )]
            )
        )
    },
)
```

---

## Executor Comparison

| Feature | Sequential | Local | Celery | Kubernetes |
|---------|-----------|-------|--------|-----------|
| Parallelism | None | Single machine | Distributed | Pod-per-task |
| Setup complexity | Minimal | Low | Medium | High |
| Persistent workers | N/A | No | Yes | No |
| Resource isolation | No | No | Partial | Full |
| Startup time | Instant | Fast | Fast | Slow (~30s) |
| Scalability | None | Vertical | Horizontal | Horizontal |
| Fault tolerance | None | Low | High | High |
| Database | SQLite only | PG/MySQL | PG/MySQL | PG/MySQL |
| Best for | Dev/test | Small prod | Large prod | Cloud-native prod |

---

## Queue Management

```python
# Assign task to specific queue
scraper_task = PythonOperator(
    task_id="scraper",
    queue="scraper_queue",    # must match worker queue name
    python_callable=run_scraper,
)

gpu_task = PythonOperator(
    task_id="train_model",
    queue="gpu_queue",
    python_callable=train,
)
```

```bash
# Start worker consuming specific queues
airflow celery worker --queues scraper_queue,default
airflow celery worker --queues gpu_queue
```

---

## Redis vs RabbitMQ as Celery Broker

| Feature | Redis | RabbitMQ |
|---------|-------|----------|
| Setup | Simple | More complex |
| Persistence | Optional (AOF) | Yes (durable queues) |
| Message routing | Basic | Advanced (exchanges, routing keys) |
| Admin UI | Redis Commander | Management UI built-in |
| Performance | Higher | Slightly lower |
| Community preference for Airflow | More common | Less common |

---

## Performance Tuning

### Key Config Parameters

```ini
[core]
parallelism = 32              # total concurrent tasks across all DAGs
max_active_tasks_per_dag = 16 # per-DAG concurrent tasks
max_active_runs_per_dag = 5   # concurrent DAG runs per DAG

[scheduler]
parsing_processes = 4         # parallel DAG file parsers
scheduler_heartbeat_sec = 5
min_file_process_interval = 30

[celery]
worker_concurrency = 16       # tasks per worker
worker_prefetch_multiplier = 1 # don't pre-fetch tasks
acks_late = True              # acknowledge after execution (safer)
```

### Celery Worker Tuning

```bash
# Production worker command
airflow celery worker \
    --concurrency 16 \           # 16 concurrent tasks
    --queues default,high_prio \ # queues to consume
    --logfile /var/log/airflow/worker.log

# Auto-scale workers (KEDA for Kubernetes)
# See 17_kubernetes.md
```

---

## Fault Tolerance

### Celery Executor
- **Worker crash**: Tasks in "running" state become stuck → need `acks_late = True` for requeue
- **Broker crash**: Workers stop receiving tasks; scheduler retries task assignment
- **Result backend failure**: Task state may not update; scheduler has health checks

### Kubernetes Executor  
- **Pod OOM killed**: Task fails with exit code 137 → retry if configured
- **Node failure**: K8s reschedules pod on another node
- **API server down**: Scheduler cannot create pods, tasks stuck in queued

```bash
# Find stuck tasks and reset
airflow tasks states-for-dag-run my_dag run_id_here
airflow tasks clear my_dag --start-date 2024-01-01 --end-date 2024-01-01
```

---

## Executor-specific executor_config

```python
# Kubernetes executor — customize the worker pod
from kubernetes.client import models as k8s

task = PythonOperator(
    task_id="gpu_task",
    executor_config={
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"team": "ml"}),
            spec=k8s.V1PodSpec(
                node_selector={"gpu": "true"},
                containers=[k8s.V1Container(
                    name="base",
                    image="my-gpu-image:latest",
                    resources=k8s.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1", "memory": "16Gi"},
                        requests={"memory": "8Gi", "cpu": "4"},
                    ),
                )],
            ),
        )
    },
)
```
