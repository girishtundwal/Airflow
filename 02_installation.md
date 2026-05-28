# 02 — Airflow Installation & Setup

## Installation Overview

Airflow requires Python 3.8+ and a metadata database (PostgreSQL recommended for production).

**Critical**: Always install Airflow with the **constraints file** — Airflow's dependency tree is complex and pip will otherwise resolve to incompatible versions.

---

## Local Setup with pip

```bash
# 1. Create virtual environment
python -m venv airflow_venv
source airflow_venv/bin/activate  # Linux/Mac
# airflow_venv\Scripts\activate   # Windows

# 2. Set Airflow home
export AIRFLOW_HOME=~/airflow

# 3. Install Airflow with constraints (REQUIRED)
AIRFLOW_VERSION=2.9.2
PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 4. Install providers (same constraints)
pip install "apache-airflow-providers-postgres" \
            "apache-airflow-providers-amazon" \
            "apache-airflow-providers-google" \
            --constraint "${CONSTRAINT_URL}"

# 5. Initialize DB (SQLite by default)
airflow db init

# 6. Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 7. Start services (dev mode — two terminals)
airflow webserver --port 8080
airflow scheduler
```

---

## Docker Compose Setup (Recommended for Local Dev)

```yaml
# docker-compose.yaml — Airflow 2.x with CeleryExecutor
version: '3.8'

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.2
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on: &airflow-common-depends-on
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5

  redis:
    image: redis:latest
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      retries: 5

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      retries: 5

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 30s
      retries: 5

  airflow-worker:
    <<: *airflow-common
    command: celery worker
    environment:
      <<: *airflow-common-env
      DUMB_INIT_SETSID: "0"
    healthcheck:
      test: ["CMD-SHELL", 'celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"']
      interval: 30s
      retries: 5

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      retries: 5

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init
        airflow db upgrade
        airflow users create --username admin --firstname Admin --lastname User \
          --role Admin --email admin@example.com --password admin
    environment:
      <<: *airflow-common-env

volumes:
  postgres-db-volume:
```

```bash
# Start everything
docker compose up -d

# Access UI: http://localhost:8080
# Username: admin / Password: admin
```

---

## Astro CLI (Quickest Local Setup)

```bash
# Install Astro CLI (Mac)
brew install astro

# Or Linux
curl -sSL install.astronomer.io | sudo bash -s

# Initialize new project
mkdir my-airflow-project && cd my-airflow-project
astro dev init

# Project structure created:
# ├── dags/
# ├── plugins/
# ├── include/
# ├── tests/
# ├── requirements.txt
# ├── packages.txt          # OS-level packages
# ├── Dockerfile
# └── .astro/config.yaml

# Start local Airflow
astro dev start
# Access UI: http://localhost:8080

# Stop
astro dev stop
```

---

## Metadata Database Setup

### PostgreSQL (Recommended)

```bash
# Create database and user
sudo -u postgres psql
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
\q

# Configure in airflow.cfg or env var
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

# Initialize schema
airflow db init
airflow db upgrade  # run after Airflow upgrades
```

### MySQL

```bash
# MySQL requires utf8mb4 charset
mysql -u root -p
CREATE DATABASE airflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'localhost';

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+mysqldb://airflow:airflow@localhost:3306/airflow"
```

### SQLite Limitations
- **Development only** — no production use
- Cannot run multiple schedulers
- No parallelism (SequentialExecutor only)
- No concurrent webserver + scheduler writes
- Default path: `~/airflow/airflow.db`

---

## Redis Setup (for CeleryExecutor)

```bash
# Install Redis
brew install redis     # Mac
sudo apt install redis-server  # Ubuntu

# Start Redis
redis-server

# Test
redis-cli ping   # → PONG

# Configure in Airflow
export AIRFLOW__CELERY__BROKER_URL="redis://localhost:6379/0"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow:airflow@localhost/airflow"
```

---

## Key airflow.cfg Sections

```ini
[core]
dags_folder = /opt/airflow/dags
executor = CeleryExecutor           # LocalExecutor, SequentialExecutor, KubernetesExecutor
load_examples = False               # disable example DAGs in prod
parallelism = 32                    # max concurrent task instances
max_active_tasks_per_dag = 16       # per-DAG concurrency limit
max_active_runs_per_dag = 16        # max concurrent DAG runs
fernet_key = <your_fernet_key>      # encrypts DB credentials
default_timezone = UTC

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
sql_alchemy_pool_size = 5
sql_alchemy_max_overflow = 10

[scheduler]
scheduler_heartbeat_sec = 5
min_file_process_interval = 30      # how often to re-parse DAG files
dag_dir_list_interval = 300         # how often to scan dags folder
parsing_processes = 2               # parallel DAG file parsers
max_dagruns_to_create_per_loop = 10

[celery]
celery_app_name = airflow.executors.celery_executor
worker_concurrency = 16             # tasks per worker
broker_url = redis://localhost:6379/0
result_backend = db+postgresql://airflow:airflow@localhost/airflow
flower_host = 0.0.0.0
flower_port = 5555

[webserver]
web_server_host = 0.0.0.0
web_server_port = 8080
secret_key = <random_secret>        # session signing key — change in prod!
authenticate = True
rbac = True

[smtp]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_starttls = True
smtp_mail_from = airflow@example.com
```

---

## Environment Variables Pattern

All `airflow.cfg` settings can be overridden with env vars:
```
Pattern: AIRFLOW__{SECTION}__{KEY}
```

```bash
# Examples
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@host/db
export AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
export AIRFLOW__CORE__FERNET_KEY=<base64_key>
export AIRFLOW__WEBSERVER__SECRET_KEY=<random_string>
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
```

---

## Initializing Airflow

```bash
# Initialize (creates DB schema, first-time setup)
airflow db init

# Upgrade (after Airflow version upgrade)
airflow db upgrade

# Check DB status
airflow db check

# Create admin user
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Verify setup
airflow info
airflow dags list
```

---

## User & Role Management

```bash
# List users
airflow users list

# Create user with specific role
airflow users create --username viewer1 --role Viewer \
  --firstname View --lastname User --email v@example.com --password pass

# Add role to existing user
airflow users add-role --username admin --role Op

# Remove role
airflow users remove-role --username admin --role Op

# Delete user
airflow users delete --username viewer1

# List roles
airflow roles list

# Create custom role
airflow roles create my_custom_role
```

---

## Kubernetes Deployment (Helm)

```bash
# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  --set executor=CeleryExecutor \
  --set config.core.load_examples=False

# Upgrade
helm upgrade airflow apache-airflow/airflow \
  --namespace airflow \
  -f values.yaml

# Check status
kubectl get pods -n airflow
```

---

## Managed Airflow Services

| Service | Provider | Notes |
|---------|----------|-------|
| **MWAA** | AWS | Fargate-based, S3 for DAGs, Aurora Postgres metadata |
| **Cloud Composer** | GCP | GKE-based, version 2 = Autopilot |
| **Astro** | Astronomer | SaaS, best developer experience |
| **HDInsight** | Azure | Older, less popular |

---

## Common Installation Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError` after install | Missing constraints file | Reinstall with `--constraint` |
| `SQLAlchemy connection error` | Wrong DB URL or DB not running | Check `sql_alchemy_conn`, verify DB is up |
| `No module named 'psycopg2'` | Postgres driver missing | `pip install psycopg2-binary` |
| DAGs not appearing | Wrong dags_folder path | Check `AIRFLOW__CORE__DAGS_FOLDER` |
| `Broken DAG` in UI | Import error in DAG file | Check scheduler logs or `airflow dags list-import-errors` |
| `Fernet key` error | Key not set or changed | Set `AIRFLOW__CORE__FERNET_KEY` consistently |
| Webserver 502 | Scheduler not running | Start scheduler separately |
| Tasks stuck in `queued` | Workers not running | Start `airflow celery worker` |
| Permission denied on logs/ | Wrong file permissions | `chmod -R 777 logs/` (dev) or fix UID in Docker |
