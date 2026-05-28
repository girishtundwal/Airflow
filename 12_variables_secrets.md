# 12 — Airflow Variables, Secrets & Configurations

## Airflow Variables

Key-value pairs stored in the metadata database. Use for configuration that changes across environments without redeploying DAG code.

```python
from airflow.models import Variable

# Get variable (raises KeyError if not found and no default)
env = Variable.get("environment")

# Get with default value
env = Variable.get("environment", default_var="prod")

# Get JSON variable (deserialize from JSON string)
config = Variable.get("pipeline_config", deserialize_json=True)
# Returns: {"batch_size": 1000, "source": "api", "retries": 3}

# Set variable
Variable.set("environment", "prod")
Variable.set("pipeline_config", {"batch_size": 1000}, serialize_json=True)

# Delete
Variable.delete("old_variable")

# WRONG: calling Variable.get at top-level runs on every DAG parse
env = Variable.get("env")   # runs thousands of times per day!

# CORRECT: call inside task callables
def my_task(**context):
    env = Variable.get("environment")  # called only when task executes
```

### Variables in Jinja Templates

```python
BashOperator(
    task_id="run",
    bash_command="python script.py --env {{ var.value.environment }} --batch {{ var.json.pipeline_config.batch_size }}",
)
```

### Managing Variables via CLI

```bash
# Set variable
airflow variables set environment prod
airflow variables set pipeline_config '{"batch_size": 1000}' --json

# Get variable
airflow variables get environment

# List all variables
airflow variables list

# Export all variables
airflow variables export variables.json

# Import variables
airflow variables import variables.json

# Delete
airflow variables delete old_variable
```

---

## Environment Variables

All `airflow.cfg` settings can be overridden with environment variables using the pattern:
```
AIRFLOW__{SECTION}__{KEY}
```

```bash
# Core settings
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=32
export AIRFLOW__CORE__FERNET_KEY=<base64_key>
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

# Database
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:pass@host/db

# Scheduler
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
export AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5

# Webserver
export AIRFLOW__WEBSERVER__SECRET_KEY=<random_string>
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080

# Celery
export AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
export AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://user:pass@host/db

# SMTP (email notifications)
export AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
export AIRFLOW__SMTP__SMTP_PORT=587
export AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow@company.com
```

**Priority**: Environment variables > airflow.cfg > default values

---

## airflow.cfg Key Sections

```ini
[core]
executor = CeleryExecutor
dags_folder = /opt/airflow/dags
load_examples = False
fernet_key = <base64_fernet_key>
default_timezone = UTC
parallelism = 32
max_active_tasks_per_dag = 16
max_active_runs_per_dag = 5

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
sql_alchemy_pool_size = 5
sql_alchemy_max_overflow = 10
sql_alchemy_pool_timeout = 30

[scheduler]
scheduler_heartbeat_sec = 5
min_file_process_interval = 30
dag_dir_list_interval = 300
parsing_processes = 2

[webserver]
web_server_host = 0.0.0.0
web_server_port = 8080
secret_key = <long_random_string>

[celery]
broker_url = redis://localhost:6379/0
result_backend = db+postgresql://airflow:airflow@localhost/airflow
worker_concurrency = 16

[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = airflow@company.com
smtp_password = <password>
smtp_port = 587
smtp_mail_from = airflow@company.com

[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"url": "http://vault:8200", "token": "...", "connections_path": "airflow/connections"}
```

---

## Fernet Encryption

Fernet encrypts passwords and connection credentials stored in the metadata database. Without it, credentials are stored in plaintext.

```bash
# Generate a Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Output: something like: dSkRMhXnRiqN1vOXcF28bP3ysKXnTEyNqMCYKKVkUcQ=

# Set in environment
export AIRFLOW__CORE__FERNET_KEY="dSkRMhXnRiqN1vOXcF28bP3ysKXnTEyNqMCYKKVkUcQ="

# Rotate Fernet key (encrypts existing values with new key)
airflow rotate-fernet-key
```

**Important**: If you change the Fernet key without rotating, all encrypted values become unreadable. Always use `rotate-fernet-key` when changing keys.

---

## Secret Backends

```
Resolution order for connections and variables:
1. Environment variable (AIRFLOW_CONN_* / AIRFLOW_VAR_*)
2. Secrets backend (Vault, AWS SM, GCP SM, Azure KV)
3. Metadata database (Connection/Variable tables)
```

### HashiCorp Vault

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {
    "connections_path": "airflow/connections",
    "variables_path": "airflow/variables",
    "config_path": "airflow/config",
    "url": "http://vault.service:8200",
    "auth_type": "token",
    "token": "vault-token-here",
    "mount_point": "secret"
}
```

```bash
# Store in Vault
vault kv put secret/airflow/connections/my_postgres \
    conn_uri="postgresql://user:pass@db:5432/analytics"

vault kv put secret/airflow/variables/api_key \
    value="sk-abc123"
```

### AWS Secrets Manager

```ini
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {
    "connections_prefix": "airflow/connections",
    "variables_prefix": "airflow/variables",
    "config_prefix": "airflow/config",
    "region_name": "us-east-1"
}
```

```bash
# Store connection in AWS SM
aws secretsmanager create-secret \
    --name "airflow/connections/my_postgres" \
    --secret-string "postgresql://user:pass@host:5432/db"

# Or as JSON
aws secretsmanager create-secret \
    --name "airflow/connections/my_postgres" \
    --secret-string '{
        "conn_type": "postgres",
        "host": "db.example.com",
        "port": "5432",
        "schema": "analytics",
        "login": "airflow",
        "password": "secret123"
    }'
```

### GCP Secret Manager

```ini
[secrets]
backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
backend_kwargs = {
    "connections_prefix": "airflow-connections",
    "variables_prefix": "airflow-variables",
    "project_id": "my-gcp-project",
    "sep": "-"
}
```

```bash
# Store in GCP SM
echo -n "postgresql://user:pass@host:5432/db" | \
    gcloud secrets create airflow-connections-my-postgres \
    --data-file=-
```

---

## Airflow Params (Runtime Parameters)

```python
from airflow.models.param import Param
from airflow.decorators import dag, task
import pendulum

@dag(
    schedule_interval=None,  # manual only
    start_date=pendulum.datetime(2024, 1, 1),
    params={
        "environment": Param(
            default="prod",
            type="string",
            enum=["dev", "staging", "prod"],
            description="Target environment",
        ),
        "date": Param(
            default="{{ ds }}",
            type="string",
            description="Processing date (YYYY-MM-DD)",
        ),
        "batch_size": Param(
            default=1000,
            type="integer",
            minimum=1,
            maximum=50000,
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
        ),
    },
)
def parameterized_pipeline():
    
    @task
    def process(**context):
        params = context["params"]
        env = params["environment"]
        batch = params["batch_size"]
        dry = params["dry_run"]
        
        print(f"Running in {env}, batch={batch}, dry_run={dry}")

pipeline = parameterized_pipeline()
```

```bash
# Trigger with custom params
airflow dags trigger parameterized_pipeline \
    --conf '{"environment": "staging", "batch_size": 500, "dry_run": true}'
```

---

## Secure Configuration Management

```
NEVER do this:
    DB_PASSWORD = "mysecret"              # hardcoded in DAG code
    conn = psycopg2.connect(password="X") # bypasses Airflow connections
    Variable.get("api_key")              # at top-level (exposed in parse logs)

DO this:
    hook = PostgresHook(postgres_conn_id="my_db")  # uses connection
    api_key = Variable.get("api_key")              # inside task callable
    # Or use secrets backend for sensitive vars
```

### .env file for local development

```bash
# .env (never commit to git!)
AIRFLOW__CORE__FERNET_KEY=<key>
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:dev@localhost/airflow
AIRFLOW__CELERY__BROKER_URL=redis://localhost:6379/0
AIRFLOW_CONN_MY_DB=postgresql://dev_user:dev_pass@localhost:5432/dev_db
AIRFLOW_VAR_API_KEY=dev-api-key-123

# Load in docker-compose
env_file:
  - .env
```
