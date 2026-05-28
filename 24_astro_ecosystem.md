# 24 — Astro & Modern Airflow Ecosystem

## Astronomer Overview

**Astronomer** is the company behind Apache Airflow. They contribute heavily to the core project and offer:
- **Astro CLI**: Local development and deployment tool
- **Astro Cloud**: Fully managed Airflow SaaS platform
- **Astro Runtime**: Enhanced Airflow Docker images with extras
- **Cosmos**: dbt + Airflow integration library
- **Astro SDK**: Data-frame-based task SDK (less common)

---

## Astro CLI

### Installation

```bash
# Mac (Homebrew)
brew install astro

# Linux / WSL
curl -sSL install.astronomer.io | sudo bash -s

# Windows (PowerShell)
winget install -e --id Astronomer.Astro

# Verify
astro version
```

### Project Initialization

```bash
# Create new Airflow project
mkdir my-airflow && cd my-airflow
astro dev init

# Project structure created:
# ├── dags/
# │   └── example_astronaut_etl.py
# ├── plugins/
# ├── include/
# ├── tests/
# │   └── test_dag_integrity.py
# ├── requirements.txt            # Python dependencies
# ├── packages.txt                # OS-level packages (apt-get)
# ├── Dockerfile                  # extends astro-runtime
# ├── .astro/
# │   └── config.yaml             # project config
# └── airflow_settings.yaml       # connections/variables/pools for local dev
```

### Local Development Commands

```bash
# Start local Airflow (Docker-based)
astro dev start
# ► Webserver: http://localhost:8080 (admin / admin)
# ► Postgres: localhost:5432
# ► Flower: http://localhost:5555 (if CeleryExecutor)

# Stop
astro dev stop

# Restart (keeps data)
astro dev restart

# Kill (removes containers + data)
astro dev kill

# Check status
astro dev ps

# View logs
astro dev logs --scheduler
astro dev logs --webserver

# Open bash in scheduler container
astro dev bash -c scheduler

# Run a DAG task locally (without metadata DB)
astro dev run dags test my_dag 2024-01-15

# Validate DAG syntax
astro dev parse

# Run tests
astro dev pytest tests/
```

### Deployment Commands

```bash
# Login to Astro Cloud
astro login astronomer.io

# List workspaces
astro workspace list

# List deployments
astro deployment list

# Deploy to cloud
astro deploy <deployment-id>
# Or deploy from CI/CD:
astro deploy <deployment-id> --force --no-prompt

# Inspect deployment
astro deployment inspect <deployment-id>

# Create deployment
astro deployment create \
    --name "Production" \
    --workspace-id <workspace-id> \
    --executor CELERY \
    --scheduler-size medium
```

### airflow_settings.yaml (local dev seed)

```yaml
# Automatically creates connections/variables/pools on `astro dev start`
airflow:
  connections:
    - conn_id: postgres_default
      conn_type: postgres
      conn_host: host.docker.internal
      conn_port: 5432
      conn_schema: my_db
      conn_login: user
      conn_password: pass
    
    - conn_id: aws_default
      conn_type: aws
      conn_extra: '{"aws_access_key_id": "AKIA...", "aws_secret_access_key": "...", "region_name": "us-east-1"}'
  
  variables:
    - variable_name: environment
      variable_value: "dev"
    - variable_name: batch_size
      variable_value: "1000"
  
  pools:
    - pool_name: db_connections
      pool_slot: 10
      pool_description: "PostgreSQL connection slots"
```

---

## Astro Runtime

```dockerfile
# Dockerfile — use Astro Runtime instead of base Apache Airflow
FROM quay.io/astronomer/astro-runtime:10.2.0
# Version mapping: astro-runtime:10.x.x = Airflow 2.9.x

# astro-runtime includes:
# - Apache Airflow (latest stable)
# - 20+ most popular providers pre-installed
# - OpenLineage integration
# - Airflow upgrade manager
# - Extra security patches

# Add custom packages
USER root
RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*
USER astro

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
```

---

## Cosmos (dbt + Airflow)

**Cosmos** by Astronomer generates Airflow DAGs from dbt projects — each dbt model becomes a task.

```bash
pip install astronomer-cosmos
```

### DbtDag — Full DAG from dbt project

```python
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import ExecutionMode
import pendulum

profile_config = ProfileConfig(
    profile_name="my_project",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "ANALYTICS",
            "schema": "TRANSFORMED",
            "warehouse": "TRANSFORM_WH",
            "role": "TRANSFORM_ROLE",
        },
    ),
)

dbt_dag = DbtDag(
    dag_id="dbt_transform",
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/my_project",
        models_relative_path="models/",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,    # run dbt locally
        # Or: ExecutionMode.DOCKER, ExecutionMode.KUBERNETES
    ),
    render_config=RenderConfig(
        select=["tag:daily"],                  # only daily models
        exclude=["tag:skip"],
        test_behavior="after_each",            # run dbt test after each model
    ),
    operator_args={
        "retries": 2,
        "pool": "dbt_pool",
    },
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
)
```

### DbtTaskGroup — Embed dbt inside a larger DAG

```python
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def full_pipeline():
    
    @task
    def extract():
        pass
    
    @task
    def load_raw():
        pass
    
    # Cosmos manages dbt as a task group
    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig("/usr/local/airflow/dbt/project"),
        profile_config=profile_config,
        render_config=RenderConfig(select=["path:models/marts/"]),
    )
    
    @task
    def run_reports():
        pass
    
    extract() >> load_raw() >> dbt_models >> run_reports()

pipeline = full_pipeline()
```

---

## OpenLineage (Data Lineage)

OpenLineage is an open standard for metadata and lineage collection.

```bash
pip install apache-airflow-providers-openlineage
```

```ini
# airflow.cfg
[openlineage]
transport = {"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"}
disabled = False
namespace = my_airflow_instance
```

```python
# Operators auto-emit lineage events when OpenLineage is configured
# No code changes needed for built-in operators

# For custom operators, emit lineage manually:
from openlineage.airflow.utils import get_job_name
from openlineage.client.run import Dataset

def my_custom_task():
    # OpenLineage events auto-captured for: BigQuery, Snowflake, Spark, dbt
    pass
```

---

## Airbyte + Airflow

```python
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

# Trigger Airbyte sync (EL job)
trigger_sync = AirbyteTriggerSyncOperator(
    task_id="trigger_airbyte_sync",
    airbyte_conn_id="airbyte_default",
    connection_id="your-airbyte-connection-id",
    asynchronous=True,    # don't wait — use sensor below
)

# Wait for sync to complete
wait_for_sync = AirbyteJobSensor(
    task_id="wait_for_airbyte",
    airbyte_conn_id="airbyte_default",
    airbyte_job_id=trigger_sync.output,
    poke_interval=60,
    timeout=3600,
)

trigger_sync >> wait_for_sync
```

---

## Great Expectations + Airflow

```python
from airflow.operators.bash import BashOperator

# Using GX CLI in BashOperator
validate = BashOperator(
    task_id="gx_validate",
    bash_command="""
        cd /gx && \
        great_expectations checkpoint run orders_{{ ds_nodash }}_checkpoint
    """,
)

# Or using GreatExpectationsOperator (deprecated but still used)
# from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Or custom @task with GX Python API (see 23_realworld_patterns.md)
```

---

## Modern Data Stack Integration

```
Airbyte (EL: source → warehouse raw)
    ↓ triggered by Airflow
Apache Airflow (Orchestration)
    ↓ triggers
dbt (Transform: raw → staging → marts)
    ↓ managed by Cosmos in Airflow
Great Expectations (Data Quality)
    ↓ validated in Airflow
Snowflake / BigQuery / Redshift (Storage + Compute)
    ↓ lineage tracked by
OpenLineage → Marquez / DataHub (Lineage & Catalog)
```

### Full Modern Data Stack DAG

```python
from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
import pendulum

@dag(
    dag_id="modern_data_stack",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["modern-data-stack"],
)
def modern_data_stack():
    
    # 1. Airbyte: Extract & Load (source → raw layer in Snowflake)
    extract_orders = AirbyteTriggerSyncOperator(
        task_id="airbyte_orders_sync",
        airbyte_conn_id="airbyte_default",
        connection_id="orders-postgres-to-snowflake",
        wait_seconds=30,
        timeout=3600,
    )
    
    extract_customers = AirbyteTriggerSyncOperator(
        task_id="airbyte_customers_sync",
        airbyte_conn_id="airbyte_default",
        connection_id="customers-api-to-snowflake",
        wait_seconds=30,
        timeout=3600,
    )
    
    # 2. dbt: Transform (via Cosmos)
    dbt_transforms = DbtTaskGroup(
        group_id="dbt_transforms",
        project_config=ProjectConfig("/usr/local/airflow/dbt/analytics"),
        profile_config=ProfileConfig(
            profile_name="analytics",
            target_name="prod",
            profile_mapping=...,
        ),
        render_config=RenderConfig(select=["tag:daily"]),
    )
    
    # 3. Data quality validation
    @task
    def validate_output(**context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        
        checks = [
            f"SELECT COUNT(*) > 0 FROM marts.orders_daily WHERE order_date = '{context['ds']}'",
            f"SELECT COUNT(*) > 0 FROM marts.customer_summary WHERE snapshot_date = '{context['ds']}'",
        ]
        
        for check in checks:
            result = hook.get_first(check)[0]
            if not result:
                raise ValueError(f"Quality check failed: {check}")
        
        return "all_checks_passed"
    
    # Pipeline flow
    [extract_orders, extract_customers] >> dbt_transforms >> validate_output()

pipeline = modern_data_stack()
```

---

## Airflow Ecosystem Tools Reference

| Tool | Category | Integration |
|------|----------|------------|
| **OpenLineage** | Data lineage | Built-in provider, auto-emits |
| **Marquez** | Lineage backend | OpenLineage → Marquez API |
| **DataHub** | Data catalog | DataHub Airflow plugin |
| **Great Expectations** | Data quality | BashOperator or custom @task |
| **dbt Core** | Transformation | Cosmos or BashOperator |
| **dbt Cloud** | Transformation (SaaS) | DbtCloudRunJobOperator |
| **Airbyte** | EL (data movement) | AirbyteTriggerSyncOperator |
| **Fivetran** | EL (SaaS) | FivetranOperator |
| **MLflow** | ML tracking | Custom @task with mlflow SDK |
| **Grafana** | Monitoring | StatsD/Prometheus metrics |
| **Apache Atlas** | Metadata/lineage | Custom hook |
