# 16 — Airflow CI/CD & DevOps

## Git Integration

```
Recommended project structure:
my-airflow-project/
├── dags/
│   ├── etl/
│   │   ├── orders_pipeline.py
│   │   └── customers_pipeline.py
│   └── ml/
│       └── training_pipeline.py
├── plugins/
│   └── custom_hooks.py
├── include/
│   ├── sql/
│   │   └── transform_orders.sql
│   └── config/
│       └── pipelines.yaml
├── tests/
│   ├── test_dags.py
│   └── test_operators.py
├── requirements.txt
├── constraints.txt
├── Dockerfile
├── docker-compose.yaml
└── .github/
    └── workflows/
        └── airflow-ci.yaml
```

---

## Custom Docker Image

```dockerfile
# Dockerfile
ARG AIRFLOW_VERSION=2.9.2
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements first for Docker layer caching
COPY requirements.txt /
COPY constraints.txt /

# Install with constraints — REQUIRED for Airflow
RUN pip install --no-cache-dir \
    -r /requirements.txt \
    --constraint /constraints.txt

# Copy DAGs and plugins
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root include/ /opt/airflow/include/
```

```text
# requirements.txt
apache-airflow-providers-postgres==5.7.0
apache-airflow-providers-amazon==8.5.0
apache-airflow-providers-google==10.8.0
apache-airflow-providers-snowflake==5.1.0
pandas==2.0.3
```

---

## DAG Testing

### DAG Validation (Import Test)

```python
# tests/test_dags.py
import os
import pytest
from airflow.models import DagBag

DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")

@pytest.fixture
def dagbag():
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)

def test_no_import_errors(dagbag):
    """All DAG files should import without errors."""
    assert len(dagbag.import_errors) == 0, \
        f"DAG import errors: {dagbag.import_errors}"

def test_dag_count(dagbag):
    """Verify expected number of DAGs are loaded."""
    assert len(dagbag.dags) >= 1

@pytest.mark.parametrize("dag_id,expected_tasks", [
    ("orders_pipeline", ["extract", "transform", "load"]),
    ("customers_pipeline", ["extract", "validate", "load"]),
])
def test_dag_has_expected_tasks(dagbag, dag_id, expected_tasks):
    dag = dagbag.get_dag(dag_id=dag_id)
    assert dag is not None
    task_ids = [task.task_id for task in dag.tasks]
    for expected in expected_tasks:
        assert expected in task_ids, f"Task '{expected}' not found in {dag_id}"

def test_dag_schedule_interval(dagbag):
    """Check all DAGs have explicitly set schedule_interval (not None accidentally)."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.schedule_interval is not None, \
            f"DAG '{dag_id}' has no schedule_interval — use None explicitly if intentional"

def test_dag_has_tags(dagbag):
    """All DAGs should have at least one tag."""
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tags) > 0, f"DAG '{dag_id}' has no tags"

def test_dag_retries(dagbag):
    """Critical DAGs should have retries configured."""
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries >= 1, \
                f"Task '{task.task_id}' in '{dag_id}' has no retries"
```

### Unit Testing with Mocked Context

```python
# tests/test_operators.py
from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
import pendulum

from dags.orders_pipeline import transform_orders, validate_schema

def create_mock_context(dag, task):
    """Create a minimal context dict for testing."""
    dagrun = DagRun(
        dag_id=dag.dag_id,
        run_type=DagRunType.MANUAL,
        execution_date=pendulum.datetime(2024, 1, 15, tz="UTC"),
        run_id="test_run",
        state=DagRunState.RUNNING,
    )
    ti = TaskInstance(task=task, run_id="test_run")
    ti.dag_run = dagrun
    
    return {
        "dag": dag,
        "task": task,
        "ti": ti,
        "dag_run": dagrun,
        "ds": "2024-01-15",
        "ts": "2024-01-15T00:00:00+00:00",
        "data_interval_start": pendulum.datetime(2024, 1, 15, tz="UTC"),
        "data_interval_end": pendulum.datetime(2024, 1, 16, tz="UTC"),
        "run_id": "test_run",
        "params": {},
    }

def test_transform_orders():
    """Test transform logic without touching real DB."""
    raw_data = [
        {"id": 1, "amount": 100.0, "status": "pending"},
        {"id": 2, "amount": 200.0, "status": "complete"},
    ]
    
    result = transform_orders(raw_data)
    
    assert len(result) == 2
    assert result[0]["amount_usd"] == 100.0
    assert result[1]["status"] == "complete"

@patch("dags.orders_pipeline.PostgresHook")
def test_extract_task(mock_hook_class):
    """Test extract task with mocked DB connection."""
    mock_hook = MagicMock()
    mock_hook_class.return_value = mock_hook
    mock_hook.get_pandas_df.return_value = MagicMock()
    mock_hook.get_pandas_df.return_value.__len__ = lambda self: 100
    
    from dags.orders_pipeline import extract_orders
    result = extract_orders.__wrapped__(date="2024-01-15")  # call unwrapped for @task
    
    mock_hook.get_pandas_df.assert_called_once()

@patch("airflow.models.Variable.get")
def test_variable_usage(mock_var_get):
    """Test Variable.get is called correctly."""
    mock_var_get.return_value = "prod"
    
    from dags.orders_pipeline import get_environment
    env = get_environment()
    
    mock_var_get.assert_called_with("environment", default_var="prod")
    assert env == "prod"
```

---

## GitHub Actions CI/CD

```yaml
# .github/workflows/airflow-ci.yaml
name: Airflow CI/CD

on:
  push:
    branches: [main, staging]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_USER: airflow
          POSTGRES_PASSWORD: airflow
          POSTGRES_DB: airflow
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"
      
      - name: Cache pip
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
      
      - name: Install Airflow
        env:
          AIRFLOW_VERSION: "2.9.2"
          PYTHON_VERSION: "3.11"
        run: |
          CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
          pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
          pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"
          pip install pytest pytest-airflow
      
      - name: Initialize Airflow DB
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@localhost/airflow
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
        run: |
          airflow db init
      
      - name: Lint DAGs
        run: |
          pip install ruff
          ruff check dags/ plugins/
      
      - name: Validate DAG imports
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@localhost/airflow
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
        run: |
          python -c "
          from airflow.models import DagBag
          db = DagBag(dag_folder='dags/', include_examples=False)
          if db.import_errors:
              print('Import errors:', db.import_errors)
              exit(1)
          print(f'Loaded {len(db.dags)} DAGs successfully')
          "
      
      - name: Run tests
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@localhost/airflow
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
        run: |
          pytest tests/ -v --tb=short
  
  build-and-push:
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      
      - name: Login to Amazon ECR
        id: login-ecr
        uses: aws-actions/amazon-ecr-login@v2
      
      - name: Build and push Docker image
        env:
          ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          docker build -t $ECR_REGISTRY/airflow:$IMAGE_TAG .
          docker push $ECR_REGISTRY/airflow:$IMAGE_TAG
          docker tag $ECR_REGISTRY/airflow:$IMAGE_TAG $ECR_REGISTRY/airflow:latest
          docker push $ECR_REGISTRY/airflow:latest
  
  deploy:
    runs-on: ubuntu-latest
    needs: build-and-push
    if: github.ref == 'refs/heads/main'
    
    steps:
      - name: Deploy to production via Helm
        run: |
          helm upgrade airflow apache-airflow/airflow \
            --namespace airflow \
            --set images.airflow.repository=$ECR_REGISTRY/airflow \
            --set images.airflow.tag=${{ github.sha }} \
            --wait --timeout=5m
```

---

## Helm Chart Key Values

```yaml
# values.yaml — key production settings
images:
  airflow:
    repository: my-ecr.amazonaws.com/airflow
    tag: latest
    pullPolicy: Always

executor: CeleryExecutor

config:
  core:
    load_examples: "False"
    parallelism: "32"
  scheduler:
    min_file_process_interval: "30"
  celery:
    worker_concurrency: "16"

dags:
  gitSync:
    enabled: true
    repo: "https://github.com/myorg/airflow-dags.git"
    branch: main
    rev: HEAD
    depth: 1
    maxFailures: 3
    subPath: "dags"
    sshKeySecret: airflow-ssh-secret

workers:
  replicas: 3
  resources:
    requests:
      memory: "2Gi"
      cpu: "500m"
    limits:
      memory: "4Gi"
      cpu: "2"

scheduler:
  replicas: 2   # HA schedulers
  resources:
    requests:
      memory: "2Gi"
      cpu: "500m"

postgresql:
  enabled: false  # use external RDS

externalDatabase:
  type: postgres
  host: "airflow.cluster.rds.amazonaws.com"
  port: 5432
  database: airflow
  user: airflow
  passwordSecret: airflow-db-secret
  passwordSecretKey: password

redis:
  enabled: false  # use external ElastiCache

externalRedis:
  host: "airflow.abc123.0001.use1.cache.amazonaws.com"
  port: 6379
```

---

## GitOps with ArgoCD

```yaml
# argocd/airflow-app.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/myorg/airflow-infra.git
    targetRevision: main
    path: helm/airflow
    helm:
      valueFiles:
        - values-prod.yaml
  destination:
    server: https://kubernetes.default.svc
    namespace: airflow
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```
