# 25 — Hands-on Airflow Labs & Projects

## Lab 1: Local Airflow Setup

```bash
# Quick setup with Docker Compose
mkdir airflow-labs && cd airflow-labs

# Create required directories
mkdir -p dags logs plugins

# Set user ID (required for Docker permissions)
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env

# Download official docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Disable example DAGs
echo "AIRFLOW__CORE__LOAD_EXAMPLES=False" >> .env

# Initialize
docker compose up airflow-init

# Start all services
docker compose up -d

# Verify (wait ~2 minutes)
docker compose ps
curl http://localhost:8080/health
# Open: http://localhost:8080 (airflow/airflow)

# View logs
docker compose logs -f airflow-scheduler

# Stop
docker compose down
```

---

## Lab 2: ETL Pipeline — API → PostgreSQL

```python
# dags/lab_etl_pipeline.py
from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
import logging

log = logging.getLogger(__name__)

@dag(
    dag_id="lab_etl_pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lab", "etl"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
)
def etl_pipeline():
    
    @task
    def extract(**context) -> list:
        """Extract data from a mock API."""
        import requests
        
        date = context["ds"]
        
        # Using JSONPlaceholder as mock API
        response = requests.get(
            "https://jsonplaceholder.typicode.com/todos",
            params={"userId": 1},
            timeout=30,
        )
        response.raise_for_status()
        
        records = response.json()
        log.info(f"Extracted {len(records)} records for {date}")
        return records
    
    @task
    def transform(records: list, **context) -> list:
        """Transform raw records."""
        date = context["ds"]
        
        transformed = []
        for r in records:
            transformed.append({
                "todo_id": r["id"],
                "user_id": r["userId"],
                "title": r["title"][:100],  # truncate
                "completed": r["completed"],
                "load_date": date,
            })
        
        log.info(f"Transformed {len(transformed)} records")
        return transformed
    
    @task
    def load(records: list, **context) -> int:
        """Load records to PostgreSQL."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        if not records:
            log.warning("No records to load")
            return 0
        
        hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # Create table if not exists
        hook.run("""
            CREATE TABLE IF NOT EXISTS todos (
                todo_id INTEGER,
                user_id INTEGER,
                title VARCHAR(200),
                completed BOOLEAN,
                load_date DATE,
                PRIMARY KEY (todo_id, load_date)
            )
        """)
        
        # Idempotent: delete and reload
        date = context["ds"]
        hook.run(f"DELETE FROM todos WHERE load_date = '{date}'")
        
        hook.insert_rows(
            table="todos",
            rows=[(r["todo_id"], r["user_id"], r["title"], r["completed"], r["load_date"]) 
                  for r in records],
            target_fields=["todo_id", "user_id", "title", "completed", "load_date"],
        )
        
        count = hook.get_first(f"SELECT COUNT(*) FROM todos WHERE load_date = '{date}'"  )[0]
        log.info(f"Loaded {count} records")
        return count
    
    @task
    def verify(count: int, **context):
        """Verify load was successful."""
        if count == 0:
            raise ValueError("No records were loaded!")
        log.info(f"Verification passed: {count} records loaded for {context['ds']}")
    
    records = extract()
    transformed = transform(records)
    loaded = load(transformed)
    verify(loaded)

pipeline = etl_pipeline()
```

---

## Lab 3: Incremental Pipeline

```python
# dags/lab_incremental.py
from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id="lab_incremental_load",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lab", "incremental"],
)
def incremental_pipeline():
    
    @task
    def extract_window(**context) -> str:
        """Extract only data within the data interval window."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
        
        # Use data interval for precise windowing
        start = context["data_interval_start"].isoformat()
        end = context["data_interval_end"].isoformat()
        
        log_group = f"{context['ds_nodash']}_{context['data_interval_start'].strftime('%H%M')}"
        
        hook = PostgresHook(postgres_conn_id="source_db")
        df = hook.get_pandas_df(f"""
            SELECT id, event_type, user_id, payload, created_at
            FROM events
            WHERE created_at >= '{start}'
              AND created_at < '{end}'
        """)
        
        if df.empty:
            return None
        
        output_path = f"/tmp/events_{log_group}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path
    
    @task
    def upsert_to_warehouse(file_path: str, **context):
        """Upsert extracted data to warehouse."""
        if file_path is None:
            return 0
        
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        df = pd.read_parquet(file_path)
        hook = PostgresHook(postgres_conn_id="warehouse_db")
        
        # UPSERT: safe to re-run
        for _, row in df.iterrows():
            hook.run("""
                INSERT INTO events_warehouse (id, event_type, user_id, payload, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    user_id = EXCLUDED.user_id,
                    payload = EXCLUDED.payload
            """, parameters=(row["id"], row["event_type"], row["user_id"], 
                            str(row["payload"]), row["created_at"]))
        
        return len(df)
    
    file = extract_window()
    upsert_to_warehouse(file)

pipeline = incremental_pipeline()
```

---

## Lab 4: Dynamic DAG Factory

```yaml
# config/tables.yaml
tables:
  - name: orders
    source_schema: raw
    target_schema: processed
    schedule: "@daily"
    min_rows: 100
    partition_col: order_date
    
  - name: customers
    source_schema: raw
    target_schema: processed
    schedule: "@daily"
    min_rows: 10
    partition_col: created_date

  - name: products
    source_schema: raw
    target_schema: processed
    schedule: "@weekly"
    min_rows: 1
    partition_col: updated_date
```

```python
# dags/lab_factory.py
import yaml
from pathlib import Path
from airflow.decorators import dag, task
import pendulum


def make_dag(table_config: dict):
    
    @dag(
        dag_id=f"process_{table_config['name']}",
        schedule_interval=table_config["schedule"],
        start_date=pendulum.datetime(2024, 1, 1),
        catchup=False,
        tags=["factory", table_config["source_schema"]],
    )
    def table_pipeline():
        
        @task
        def extract(**context) -> str:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            import pandas as pd
            
            hook = PostgresHook(postgres_conn_id="source_db")
            df = hook.get_pandas_df(f"""
                SELECT * FROM {table_config['source_schema']}.{table_config['name']}
                WHERE {table_config['partition_col']} = '{context['ds']}'
            """)
            
            path = f"/tmp/{table_config['name']}_{context['ds_nodash']}.parquet"
            df.to_parquet(path, index=False)
            return path
        
        @task
        def validate(file_path: str) -> str:
            import pandas as pd
            df = pd.read_parquet(file_path)
            assert len(df) >= table_config["min_rows"], \
                f"Only {len(df)} rows, need {table_config['min_rows']}"
            return file_path
        
        @task
        def load(file_path: str, **context):
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            
            df = pd.read_parquet(file_path)
            hook = PostgresHook(postgres_conn_id="target_db")
            
            hook.run(f"""
                DELETE FROM {table_config['target_schema']}.{table_config['name']}
                WHERE {table_config['partition_col']} = '{context['ds']}'
            """)
            
            df.to_sql(
                name=table_config["name"],
                con=hook.get_sqlalchemy_engine(),
                schema=table_config["target_schema"],
                if_exists="append",
                index=False,
            )
        
        raw = extract()
        good = validate(raw)
        load(good)
    
    return table_pipeline()


# Load config and create all DAGs
config_file = Path(__file__).parent.parent / "config" / "tables.yaml"
with open(config_file) as f:
    config = yaml.safe_load(f)

for tbl in config["tables"]:
    d = make_dag(tbl)
    globals()[d.dag_id] = d
```

---

## Lab 5: TaskFlow API with Dynamic Mapping

```python
# dags/lab_taskflow_dynamic.py
from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id="lab_taskflow_dynamic",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["lab", "taskflow", "dynamic"],
)
def taskflow_dynamic():
    
    @task
    def get_regions() -> list:
        """Return list of regions to process."""
        return ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
    
    @task
    def process_region(region: str, **context) -> dict:
        """Process data for one region."""
        import random  # simulate processing
        records = random.randint(100, 10000)
        return {"region": region, "records": records, "date": context["ds"]}
    
    @task
    def aggregate(results: list, **context) -> dict:
        """Aggregate results from all regions."""
        total = sum(r["records"] for r in results)
        regions_done = [r["region"] for r in results]
        
        summary = {
            "date": context["ds"],
            "total_records": total,
            "regions_processed": len(regions_done),
            "details": results,
        }
        
        print(f"Daily summary: {total} records across {len(regions_done)} regions")
        return summary
    
    @task
    def send_report(summary: dict):
        """Send daily report."""
        print(f"Report for {summary['date']}: {summary['total_records']} total records")
    
    regions = get_regions()
    
    # Dynamic mapping: one task per region, all in parallel
    results = process_region.expand(region=regions)
    
    summary = aggregate(results)
    send_report(summary)

pipeline = taskflow_dynamic()
```

---

## Lab 6: Sensors & Event-driven

```python
# dags/lab_sensors.py
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
import pendulum

INPUT_DATASET = Dataset("file:///data/input/")
OUTPUT_DATASET = Dataset("file:///data/output/")

@dag(
    dag_id="lab_producer",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["lab", "sensor"],
)
def producer():
    
    wait_input = FileSensor(
        task_id="wait_for_input",
        filepath="/data/input/{{ ds }}/data.csv",
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
        soft_fail=False,
    )
    
    @task(outlets=[OUTPUT_DATASET])
    def process(**context):
        import pandas as pd
        
        df = pd.read_csv(f"/data/input/{context['ds']}/data.csv")
        output_path = f"/data/output/{context['ds']}/processed.parquet"
        df.to_parquet(output_path)
        print(f"Processed {len(df)} rows → {output_path}")
    
    wait_input >> process()

@dag(
    dag_id="lab_consumer",
    schedule=[OUTPUT_DATASET],  # triggered by producer
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["lab", "sensor", "event-driven"],
)
def consumer():
    
    @task(inlets=[OUTPUT_DATASET])
    def analyze(**context):
        import pandas as pd
        
        df = pd.read_parquet(f"/data/output/{context['ds']}/processed.parquet")
        print(f"Analyzing {len(df)} records")

p = producer()
c = consumer()
```

---

## Lab 7: CI/CD DAG Validation Suite

```python
# tests/test_all_dags.py
import os
import pytest
from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

class TestDagIntegrity:
    
    def test_no_import_errors(self, dag_bag):
        assert dag_bag.import_errors == {}, \
            f"DAG import errors:\n" + "\n".join(
                f"  {k}: {v}" for k, v in dag_bag.import_errors.items()
            )
    
    def test_all_dags_have_tags(self, dag_bag):
        for dag_id, dag in dag_bag.dags.items():
            assert dag.tags, f"DAG '{dag_id}' is missing tags"
    
    def test_all_dags_have_retries(self, dag_bag):
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                assert task.retries >= 1, \
                    f"Task '{task.task_id}' in '{dag_id}' has no retries configured"
    
    def test_no_dynamic_start_date(self, dag_bag):
        import pendulum
        cutoff = pendulum.now().subtract(days=1)
        for dag_id, dag in dag_bag.dags.items():
            if dag.start_date:
                assert dag.start_date < cutoff, \
                    f"DAG '{dag_id}' start_date ({dag.start_date}) appears dynamic (too recent)"
    
    def test_catchup_disabled(self, dag_bag):
        # All production DAGs should have explicit catchup setting
        for dag_id, dag in dag_bag.dags.items():
            assert dag.catchup is not None, \
                f"DAG '{dag_id}' has no explicit catchup setting"
    
    def test_task_dependency_cycle(self, dag_bag):
        for dag_id, dag in dag_bag.dags.items():
            # Airflow prevents cycles, but verify dag is loadable
            assert dag.tasks is not None
            assert dag.dag_id == dag_id
    
    @pytest.mark.parametrize("dag_id", ["lab_etl_pipeline", "lab_incremental_load"])
    def test_specific_dag_task_count(self, dag_bag, dag_id):
        dag = dag_bag.get_dag(dag_id)
        assert dag is not None, f"DAG '{dag_id}' not found"
        assert len(dag.tasks) >= 2, f"DAG '{dag_id}' should have at least 2 tasks"
```

```yaml
# .github/workflows/validate-dags.yaml
name: Validate DAGs
on: [push, pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with: {python-version: "3.11"}
      - run: |
          AIRFLOW_VERSION=2.9.2
          PYTHON_VERSION=3.11
          CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
          pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
          pip install pytest
      - run: |
          export AIRFLOW__CORE__LOAD_EXAMPLES=False
          airflow db init
          pytest tests/ -v
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////tmp/airflow.db
```

---

## Lab 8: Monitoring Setup

```yaml
# docker-compose-monitoring.yaml — Add to your main docker-compose
version: "3.8"
services:
  statsd-exporter:
    image: prom/statsd-exporter:latest
    ports:
      - "9102:9102"
      - "8125:8125/udp"
    command: ["--statsd.mapping-config=/etc/statsd/mapping.yml"]
    volumes:
      - ./config/statsd-mapping.yml:/etc/statsd/mapping.yml

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./config/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

```yaml
# config/prometheus.yml
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: airflow
    static_configs:
      - targets: ["statsd-exporter:9102"]
```

```ini
# airflow.cfg additions for StatsD
[metrics]
statsd_on = True
statsd_host = statsd-exporter
statsd_port = 8125
statsd_prefix = airflow
```
