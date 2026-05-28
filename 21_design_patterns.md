# 21 — Airflow Design Patterns

## Idempotent DAG Design

An idempotent DAG produces the same result whether run once or many times with the same inputs.

```python
from airflow.decorators import dag, task
import pendulum

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def idempotent_pipeline():
    
    @task
    def load_orders(**context) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ds = context["ds"]
        hook = PostgresHook(postgres_conn_id="target_db")
        
        # IDEMPOTENT: DELETE first, then INSERT (or use UPSERT)
        # Ensures re-runs don't create duplicates
        hook.run(f"""
            DELETE FROM orders_processed WHERE order_date = '{ds}';
            
            INSERT INTO orders_processed (order_date, order_id, amount)
            SELECT order_date, order_id, amount
            FROM raw_orders
            WHERE order_date = '{ds}';
        """)
        
        count = hook.get_first(
            f"SELECT COUNT(*) FROM orders_processed WHERE order_date = '{ds}'"
        )[0]
        return count

pipeline = idempotent_pipeline()
```

---

## Incremental Processing Pattern

```python
@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=True)
def incremental_load():
    
    @task
    def extract_incremental(**context) -> str:
        """Extract only data for the specific data interval."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
        
        # Use data_interval_start and data_interval_end for precise windowing
        start = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        
        hook = PostgresHook(postgres_conn_id="source_db")
        df = hook.get_pandas_df(f"""
            SELECT * FROM events
            WHERE created_at >= '{start}'
              AND created_at < '{end}'
        """)
        
        output_path = f"/tmp/events_{context['ds_nodash']}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path
    
    @task
    def upsert_to_warehouse(file_path: str, **context):
        """Upsert pattern — safe to re-run."""
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        df = pd.read_parquet(file_path)
        hook = PostgresHook(postgres_conn_id="target_db")
        
        # UPSERT: insert new, update existing
        for _, row in df.iterrows():
            hook.run("""
                INSERT INTO events_warehouse (id, event_type, user_id, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    user_id = EXCLUDED.user_id,
                    created_at = EXCLUDED.created_at
            """, parameters=(row["id"], row["event_type"], row["user_id"], row["created_at"]))
    
    file = extract_incremental()
    upsert_to_warehouse(file)

pipeline = incremental_load()
```

---

## Dynamic DAG Factory from YAML

```yaml
# config/pipelines.yaml
pipelines:
  - name: orders
    source_conn: postgres_source
    target_conn: snowflake_prod
    source_table: raw.orders
    target_table: staging.orders
    schedule: "@daily"
    quality_threshold: 1000

  - name: customers
    source_conn: postgres_source
    target_conn: snowflake_prod
    source_table: raw.customers
    target_table: staging.customers
    schedule: "@daily"
    quality_threshold: 100
```

```python
# dags/factory.py
import yaml
from pathlib import Path
from airflow.decorators import dag, task
import pendulum

def create_pipeline_dag(config: dict):
    
    @dag(
        dag_id=f"pipeline_{config['name']}",
        schedule_interval=config["schedule"],
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["factory", config["name"]],
    )
    def pipeline():
        
        @task
        def extract(**context) -> str:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            import pandas as pd
            
            hook = PostgresHook(postgres_conn_id=config["source_conn"])
            df = hook.get_pandas_df(
                f"SELECT * FROM {config['source_table']} WHERE date = '{context['ds']}'"
            )
            path = f"/tmp/{config['name']}_{context['ds_nodash']}.parquet"
            df.to_parquet(path)
            return path
        
        @task
        def validate(file_path: str, **context) -> str:
            import pandas as pd
            df = pd.read_parquet(file_path)
            if len(df) < config["quality_threshold"]:
                raise ValueError(f"Only {len(df)} rows, need {config['quality_threshold']}")
            return file_path
        
        @task
        def load(file_path: str, **context):
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
            import pandas as pd
            
            df = pd.read_parquet(file_path)
            hook = SnowflakeHook(snowflake_conn_id=config["target_conn"])
            hook.insert_rows(
                table=config["target_table"],
                rows=df.values.tolist(),
                target_fields=df.columns.tolist(),
            )
        
        file = extract()
        validated = validate(file)
        load(validated)
    
    return pipeline()


# Load configs and create DAGs
config_path = Path(__file__).parent.parent / "config" / "pipelines.yaml"
with open(config_path) as f:
    all_configs = yaml.safe_load(f)

for cfg in all_configs["pipelines"]:
    dag_instance = create_pipeline_dag(cfg)
    globals()[dag_instance.dag_id] = dag_instance  # register in module globals
```

---

## Fan-out / Fan-in with Dynamic Task Mapping

```python
@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def fan_out_fan_in():
    
    @task
    def get_partitions(**context) -> list:
        """Return list of partitions to process."""
        return [
            {"date": context["ds"], "region": "us-east"},
            {"date": context["ds"], "region": "us-west"},
            {"date": context["ds"], "region": "eu-west"},
            {"date": context["ds"], "region": "ap-southeast"},
        ]
    
    @task
    def process_partition(partition: dict) -> dict:
        """Process one partition (fan-out)."""
        # Each region processed in parallel
        region = partition["region"]
        date = partition["date"]
        print(f"Processing {region} for {date}")
        return {"region": region, "count": 1000, "status": "ok"}
    
    @task
    def aggregate_results(results: list) -> dict:
        """Aggregate all partition results (fan-in)."""
        total = sum(r["count"] for r in results)
        failed = [r["region"] for r in results if r["status"] != "ok"]
        
        if failed:
            raise ValueError(f"Regions failed: {failed}")
        
        print(f"Total records processed: {total}")
        return {"total": total, "partitions": len(results)}
    
    partitions = get_partitions()
    results = process_partition.expand(partition=partitions)  # fan-out
    aggregate_results(results)                                  # fan-in

pipeline = fan_out_fan_in()
```

---

## Error Handling Pattern

```python
import json
import urllib.request
from datetime import datetime

def create_slack_callback(webhook_url: str, channel: str = "#alerts"):
    def slack_on_failure(context):
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        run_id = context["run_id"]
        exception = str(context.get("exception", "Unknown error"))[:300]
        log_url = context["task_instance"].log_url
        
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🔴 Airflow Task Failed"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{run_id}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:*\n```{exception}```"}
            },
            {
                "type": "actions",
                "elements": [
                    {"type": "button", "text": {"type": "plain_text", "text": "View Logs"}, "url": log_url}
                ]
            }
        ]
        
        payload = {"channel": channel, "blocks": blocks}
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    
    return slack_on_failure


SLACK_WEBHOOK = "https://hooks.slack.com/services/..."
on_failure = create_slack_callback(SLACK_WEBHOOK, channel="#data-alerts")

with DAG(
    dag_id="production_pipeline",
    default_args={"on_failure_callback": on_failure, "retries": 3},
    on_failure_callback=on_failure,
) as dag:
    pass
```

---

## Retry with Exponential Backoff

```python
from datetime import timedelta

PythonOperator(
    task_id="flaky_api_call",
    python_callable=call_api,
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,        # 30s, 60s, 120s, 240s, 480s
    max_retry_delay=timedelta(hours=1),    # cap at 1 hour
    execution_timeout=timedelta(minutes=10),
)
```

---

## Dataset-based ELT Pattern

```python
from airflow.datasets import Dataset

raw_orders = Dataset("s3://bucket/raw/orders/")
clean_orders = Dataset("s3://bucket/clean/orders/")
reporting_orders = Dataset("s3://bucket/reporting/orders/")

# Stage 1: Ingestion (runs on schedule)
@dag(schedule_interval="@hourly", ...)
def ingest():
    @task(outlets=[raw_orders])
    def load_from_source(): pass
    load_from_source()

# Stage 2: Cleaning (triggered by raw_orders update)
@dag(schedule=[raw_orders])
def clean():
    @task(inlets=[raw_orders], outlets=[clean_orders])
    def validate_and_clean(): pass
    validate_and_clean()

# Stage 3: dbt reporting (triggered by clean_orders update)
@dag(schedule=[clean_orders])
def report():
    @task(inlets=[clean_orders], outlets=[reporting_orders])
    def dbt_models(): pass
    dbt_models()

ingest_pipeline = ingest()
clean_pipeline = clean()
report_pipeline = report()
```

---

## Multi-tenant Pipeline

```python
TEAMS = {
    "analytics": {"pool": "analytics_pool", "queue": "analytics_queue"},
    "ml": {"pool": "ml_pool", "queue": "ml_queue"},
    "finance": {"pool": "finance_pool", "queue": "finance_queue"},
}

def create_team_dag(team: str, config: dict):
    @dag(
        dag_id=f"{team}_daily_pipeline",
        schedule_interval="@daily",
        start_date=pendulum.datetime(2024, 1, 1),
        tags=[team, "daily"],
        default_args={
            "pool": config["pool"],
            "queue": config["queue"],
            "owner": team,
        },
    )
    def team_pipeline():
        @task
        def process(**context):
            print(f"Processing for team: {team}")
    
    return team_pipeline()

for team, config in TEAMS.items():
    dag = create_team_dag(team, config)
    globals()[dag.dag_id] = dag
```

---

## SLA-driven Workflow

```python
from datetime import timedelta

def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alert when SLA is missed."""
    # Send to PagerDuty or Slack
    for sla in slas:
        print(f"SLA missed: {sla.task_id} in {dag.dag_id}")

with DAG(
    dag_id="critical_pipeline",
    sla_miss_callback=sla_miss_alert,
    schedule_interval="0 6 * * *",  # 6am daily
    ...
) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        sla=timedelta(hours=1),       # must finish within 1h of DAG start
        priority_weight=10,           # run this first
        pool="critical_pool",
    )
    
    transform = PythonOperator(
        task_id="transform",
        sla=timedelta(hours=2),       # must finish within 2h
        priority_weight=9,
    )
    
    load = PythonOperator(
        task_id="load",
        sla=timedelta(hours=3),       # must finish within 3h (report by 9am)
        priority_weight=8,
    )
    
    extract >> transform >> load
```
