# 23 — Real-world Data Engineering Orchestration Patterns

## Full Lakehouse Pipeline (S3 → Spark → Delta → Snowflake)

```python
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pendulum

@dag(
    dag_id="lakehouse_pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "etl", "production"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
    },
    on_failure_callback=lambda ctx: print("DAG Failed — alert sent"),
)
def lakehouse_etl():
    
    # 1. Wait for raw data landing in S3
    wait_raw = S3KeySensor(
        task_id="wait_for_raw_data",
        bucket_name="my-data-lake",
        bucket_key="raw/orders/{{ ds }}/data_*.json",
        wildcard_match=True,
        aws_conn_id="aws_default",
        mode="reschedule",
        poke_interval=300,        # check every 5 min
        timeout=7200,             # fail after 2 hours
    )
    
    # 2. Spark: raw JSON → Delta Lake (silver layer)
    raw_to_silver = BashOperator(
        task_id="raw_to_silver",
        bash_command="""
            spark-submit \
                --master k8s://https://kubernetes.default.svc \
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                --packages io.delta:delta-core_2.12:2.4.0 \
                /jobs/raw_to_silver.py \
                --date {{ ds }} \
                --input s3://my-data-lake/raw/orders/{{ ds }}/ \
                --output s3://my-data-lake/silver/orders/
        """,
    )
    
    # 3. Data quality check on silver layer
    @task
    def validate_silver(**context) -> dict:
        import boto3
        import pyarrow.dataset as ds_pa
        
        date = context["ds"]
        path = f"s3://my-data-lake/silver/orders/date={date}/"
        
        # Read delta table partition
        dataset = ds_pa.dataset(path, format="parquet")
        count = dataset.count_rows()
        
        if count == 0:
            raise ValueError(f"Silver layer has 0 rows for {date}")
        
        null_check = sum(1 for batch in dataset.to_batches() 
                        for col in batch.columns 
                        if batch.column(col).null_count > 0)
        
        return {"row_count": count, "null_issues": null_check, "date": date}
    
    # 4. Spark: silver → gold (aggregations, joins)
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
            spark-submit /jobs/silver_to_gold.py \
                --date {{ ds }} \
                --silver-path s3://my-data-lake/silver/orders/ \
                --gold-path s3://my-data-lake/gold/orders_daily/
        """,
    )
    
    # 5. Load gold to Snowflake
    load_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id="snowflake_prod",
        sql="""
            -- Idempotent load: truncate partition first
            DELETE FROM analytics.orders_daily WHERE order_date = '{{ ds }}';
            
            COPY INTO analytics.orders_daily
            FROM (
                SELECT $1:order_date::DATE, $1:total_revenue::FLOAT,
                       $1:order_count::INT, $1:avg_order_value::FLOAT
                FROM @my_s3_stage/gold/orders_daily/date={{ ds }}/
            )
            FILE_FORMAT = (TYPE = PARQUET);
        """,
        warehouse="LOAD_WH",
    )
    
    # 6. Final validation
    @task
    def final_validation(**context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
        result = hook.get_first(
            f"SELECT order_count FROM analytics.orders_daily WHERE order_date = '{context['ds']}'"
        )
        
        if not result or result[0] < 100:
            raise ValueError(f"Final validation failed: only {result} rows in Snowflake")
        
        return result[0]
    
    # Wire the pipeline
    validation = validate_silver()
    wait_raw >> raw_to_silver >> validation >> silver_to_gold >> load_snowflake >> final_validation()

pipeline = lakehouse_etl()
```

---

## ML Pipeline Orchestration

```python
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

@dag(
    dag_id="ml_training_pipeline",
    schedule_interval="@weekly",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "training"],
)
def ml_pipeline():
    
    @task
    def prepare_features(**context) -> str:
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook(postgres_conn_id="feature_store")
        df = hook.get_pandas_df(f"""
            SELECT user_id, purchase_count_30d, avg_order_value, 
                   days_since_last_order, category_diversity
            FROM feature_store.user_features
            WHERE snapshot_date = '{context['ds']}'
        """)
        
        path = f"s3://ml-artifacts/features/{context['ds']}/train.parquet"
        df.to_parquet(path)
        return path
    
    @task
    def train_model(features_path: str, **context) -> dict:
        import mlflow
        import mlflow.sklearn
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score
        import pandas as pd
        
        mlflow.set_tracking_uri("http://mlflow.internal:5000")
        mlflow.set_experiment("churn_prediction")
        
        df = pd.read_parquet(features_path)
        X = df.drop("churn_label", axis=1)
        y = df["churn_label"]
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        with mlflow.start_run(run_name=f"weekly_{context['ds']}") as run:
            model = GradientBoostingClassifier(n_estimators=200, max_depth=5)
            model.fit(X_train, y_train)
            
            auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
            
            mlflow.log_metric("auc_roc", auc)
            mlflow.log_param("n_estimators", 200)
            mlflow.log_param("training_date", context["ds"])
            mlflow.sklearn.log_model(model, "model")
        
        return {"run_id": run.info.run_id, "auc": auc}
    
    @task
    def evaluate_model(metrics: dict) -> str:
        """Decide whether to promote model to production."""
        if metrics["auc"] >= 0.80:
            return "register_model"     # branch: promote
        else:
            return "skip_registration"  # branch: don't promote
    
    @task
    def register_model(metrics: dict) -> str:
        import mlflow
        
        mlflow.set_tracking_uri("http://mlflow.internal:5000")
        client = mlflow.MlflowClient()
        
        mv = client.create_model_version(
            name="churn_prediction_prod",
            source=f"runs:/{metrics['run_id']}/model",
            run_id=metrics["run_id"],
        )
        
        # Transition to production
        client.transition_model_version_stage(
            name="churn_prediction_prod",
            version=mv.version,
            stage="Production",
            archive_existing_versions=True,
        )
        
        return mv.version
    
    skip = EmptyOperator(task_id="skip_registration")
    
    @task
    def notify_team(version=None, **context):
        if version:
            print(f"Model v{version} promoted to production on {context['ds']}")
        else:
            print(f"Model not promoted on {context['ds']} — AUC below threshold")
    
    features = prepare_features()
    metrics = train_model(features)
    
    # Can't use TaskFlow branching directly — wire manually
    features >> metrics >> notify_team()

pipeline = ml_pipeline()
```

---

## Data Quality Framework Integration (Great Expectations)

```python
from airflow.decorators import dag, task
import pendulum

@dag(
    dag_id="etl_with_data_quality",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["data-quality"],
)
def etl_with_dq():
    
    @task
    def extract(**context) -> str:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
        
        hook = PostgresHook(postgres_conn_id="source_db")
        df = hook.get_pandas_df(
            f"SELECT * FROM orders WHERE order_date = '{context['ds']}'"
        )
        path = f"/tmp/orders_{context['ds_nodash']}.parquet"
        df.to_parquet(path)
        return path
    
    @task
    def validate_with_great_expectations(file_path: str, **context) -> str:
        import great_expectations as ge
        import pandas as pd
        
        df = pd.read_parquet(file_path)
        ge_df = ge.from_pandas(df)
        
        # Define expectations
        results = []
        
        # Check not null
        r = ge_df.expect_column_values_to_not_be_null("order_id")
        results.append(r)
        if not r["success"]:
            raise ValueError(f"Null order_ids found: {r['result']}")
        
        # Check value range
        r = ge_df.expect_column_values_to_be_between("amount", min_value=0, max_value=100000)
        results.append(r)
        if not r["success"]:
            raise ValueError(f"Amount out of range: {r['result']}")
        
        # Check row count
        r = ge_df.expect_table_row_count_to_be_between(min_value=100, max_value=10000000)
        results.append(r)
        if not r["success"]:
            raise ValueError(f"Row count issue: {r['result']}")
        
        # Check unique
        r = ge_df.expect_column_values_to_be_unique("order_id")
        results.append(r)
        if not r["success"]:
            duplicate_count = r["result"].get("unexpected_count", "?")
            raise ValueError(f"{duplicate_count} duplicate order_ids found")
        
        passed = sum(1 for r in results if r["success"])
        print(f"Data quality: {passed}/{len(results)} checks passed")
        return file_path
    
    @task
    def load(file_path: str, **context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
        
        df = pd.read_parquet(file_path)
        hook = PostgresHook(postgres_conn_id="target_db")
        
        hook.run(f"DELETE FROM orders_processed WHERE order_date = '{context['ds']}'")
        
        conn = hook.get_conn()
        df.to_sql("orders_processed", conn, if_exists="append", index=False, method="multi")
    
    raw = extract()
    validated = validate_with_great_expectations(raw)
    load(validated)

pipeline = etl_with_dq()
```

---

## Event-driven Multi-DAG Pipeline

```python
from airflow.datasets import Dataset
from airflow.decorators import dag, task
import pendulum

# Domain datasets
raw_events = Dataset("s3://datalake/raw/events/")
clean_events = Dataset("s3://datalake/clean/events/")
user_features = Dataset("s3://datalake/features/users/")
model_ready = Dataset("s3://datalake/models/production/")

# Ingestion team DAG
@dag(schedule_interval="*/15 * * * *", start_date=pendulum.datetime(2024, 1, 1))
def ingest_events():
    @task(outlets=[raw_events])
    def pull_from_kafka(): pass
    pull_from_kafka()

# Data engineering team DAG — event-driven
@dag(schedule=[raw_events])
def clean_events_pipeline():
    @task(inlets=[raw_events], outlets=[clean_events])
    def deduplicate_and_validate(): pass
    deduplicate_and_validate()

# Feature engineering team DAG — event-driven
@dag(schedule=[clean_events])
def feature_engineering():
    @task(inlets=[clean_events], outlets=[user_features])
    def compute_user_features(): pass
    compute_user_features()

# ML team DAG — triggered when features ready
@dag(schedule=[user_features])
def retrain_model():
    @task(inlets=[user_features], outlets=[model_ready])
    def train_and_evaluate(): pass
    train_and_evaluate()

# All pipelines loosely coupled via datasets
i = ingest_events()
c = clean_events_pipeline()
f = feature_engineering()
m = retrain_model()
```

---

## Disaster Recovery Strategy

```python
@dag(
    dag_id="dr_sync_metadata",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["ops", "dr"],
)
def dr_backup():
    
    @task
    def backup_metadata_db(**context):
        """Export DAG runs, task states, connections, variables to S3."""
        import subprocess
        import boto3
        from datetime import datetime
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = f"/tmp/airflow_backup_{timestamp}.sql"
        
        # pg_dump to local file
        subprocess.run([
            "pg_dump",
            "-h", "db.internal",
            "-U", "airflow",
            "-d", "airflow",
            "-f", backup_file,
            "--clean",
            "--if-exists",
        ], check=True, env={"PGPASSWORD": "airflow_password"})
        
        # Upload to S3
        s3 = boto3.client("s3")
        s3.upload_file(backup_file, "dr-bucket", f"airflow/backups/{timestamp}/metadata.sql")
        
        # Also export connections and variables as JSON
        result = subprocess.run(
            ["airflow", "connections", "export", "/tmp/connections.json"],
            capture_output=True, text=True,
        )
        s3.upload_file("/tmp/connections.json", "dr-bucket", f"airflow/backups/{timestamp}/connections.json")
        
        return f"s3://dr-bucket/airflow/backups/{timestamp}/"
    
    backup_metadata_db()

backup = dr_backup()
```
