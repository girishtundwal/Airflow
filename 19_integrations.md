# 19 — Airflow Integration with Data Engineering Stack

## Airflow + Spark

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

# Local/standalone Spark cluster
spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/opt/spark/jobs/transform.py",
    conn_id="spark_default",                      # spark connection
    java_class="com.example.MainClass",
    application_args=["--date", "{{ ds }}"],
    conf={
        "spark.executor.memory": "4g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "5",
    },
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
)

# Dataproc (GCP)
dataproc_job = DataprocSubmitJobOperator(
    task_id="dataproc_spark",
    job={
        "reference": {"project_id": "my-project"},
        "placement": {"cluster_name": "my-cluster"},
        "pyspark_job": {
            "main_python_file_uri": "gs://bucket/jobs/transform.py",
            "args": ["--date", "{{ ds }}"],
        },
    },
    region="us-central1",
    project_id="my-project",
    gcp_conn_id="google_cloud_default",
)
```

---

## Airflow + Databricks

```python
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
    DatabricksCreateJobsOperator,
)

# Trigger existing Databricks job
trigger_existing = DatabricksRunNowOperator(
    task_id="run_databricks_job",
    databricks_conn_id="databricks_default",
    job_id=12345,                                 # existing job ID
    notebook_params={"date": "{{ ds }}", "env": "prod"},
    python_params=["--date", "{{ ds }}"],
)

# Submit ad-hoc run
submit_run = DatabricksSubmitRunOperator(
    task_id="submit_notebook",
    databricks_conn_id="databricks_default",
    new_cluster={
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 4,
        "aws_attributes": {"instance_profile_arn": "arn:aws:iam::..."},
    },
    notebook_task={
        "notebook_path": "/Shared/etl/transform_orders",
        "base_parameters": {"date": "{{ ds }}"},
    },
    wait_for_termination=True,
    polling_period_seconds=30,
)

# Connection setup:
# conn_id: databricks_default
# conn_type: databricks
# host: https://adb-xxx.azuredatabricks.net
# extra: {"token": "dapi..."}
```

---

## Airflow + dbt

### Option 1: BashOperator (simplest)

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
        cd /opt/dbt/my_project &&
        dbt run --target {{ dag_run.conf.get('target', 'prod') }} \
                --select tag:daily \
                --vars '{"execution_date": "{{ ds }}"}'
    """,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/dbt/my_project && dbt test --target prod --select tag:daily",
)

dbt_run >> dbt_test
```

### Option 2: Cosmos (astronomer-cosmos) — Best Practice

```python
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"database": "ANALYTICS", "schema": "TRANSFORMED"},
    ),
)

dbt_dag = DbtDag(
    dag_id="dbt_transform",
    project_config=ProjectConfig(dbt_project_path="/usr/local/airflow/dbt/my_project"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
    render_config=RenderConfig(select=["tag:daily"]),
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
)
```

### Option 3: dbt Cloud

```python
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

dbt_cloud_run = DbtCloudRunJobOperator(
    task_id="run_dbt_cloud",
    job_id=12345,
    dbt_cloud_conn_id="dbt_cloud_default",
    check_interval=10,
    timeout=3600,
    additional_run_config={"git_sha": "{{ var.value.current_sha }}"},
)
```

---

## Airflow + Snowflake

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

# Run SQL
transform = SnowflakeOperator(
    task_id="transform",
    snowflake_conn_id="snowflake_default",
    sql="""
        INSERT INTO analytics.orders_daily
        SELECT
            order_date,
            SUM(amount) AS total_amount,
            COUNT(*) AS order_count
        FROM staging.raw_orders
        WHERE order_date = '{{ ds }}'
        GROUP BY order_date
    """,
    autocommit=True,
    warehouse="TRANSFORM_WH",
    database="ANALYTICS",
    schema="STAGING",
    role="TRANSFORM_ROLE",
)

# S3 to Snowflake COPY
load = S3ToSnowflakeOperator(
    task_id="load_from_s3",
    snowflake_conn_id="snowflake_default",
    s3_keys=["data/orders/{{ ds }}/part*.parquet"],
    table="raw_orders",
    schema="staging",
    stage="my_s3_stage",
    file_format="(TYPE = PARQUET)",
    aws_conn_id="aws_default",
)
```

---

## Airflow + BigQuery

```python
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Run query
run_query = BigQueryInsertJobOperator(
    task_id="run_query",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                SELECT date, SUM(amount) as total
                FROM `project.dataset.orders`
                WHERE date = '{{ ds }}'
                GROUP BY date
            """,
            "destinationTable": {
                "projectId": "my-project",
                "datasetId": "analytics",
                "tableId": "orders_daily${{ ds_nodash }}",
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "useLegacySql": False,
        }
    },
    location="US",
)

# Data quality check
quality_check = BigQueryCheckOperator(
    task_id="check_row_count",
    sql="SELECT COUNT(*) > 0 FROM `project.analytics.orders_daily` WHERE date = '{{ ds }}'",
    gcp_conn_id="google_cloud_default",
)

# GCS to BigQuery
gcs_to_bq = GCSToBigQueryOperator(
    task_id="gcs_to_bq",
    bucket="my-data-bucket",
    source_objects=["data/orders/{{ ds }}/*.parquet"],
    destination_project_dataset_table="project:dataset.table",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    gcp_conn_id="google_cloud_default",
)
```

---

## Airflow + Kafka

```python
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

def produce_messages(ds):
    yield {"key": ds, "value": f"pipeline_complete_{ds}"}

# Produce message to Kafka topic
notify = ProduceToTopicOperator(
    task_id="notify_kafka",
    topic="pipeline-events",
    kafka_config_id="kafka_default",
    producer_function=produce_messages,
    producer_function_kwargs={"ds": "{{ ds }}"},
)

# Wait for Kafka message
wait_for_message = AwaitMessageSensor(
    task_id="wait_for_upstream",
    topics=["upstream-events"],
    kafka_config_id="kafka_default",
    apply_function="my_module.check_message",
    poll_timeout=1.0,
    poke_interval=60,
    mode="reschedule",
    timeout=3600,
)
```

---

## Airflow + MLflow

```python
from airflow.decorators import dag, task
import pendulum

@dag(schedule_interval="@weekly", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def ml_pipeline():
    
    @task
    def prepare_features(**context) -> str:
        import pandas as pd
        # Feature engineering logic
        df = pd.DataFrame({"feature1": [1,2,3], "target": [0,1,1]})
        path = f"/tmp/features_{context['ds_nodash']}.parquet"
        df.to_parquet(path)
        return path
    
    @task
    def train_model(features_path: str, **context) -> str:
        import mlflow
        import mlflow.sklearn
        from sklearn.ensemble import RandomForestClassifier
        import pandas as pd
        
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("my_experiment")
        
        df = pd.read_parquet(features_path)
        X, y = df.drop("target", axis=1), df["target"]
        
        with mlflow.start_run(run_name=f"run_{context['ds']}") as run:
            model = RandomForestClassifier(n_estimators=100)
            model.fit(X, y)
            
            accuracy = model.score(X, y)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_param("n_estimators", 100)
            mlflow.log_param("training_date", context["ds"])
            mlflow.sklearn.log_model(model, "model")
            
            return run.info.run_id
    
    @task
    def register_model(run_id: str, **context):
        import mlflow
        mlflow.set_tracking_uri("http://mlflow:5000")
        
        client = mlflow.MlflowClient()
        # Register model if accuracy exceeds threshold
        run = client.get_run(run_id)
        if float(run.data.metrics.get("accuracy", 0)) > 0.90:
            client.create_registered_model("production_model")
            client.create_model_version(
                name="production_model",
                source=f"runs:/{run_id}/model",
                run_id=run_id,
            )
    
    features = prepare_features()
    run_id = train_model(features)
    register_model(run_id)

pipeline = ml_pipeline()
```

---

## Airflow + AWS Services

```python
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# AWS Glue
glue_job = GlueJobOperator(
    task_id="run_glue",
    job_name="my-glue-job",
    script_args={"--date": "{{ ds }}"},
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True,
)

# Athena query
athena_query = AthenaOperator(
    task_id="athena_query",
    query="SELECT COUNT(*) FROM orders WHERE dt = '{{ ds }}'",
    database="my_database",
    output_location="s3://my-bucket/athena-results/",
    aws_conn_id="aws_default",
)
```

---

## End-to-End Lakehouse Pipeline

```python
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pendulum

@dag(
    dag_id="lakehouse_pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "etl"],
)
def lakehouse_etl():
    
    # 1. Wait for raw data
    wait_for_raw = S3KeySensor(
        task_id="wait_for_raw",
        bucket_name="raw-data-bucket",
        bucket_key="orders/{{ ds }}/data.json",
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
    )
    
    # 2. Process with Spark → Delta Lake
    spark_process = SparkSubmitOperator(
        task_id="spark_to_delta",
        application="/jobs/raw_to_delta.py",
        application_args=["--date", "{{ ds }}", "--env", "prod"],
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        packages="io.delta:delta-core_2.12:2.4.0",
    )
    
    # 3. dbt transformations
    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command="dbt run --select tag:orders --vars '{\"date\": \"{{ ds }}\"}'",
    )
    
    # 4. Load to Snowflake
    load_snowflake = SnowflakeOperator(
        task_id="load_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="CALL proc_load_orders('{{ ds }}')",
    )
    
    # 5. Data quality check
    @task
    def quality_check(**context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        count = hook.get_first(
            f"SELECT COUNT(*) FROM orders WHERE order_date = '{context['ds']}'"
        )[0]
        if count < 100:
            raise ValueError(f"Quality check failed: only {count} rows")
        return count
    
    wait_for_raw >> spark_process >> dbt_transform >> load_snowflake >> quality_check()

pipeline = lakehouse_etl()
```
