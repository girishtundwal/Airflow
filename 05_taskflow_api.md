# 05 — TaskFlow API & Modern Airflow

## TaskFlow API Introduction

Introduced in **Airflow 2.0**, the TaskFlow API provides a decorator-based, Pythonic way to write DAGs. It eliminates boilerplate for XCom passing and makes pipelines look like regular Python functions.

Key decorators: `@dag`, `@task`, `@task_group`

---

## @dag Decorator

```python
from airflow.decorators import dag
import pendulum

@dag(
    dag_id="my_etl_dag",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "taskflow"],
    default_args={"retries": 2},
)
def my_etl_dag():
    # DAG body: define and wire tasks here
    pass

# Instantiate the DAG — REQUIRED to register with Airflow
dag_instance = my_etl_dag()
```

---

## @task Decorator

```python
from airflow.decorators import dag, task
import pendulum

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def my_pipeline():
    
    @task
    def extract() -> dict:
        # Return value is automatically pushed to XCom
        return {"records": [1, 2, 3], "source": "api"}
    
    @task
    def transform(raw_data: dict) -> list:
        # raw_data is automatically pulled from XCom (from extract's return)
        return [x * 2 for x in raw_data["records"]]
    
    @task
    def load(processed: list) -> int:
        print(f"Loading {len(processed)} records")
        # Simulate load
        return len(processed)
    
    # Wire tasks — pass return values as arguments
    raw = extract()
    processed = transform(raw)
    count = load(processed)

pipeline = my_pipeline()
```

### @task Options

```python
@task(
    task_id="custom_id",            # override auto-generated task_id
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=1),
    pool="heavy_tasks",
    queue="gpu_queue",
    trigger_rule="all_success",
    multiple_outputs=True,          # return dict → each key becomes separate XCom
    templates_dict={"date": "{{ ds }}"},
)
def my_task(date):
    return {"a": 1, "b": 2}        # pushed as two XCom keys: "a" and "b"
```

---

## XCom with TaskFlow (Automatic)

```python
@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def xcom_demo():
    
    @task
    def step1() -> dict:
        return {"value": 42, "name": "test"}
    
    @task
    def step2(data: dict) -> str:
        return f"Value: {data['value']}, Name: {data['name']}"
    
    @task
    def step3(message: str):
        print(message)
    
    # XCom flow is automatic — no xcom_push/pull needed
    result = step1()
    msg = step2(result)
    step3(msg)

demo = xcom_demo()
```

### Multiple outputs

```python
@task(multiple_outputs=True)
def get_config() -> dict:
    return {"host": "localhost", "port": 5432, "db": "mydb"}

@dag(...)
def pipeline():
    config = get_config()
    # config["host"], config["port"], config["db"] each become XCom keys
    use_config(config["host"], config["port"])
```

---

## Dynamic Task Mapping with expand()

Run the same task N times in parallel, where N is determined at runtime.

```python
from airflow.decorators import dag, task
import pendulum

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def dynamic_pipeline():
    
    @task
    def get_files() -> list:
        # At runtime, return a list of things to process
        return ["file_a.csv", "file_b.csv", "file_c.csv"]
    
    @task
    def process_file(filename: str) -> str:
        print(f"Processing {filename}")
        return f"done:{filename}"
    
    @task
    def summarize(results: list):
        print(f"Processed {len(results)} files: {results}")
    
    files = get_files()
    
    # expand() creates one task instance per item in the list
    processed = process_file.expand(filename=files)
    
    # summarize receives a list of all results
    summarize(processed)

dynamic = dynamic_pipeline()
```

---

## partial().expand() — Mixed Static + Dynamic

```python
@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def partial_expand_demo():
    
    @task
    def process(filename: str, env: str, batch_size: int) -> str:
        return f"Processed {filename} in {env} with batch={batch_size}"
    
    # partial() fixes static args, expand() provides the dynamic arg
    results = process.partial(
        env="prod",
        batch_size=1000,
    ).expand(
        filename=["file_a.csv", "file_b.csv", "file_c.csv"],
    )

demo = partial_expand_demo()
```

---

## expand_kwargs() — Multiple Dynamic Arguments

```python
@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def expand_kwargs_demo():
    
    @task
    def load_table(table: str, schema: str) -> int:
        print(f"Loading {schema}.{table}")
        return 100
    
    # Each dict in the list maps to one task instance
    counts = load_table.expand_kwargs([
        {"table": "orders", "schema": "raw"},
        {"table": "customers", "schema": "raw"},
        {"table": "products", "schema": "staging"},
    ])

demo = expand_kwargs_demo()
```

---

## Task Groups

```python
from airflow.decorators import dag, task, task_group
import pendulum

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def grouped_pipeline():
    
    @task_group(group_id="extract_layer")
    def extract_all():
        @task
        def extract_orders() -> list:
            return [{"id": 1}]
        
        @task
        def extract_customers() -> list:
            return [{"id": 2}]
        
        orders = extract_orders()
        customers = extract_customers()
        return orders, customers
    
    @task_group(group_id="load_layer")
    def load_all(orders, customers):
        @task
        def load_orders(data: list):
            print(f"Loading {len(data)} orders")
        
        @task
        def load_customers(data: list):
            print(f"Loading {len(data)} customers")
        
        load_orders(orders)
        load_customers(customers)
    
    orders, customers = extract_all()
    load_all(orders, customers)

pipeline = grouped_pipeline()
```

---

## Mixing TaskFlow with Traditional Operators

```python
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

@dag(schedule_interval="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False)
def mixed_pipeline():
    
    @task
    def get_config(**context) -> dict:
        return {"date": context["ds"], "table": "orders"}
    
    # Traditional operator — pass XCom value as template
    run_sql = PostgresOperator(
        task_id="run_sql",
        postgres_conn_id="my_db",
        sql="SELECT COUNT(*) FROM orders WHERE date = '{{ ti.xcom_pull(task_ids=\"get_config\")[\"date\"] }}'",
    )
    
    # Connect TaskFlow task to traditional operator
    config = get_config()
    config >> run_sql       # traditional dependency syntax still works

pipeline = mixed_pipeline()
```

---

## Reusable Tasks Across DAGs

```python
# shared_tasks/data_tasks.py
from airflow.decorators import task

@task
def validate_schema(data: list, expected_columns: list) -> list:
    for row in data:
        missing = set(expected_columns) - set(row.keys())
        if missing:
            raise ValueError(f"Missing columns: {missing}")
    return data

@task  
def send_slack_alert(message: str):
    # send to slack
    pass
```

```python
# dags/my_dag.py
from shared_tasks.data_tasks import validate_schema, send_slack_alert

@dag(...)
def my_dag():
    raw = extract()
    validated = validate_schema(raw, expected_columns=["id", "name", "date"])
    loaded = load(validated)
```

---

## TaskFlow vs Traditional Operators

| Aspect | TaskFlow API | Traditional Operators |
|--------|-------------|----------------------|
| Syntax | `@task` decorator on functions | Instantiate operator class |
| XCom | Automatic (function return/args) | Manual `xcom_push`/`xcom_pull` |
| Type hints | Supported | No |
| Code location | Inline in DAG | Separate from DAG definition |
| Reusability | Import task functions | Subclass `BaseOperator` |
| Non-Python tasks | Use `@task.bash`, `@task.docker` | Use BashOperator, DockerOperator |
| Dynamic mapping | `expand()`, `partial()` | No equivalent |
| Learning curve | Lower (Pythonic) | Higher |

---

## Modern Airflow Best Practices

1. **Use TaskFlow API** for pure Python tasks — cleaner, less boilerplate
2. **Use `@task_group`** to logically group related tasks in UI
3. **Use `expand()`** instead of manually creating N tasks in a loop
4. **Return small values** from `@task` — XCom is not for DataFrames
5. **Use `multiple_outputs=True`** for dict-returning tasks to keep XCom keys clean
6. **Keep `@dag` decorator** on a function, instantiate it at module level
7. **Don't mix** `return dag` with `globals()[dag_id] = dag` — just instantiate at bottom
8. **Use type hints** on `@task` functions — improves readability and IDE support
