# 11 — Airflow Hooks & Connections

## What are Hooks?

Hooks are **reusable interfaces to external systems**. They abstract connection management, authentication, and low-level API calls, so operators can focus on business logic.

- Every hook extends `BaseHook`
- Hooks use `conn_id` to look up credentials from Airflow Connections
- Operators use hooks internally — you can also use them directly in PythonOperator callables

---

## Hook Architecture

```python
from airflow.hooks.base import BaseHook

class BaseHook:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
    
    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        # Looks up in: env vars → secrets backend → metadata DB
        ...
    
    def get_conn(self):
        # Subclasses implement this
        raise NotImplementedError
```

**Connection resolution order**:
1. Environment variable `AIRFLOW_CONN_{CONN_ID_UPPER}`
2. Secrets backend (Vault, AWS SSM, etc.)
3. Metadata database (Connections table)

---

## PostgresHook

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

hook = PostgresHook(postgres_conn_id="my_postgres")

# Execute SQL (no return)
hook.run("INSERT INTO orders VALUES (1, '2024-01-15', 100.00)")
hook.run("UPDATE orders SET status = 'processed' WHERE date = '{{ ds }}'")

# Get records as list of tuples
rows = hook.get_records("SELECT id, amount FROM orders WHERE date = '2024-01-15'")
for row in rows:
    print(row[0], row[1])

# Get first row
first = hook.get_first("SELECT COUNT(*) FROM orders")
count = first[0]

# Get as pandas DataFrame
df = hook.get_pandas_df("SELECT * FROM orders WHERE date = %s", parameters=["2024-01-15"])

# Use raw connection (for complex operations)
with hook.get_conn() as conn:
    with conn.cursor() as cursor:
        cursor.execute("COPY orders FROM STDIN WITH CSV HEADER")
        # ...

# Bulk insert from CSV
hook.bulk_load("orders", "/tmp/orders.csv")

# Execute many
hook.run(
    "INSERT INTO orders (id, amount) VALUES (%s, %s)",
    parameters=[(1, 100), (2, 200), (3, 300)],
)
```

---

## S3Hook

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id="aws_default")

# Check if key exists
exists = hook.check_for_key(key="data/2024-01-15.parquet", bucket_name="my-bucket")

# Check if prefix has objects
has_files = hook.check_for_prefix(prefix="data/2024-01-15/", bucket_name="my-bucket", delimiter="/")

# List keys
keys = hook.list_keys(bucket_name="my-bucket", prefix="data/2024-01-15/")

# Download object
s3_obj = hook.get_key(key="data/file.parquet", bucket_name="my-bucket")
content = s3_obj.get()["Body"].read()

# Upload string
hook.load_string(
    string_data="id,name\n1,Alice\n2,Bob",
    key="data/users.csv",
    bucket_name="my-bucket",
    replace=True,
)

# Upload file
hook.load_file(
    filename="/tmp/output.parquet",
    key="processed/output.parquet",
    bucket_name="my-bucket",
    replace=True,
)

# Copy object
hook.copy_object(
    source_bucket_key="raw/file.csv",
    dest_bucket_key="processed/file.csv",
    source_bucket_name="source-bucket",
    dest_bucket_name="dest-bucket",
)

# Delete keys
hook.delete_objects(bucket="my-bucket", keys=["old/file1.csv", "old/file2.csv"])
```

---

## GCSHook

```python
from airflow.providers.google.cloud.hooks.gcs import GCSHook

hook = GCSHook(gcp_conn_id="google_cloud_default")

# Upload
hook.upload(bucket_name="my-bucket", object_name="data/file.parquet", filename="/tmp/file.parquet")

# Download
hook.download(bucket_name="my-bucket", object_name="data/file.parquet", filename="/tmp/file.parquet")

# Check exists
exists = hook.exists(bucket_name="my-bucket", object_name="data/file.parquet")

# List objects
objects = hook.list(bucket_name="my-bucket", prefix="data/2024-01-15/")

# Delete
hook.delete(bucket_name="my-bucket", object_name="old/file.parquet")
```

---

## HTTPHook

```python
from airflow.providers.http.hooks.http import HttpHook

hook = HttpHook(http_conn_id="my_api", method="GET")

# Simple GET
response = hook.run(
    endpoint="api/v1/data",
    data={"date": "2024-01-15", "limit": 1000},
    headers={"Accept": "application/json"},
)
data = response.json()

# POST request
hook_post = HttpHook(http_conn_id="my_api", method="POST")
response = hook_post.run(
    endpoint="api/v1/trigger",
    data={"job_id": "job_123"},
    headers={"Content-Type": "application/json", "Authorization": "Bearer token"},
    extra_options={"timeout": 30},
)

# With retry
response = hook.run_with_advanced_retry(
    _retry_args={"stop": stop_after_attempt(3), "wait": wait_fixed(5)},
    endpoint="api/v1/status",
)
```

---

## Custom Hook

```python
from airflow.hooks.base import BaseHook
import requests

class MyApiHook(BaseHook):
    """Hook for My Custom API."""
    
    conn_name_attr = "my_api_conn_id"        # attribute name for conn_id
    default_conn_name = "my_api_default"     # default connection ID
    conn_type = "http"
    hook_name = "My API"
    
    def __init__(self, my_api_conn_id: str = "my_api_default"):
        super().__init__()
        self.my_api_conn_id = my_api_conn_id
        self._session = None
    
    def get_conn(self) -> requests.Session:
        if self._session:
            return self._session
        
        conn = self.get_connection(self.my_api_conn_id)
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {conn.password}",
            "Content-Type": "application/json",
        })
        self._session = session
        return session
    
    def get_data(self, date: str) -> list:
        session = self.get_conn()
        conn = self.get_connection(self.my_api_conn_id)
        url = f"http://{conn.host}:{conn.port}/api/v1/data"
        
        response = session.get(url, params={"date": date}, timeout=30)
        response.raise_for_status()
        return response.json()["records"]
    
    def post_results(self, results: list) -> dict:
        session = self.get_conn()
        conn = self.get_connection(self.my_api_conn_id)
        url = f"http://{conn.host}:{conn.port}/api/v1/results"
        
        response = session.post(url, json={"data": results}, timeout=30)
        response.raise_for_status()
        return response.json()
```

---

## Connection Management

### Via Airflow UI
Navigate to Admin → Connections → Add a new record

### Via CLI

```bash
# Add connection
airflow connections add my_postgres \
    --conn-type postgres \
    --conn-host localhost \
    --conn-port 5432 \
    --conn-schema analytics \
    --conn-login airflow \
    --conn-password secret

# Add S3/AWS connection
airflow connections add aws_default \
    --conn-type aws \
    --conn-extra '{"aws_access_key_id": "AKIA...", "aws_secret_access_key": "...", "region_name": "us-east-1"}'

# List connections
airflow connections list

# Delete
airflow connections delete my_old_conn

# Export all connections
airflow connections export connections.json
```

### Via Environment Variables (highest priority)

```bash
# URI format
export AIRFLOW_CONN_MY_POSTGRES="postgresql://user:password@host:5432/dbname"
export AIRFLOW_CONN_MY_S3="aws://AKIA...KEY:SECRET@/?region_name=us-east-1"

# JSON format (more flexible)
export AIRFLOW_CONN_MY_POSTGRES='{
  "conn_type": "postgres",
  "host": "localhost",
  "port": 5432,
  "schema": "analytics",
  "login": "airflow",
  "password": "secret"
}'

# HTTP connection
export AIRFLOW_CONN_MY_API='{
  "conn_type": "http",
  "host": "api.example.com",
  "port": 443,
  "schema": "https",
  "password": "my-bearer-token",
  "extra": {"timeout": 30}
}'
```

---

## Secrets Backends

Configure where Airflow looks for connection and variable values (in addition to DB):

```ini
# airflow.cfg
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {
    "connections_path": "airflow/connections",
    "variables_path": "airflow/variables",
    "mount_point": "secret",
    "url": "http://vault:8200",
    "token": "your-vault-token"
}
```

### AWS Secrets Manager

```ini
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {
    "connections_prefix": "airflow/connections",
    "variables_prefix": "airflow/variables",
    "region_name": "us-east-1"
}
# Secret name format: airflow/connections/my_postgres
# Secret value: postgresql://user:pass@host:5432/db
```

### GCP Secret Manager

```ini
[secrets]
backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
backend_kwargs = {
    "connections_prefix": "airflow-connections",
    "variables_prefix": "airflow-variables",
    "project_id": "my-gcp-project"
}
# Secret name format: airflow-connections-my_postgres
```

### HashiCorp Vault

```bash
# Store connection in Vault
vault kv put secret/airflow/connections/my_postgres \
    conn_uri="postgresql://user:pass@host:5432/db"

# Or structured
vault kv put secret/airflow/connections/my_postgres \
    conn_type="postgres" \
    host="db.example.com" \
    port="5432" \
    schema="analytics" \
    login="airflow" \
    password="secret"
```
