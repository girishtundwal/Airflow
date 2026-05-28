# 18 — Managed Airflow Services

## Managed Services Comparison

| Feature | MWAA (AWS) | Cloud Composer (GCP) | Astro (Astronomer) |
|---------|-----------|---------------------|-------------------|
| Infrastructure | AWS Fargate | GKE (Autopilot) | Kubernetes (multi-cloud) |
| Metadata DB | Amazon Aurora PostgreSQL | Cloud SQL PostgreSQL | PostgreSQL |
| DAG storage | S3 bucket | GCS bucket | Container image / git |
| Worker scaling | Min/max workers | GKE Autopilot auto-scale | KEDA / replicas |
| Pricing | Per environment + worker hours | Per vCPU + memory | Per deployment (SaaS) |
| Airflow version lag | Behind latest | Behind latest | Close to latest (Astro Runtime) |
| SSH access to workers | No | No | No |
| Custom plugins | S3 plugins/ folder | GCS plugins/ folder | Container image |
| Support | AWS | Google | Astronomer |

---

## Amazon MWAA

### Architecture

```
Internet ──► ALB ──► MWAA Webserver (Fargate)
                              │
                     ┌────────┼────────┐
                     ▼        ▼        ▼
               Scheduler  Triggerer  Workers
               (Fargate)  (Fargate) (Fargate, auto-scale)
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
             Aurora PostgreSQL      Amazon S3
             (Metadata DB)     (DAGs, plugins, logs)
```

### Setup

```bash
# 1. Create S3 bucket for DAGs
aws s3 mb s3://my-airflow-dags

# 2. Upload DAGs
aws s3 sync ./dags/ s3://my-airflow-dags/dags/
aws s3 cp requirements.txt s3://my-airflow-dags/requirements.txt

# 3. Create MWAA environment via CLI (or Terraform)
aws mwaa create-environment \
    --name my-airflow \
    --airflow-version 2.9.2 \
    --execution-role-arn arn:aws:iam::123456789:role/mwaa-execution-role \
    --source-bucket-arn arn:aws:s3:::my-airflow-dags \
    --dag-s3-path dags/ \
    --requirements-s3-path requirements.txt \
    --network-configuration SubnetIds=subnet-xxx,subnet-yyy,SecurityGroupIds=sg-xxx \
    --webserver-access-mode PUBLIC_ONLY \
    --max-workers 10 \
    --min-workers 1 \
    --environment-class mw1.small
```

### IAM Execution Role (minimum permissions)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject", "s3:GetObjectVersion",
                "s3:ListBucket", "s3:ListBucketVersions",
                "s3:GetBucketVersioning"
            ],
            "Resource": [
                "arn:aws:s3:::my-airflow-dags",
                "arn:aws:s3:::my-airflow-dags/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup", "logs:CreateLogStream",
                "logs:PutLogEvents", "logs:GetLogEvents",
                "logs:GetLogRecord", "logs:GetLogDelivery",
                "logs:ListLogDeliveries"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:airflow-my-airflow-*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData",
                "sqs:ChangeMessageVisibility",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:ReceiveMessage",
                "sqs:SendMessage",
                "kms:Decrypt",
                "kms:DescribeKey",
                "kms:GenerateDataKey*",
                "kms:Encrypt"
            ],
            "Resource": "*"
        }
    ]
}
```

### MWAA requirements.txt

```text
# requirements.txt for MWAA
# IMPORTANT: must include --constraint flag or pin exact versions to avoid conflicts
apache-airflow-providers-snowflake==5.1.0
apache-airflow-providers-databricks==6.0.0
apache-airflow-providers-dbt-cloud==3.4.0
great-expectations==0.18.0
```

### MWAA Limitations

- No SSH to workers (Fargate)
- Airflow version changes require environment recreation
- All workers run same container image (no per-task images without KPO)
- Limited to AWS environment classes (mw1.small → mw1.xlarge)
- Network must be private subnets with NAT gateway
- Plugin changes require S3 upload + environment update

---

## Google Cloud Composer 2

### Architecture

```
Internet ──► Cloud Load Balancer ──► Webserver (GKE)
                                           │
                            ┌──────────────┼──────────────┐
                            ▼              ▼              ▼
                       Scheduler      Triggerer         Workers
                       (GKE Pod)      (GKE Pod)    (GKE Autopilot,
                                                    auto-scale)
                                           │
                              ┌────────────┴──────────┐
                              ▼                       ▼
                       Cloud SQL                  GCS Bucket
                       (PostgreSQL)           (DAGs, plugins, logs)
```

### Setup

```bash
# Create Composer 2 environment
gcloud composer environments create my-airflow \
    --location us-central1 \
    --image-version composer-2-airflow-2.9 \
    --machine-type n1-standard-4 \
    --node-count 3 \
    --service-account composer-sa@my-project.iam.gserviceaccount.com \
    --env-variables AIRFLOW__CORE__LOAD_EXAMPLES=False

# Upload DAGs to GCS
gcloud composer environments storage dags import \
    --environment my-airflow \
    --location us-central1 \
    --source ./dags/

# Install Python packages
gcloud composer environments update my-airflow \
    --location us-central1 \
    --update-pypi-packages-from-file requirements.txt

# Set Airflow variable
gcloud composer environments run my-airflow \
    --location us-central1 \
    variables -- set environment prod
```

### Composer 2 vs Composer 1

| Feature | Composer 1 | Composer 2 |
|---------|-----------|-----------|
| GKE mode | Standard | Autopilot (recommended) |
| Worker scaling | Manual | Automatic |
| Resource control | Coarse | Fine-grained |
| Cost | Higher baseline | Pay-per-use |
| Setup | More complex | Simpler |

---

## Astronomer / Astro CLI

### Astro CLI Quick Start

```bash
# Install (Mac)
brew install astro

# Or Linux/WSL
curl -sSL install.astronomer.io | sudo bash -s

# Verify
astro version

# Initialize new project
mkdir my-astro-project && cd my-astro-project
astro dev init

# Project structure:
# ├── dags/
# │   └── example_dag.py
# ├── plugins/
# ├── include/
# ├── tests/
# │   └── test_dag_integrity.py
# ├── requirements.txt
# ├── packages.txt          # OS packages (apt-get)
# ├── Dockerfile
# └── .astro/
#     └── config.yaml

# Start local environment
astro dev start
# Webserver: http://localhost:8080 (admin/admin)

# Stop
astro dev stop

# Restart with clean DB
astro dev restart

# Validate DAGs
astro dev parse

# Run a task locally
astro dev run tasks test my_dag my_task 2024-01-15

# Deploy to Astro Cloud
astro deploy --deployment-id <id>
```

### Astro Runtime

```dockerfile
# Dockerfile — Astro Runtime (includes Airflow + extras)
FROM quay.io/astronomer/astro-runtime:10.2.0
# astro-runtime:10.2.0 = Airflow 2.9.2 + astronomer extras

# Additional system packages
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*
USER astro

# Additional Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
```

---

## Cost Optimization

| Strategy | MWAA | Composer | Astro |
|----------|------|---------|-------|
| Right-size workers | Use mw1.small for low-load | Use smaller node pools | Adjust worker size |
| Autoscaling | Set min_workers=1 | Composer 2 auto-scales | KEDA/replica scaling |
| Schedule efficiency | Fewer frequent schedules | Same | Same |
| Use deferrable ops | Reduces worker hours | Reduces pod time | Reduces worker cost |
| Dev/staging | Pause dev environments overnight | Delete dev env when not needed | Pause deployments |

---

## Upgrade Strategies

```
For MWAA:
1. Create new environment with new Airflow version
2. Test all DAGs in new environment
3. Switch traffic (update cron jobs, API callers) to new environment
4. Delete old environment

For Composer 2:
gcloud composer environments update my-airflow \
    --image-version composer-2.6.6-airflow-2.9.2 \
    --location us-central1

For Self-hosted Helm:
1. Update chart version in values.yaml
2. Run `helm upgrade` on staging
3. Test thoroughly
4. Run `helm upgrade` on production
```

---

## Monitoring Managed Airflow

### MWAA

```bash
# CloudWatch logs (automatically collected)
# Log groups: airflow-{env-name}-DAGProcessing
#             airflow-{env-name}-Scheduler
#             airflow-{env-name}-Task
#             airflow-{env-name}-WebServer
#             airflow-{env-name}-Worker

# CloudWatch metrics
# - CeleryWorkerCount
# - RunningTasks
# - QueuedTasks
# - OrphanedTasks
# - TasksPendingImport
```

### Cloud Composer

```bash
# Cloud Monitoring auto-collects Airflow StatsD metrics
# Key metrics:
# - composer.googleapis.com/environment/healthy
# - composer.googleapis.com/environment/dag_run_count
# - composer.googleapis.com/environment/task_instance_count
# - composer.googleapis.com/environment/worker_pod_count
```
