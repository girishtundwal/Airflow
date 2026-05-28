# 17 — Airflow on Kubernetes

## Overview

Kubernetes is the production-grade deployment platform for Airflow. Key benefits:
- Declarative infrastructure (Helm chart)
- Auto-scaling workers
- Resource isolation per task (KubernetesExecutor)
- Built-in HA (multiple replicas)
- Cloud-native secrets management

---

## Kubernetes Executor

Each task runs in a dedicated, ephemeral Kubernetes pod. The pod is created, runs the task, and is deleted.

```
┌───────────────┐ create pod ┌──────────────────┐
│  Scheduler    │───────────►│  Kubernetes API  │
│  (watches     │◄───────────│  Server          │
│   pod status) │  events    └────────┬─────────┘
└───────────────┘                     │ schedule
                              ┌───────▼───────┐
                              │  Worker Pod   │
                              │ - runs task   │
                              │ - gets deleted│
                              └───────────────┘
```

```ini
[core]
executor = KubernetesExecutor

[kubernetes]
namespace = airflow
pod_template_file = /opt/airflow/pod_templates/worker.yaml
worker_container_repository = apache/airflow
worker_container_tag = 2.9.2
delete_worker_pods = True
delete_worker_pods_on_failure = False  # keep failed pods for debugging
in_cluster = True
worker_pods_creation_batch_size = 16   # max pods created per scheduler loop
```

---

## Pod Template File

```yaml
# pod_templates/worker.yaml
apiVersion: v1
kind: Pod
metadata:
  name: placeholder-name
  namespace: airflow
  labels:
    tier: airflow
    component: worker
    release: airflow
spec:
  serviceAccountName: airflow-worker
  restartPolicy: Never
  
  initContainers: []
  
  containers:
    - name: base
      image: my-ecr.amazonaws.com/airflow:latest
      imagePullPolicy: Always
      
      resources:
        requests:
          memory: "1Gi"
          cpu: "500m"
        limits:
          memory: "2Gi"
          cpu: "1"
      
      env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: KubernetesExecutor
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: sql-alchemy-conn
        - name: AIRFLOW__CORE__FERNET_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-secrets
              key: fernet-key
      
      volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
        - name: logs
          mountPath: /opt/airflow/logs
  
  volumes:
    - name: dags
      persistentVolumeClaim:
        claimName: airflow-dags
    - name: logs
      persistentVolumeClaim:
        claimName: airflow-logs
```

---

## KubernetesPodOperator

Run any Docker container as a task — most flexible operator for K8s deployments.

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Basic usage
basic_pod = KubernetesPodOperator(
    task_id="run_spark",
    name="spark-job-{{ ds_nodash }}",    # pod name (must be unique)
    namespace="airflow",
    image="my-spark:3.4",
    cmds=["spark-submit"],
    arguments=[
        "--master", "k8s://https://kubernetes.default.svc",
        "--conf", "spark.kubernetes.namespace=spark",
        "app.py",
        "--date", "{{ ds }}",
    ],
    in_cluster=True,
    get_logs=True,
    is_delete_operator_pod=True,
    service_account_name="spark-sa",
)

# Advanced: resource limits, env vars, secrets, volumes
advanced_pod = KubernetesPodOperator(
    task_id="ml_training",
    name="ml-train-{{ run_id | replace('_', '-') | lower | truncate(50, True, '') }}",
    namespace="ml",
    image="my-ml-image:latest",
    cmds=["python", "-m", "training.main"],
    arguments=["--date", "{{ ds }}", "--epochs", "10"],
    
    # Environment variables
    env_vars=[
        k8s.V1EnvVar(name="EXPERIMENT_NAME", value="{{ dag_run.conf.get('experiment', 'default') }}"),
        k8s.V1EnvVar(
            name="DB_PASSWORD",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(name="db-creds", key="password")
            ),
        ),
    ],
    
    # Resource requests/limits
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "2", "memory": "4Gi", "nvidia.com/gpu": "1"},
        limits={"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": "1"},
    ),
    
    # Node selector (run only on GPU nodes)
    node_selector={"cloud.google.com/gke-accelerator": "nvidia-tesla-t4"},
    
    # Volume mounts
    volumes=[
        k8s.V1Volume(
            name="data",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="ml-data"),
        ),
        k8s.V1Volume(
            name="model-output",
            empty_dir=k8s.V1EmptyDirVolumeSource(),
        ),
    ],
    volume_mounts=[
        k8s.V1VolumeMount(name="data", mount_path="/data", read_only=True),
        k8s.V1VolumeMount(name="model-output", mount_path="/output"),
    ],
    
    # Tolerations for spot/preemptible nodes
    tolerations=[
        k8s.V1Toleration(key="nvidia.com/gpu", operator="Exists", effect="NoSchedule"),
    ],
    
    in_cluster=True,
    get_logs=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
)
```

---

## Helm Deployment

```bash
# Add repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    -f values.yaml \
    --version 1.13.1 \
    --debug

# Upgrade
helm upgrade airflow apache-airflow/airflow \
    --namespace airflow \
    -f values.yaml \
    --wait --timeout=10m

# Rollback
helm rollback airflow 1 --namespace airflow

# Uninstall
helm uninstall airflow --namespace airflow

# Check status
kubectl get pods -n airflow
kubectl get services -n airflow
```

---

## Persistent Volumes

```yaml
# DAGs PVC (ReadWriteMany for multi-node access)
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-dags
  namespace: airflow
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: efs-sc    # AWS EFS / GCP Filestore / Azure Files
  resources:
    requests:
      storage: 10Gi

---
# Logs PVC
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-logs
  namespace: airflow
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: efs-sc
  resources:
    requests:
      storage: 50Gi
```

---

## KEDA Autoscaling (CeleryExecutor)

```yaml
# Autoscale workers based on Celery queue depth
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: airflow-worker
  namespace: airflow
spec:
  scaleTargetRef:
    name: airflow-worker
    kind: Deployment
  minReplicaCount: 1
  maxReplicaCount: 20
  cooldownPeriod: 60
  pollingInterval: 30
  triggers:
    - type: redis
      metadata:
        address: "redis:6379"
        listName: "celery"
        listLength: "5"         # scale up when queue > 5 tasks per worker
```

---

## Cloud-specific Setup

### AWS EKS

```bash
# IAM Role for Service Account (IRSA)
# Airflow workers need S3, Glue, etc. access

eksctl create iamserviceaccount \
    --cluster my-cluster \
    --namespace airflow \
    --name airflow-worker \
    --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess \
    --attach-policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess \
    --approve

# ALB Ingress for webserver
# Install AWS Load Balancer Controller
helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
    -n kube-system \
    --set clusterName=my-cluster
```

### GCP GKE

```bash
# Workload Identity — pods authenticate as GCP service account
# Create GSA
gcloud iam service-accounts create airflow-worker \
    --display-name="Airflow Worker"

# Grant permissions
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:airflow-worker@my-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Bind to K8s SA
gcloud iam service-accounts add-iam-policy-binding \
    airflow-worker@my-project.iam.gserviceaccount.com \
    --member="serviceAccount:my-project.svc.id.goog[airflow/airflow-worker]" \
    --role="roles/iam.workloadIdentityUser"

kubectl annotate serviceaccount airflow-worker \
    --namespace airflow \
    iam.gke.io/gcp-service-account=airflow-worker@my-project.iam.gserviceaccount.com
```

---

## NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: airflow-policy
  namespace: airflow
spec:
  podSelector: {}    # applies to all pods in namespace
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: airflow
    - ports:
        - port: 8080  # webserver
  egress:
    - to:
        - podSelector: {}   # within namespace
    - to: []
      ports:
        - port: 443    # HTTPS to external APIs
        - port: 5432   # PostgreSQL
        - port: 6379   # Redis
        - port: 80
```

---

## Production Helm Values Summary

```yaml
# values-prod.yaml
executor: CeleryExecutor

scheduler:
  replicas: 2
  resources:
    requests: {memory: "2Gi", cpu: "500m"}
    limits: {memory: "4Gi", cpu: "2"}

webserver:
  replicas: 2
  resources:
    requests: {memory: "1Gi", cpu: "250m"}
    limits: {memory: "2Gi", cpu: "1"}
  service:
    type: ClusterIP   # use Ingress in front

workers:
  replicas: 3
  keda:
    enabled: true
    minReplicaCount: 1
    maxReplicaCount: 20
  resources:
    requests: {memory: "2Gi", cpu: "500m"}
    limits: {memory: "4Gi", cpu: "2"}

triggerer:
  enabled: true
  replicas: 1

dags:
  gitSync:
    enabled: true
    repo: "git@github.com:myorg/airflow-dags.git"
    branch: main
    sshKeySecret: airflow-ssh-secret

logs:
  persistence:
    enabled: true
    storageClassName: efs-sc
    size: 100Gi

postgresql:
  enabled: false

externalDatabase:
  type: postgres
  host: "airflow.rds.amazonaws.com"
  passwordSecret: airflow-db-secret

redis:
  enabled: false

externalRedis:
  host: "airflow.elasticache.amazonaws.com"
```
