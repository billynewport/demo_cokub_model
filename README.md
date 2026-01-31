# DataSurface Bootstrap

A template for bootstrapping a DataSurface Yellow environment on Docker Desktop with Kubernetes.

## Prerequisites

- Docker Desktop with Kubernetes enabled
- `kubectl` and `helm` CLI tools
- GitHub Personal Access Token (for GitSync)

## Quick Start

### Step 1: Start PostgreSQL

```bash
cd docker/postgres
docker compose up -d
```

This creates `airflow_db` and `merge_db` databases.

### Step 2: Clone the Repository

```bash
git clone https://github.com/datasurface/demo1.git
cd demo1
```

### Step 3: Create Kubernetes Namespace and Secrets

```bash
export NAMESPACE="demo1"
kubectl create namespace $NAMESPACE

# PostgreSQL credentials
kubectl create secret generic postgres \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# Git credentials for model repository
kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_TOKEN \
  -n $NAMESPACE

# Git credentials for DAG sync (both v3 and v4 key formats)
kubectl create secret generic git-dags \
  --from-literal=GIT_SYNC_USERNAME=$GITHUB_USERNAME \
  --from-literal=GIT_SYNC_PASSWORD=$GITHUB_TOKEN \
  --from-literal=GITSYNC_USERNAME=$GITHUB_USERNAME \
  --from-literal=GITSYNC_PASSWORD=$GITHUB_TOKEN \
  -n $NAMESPACE
```

### Step 4: Install Airflow

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  -f helm/airflow-values.yaml \
  -n $NAMESPACE \
  --timeout 10m
```

### Step 5: Generate and Deploy Bootstrap

```bash
# Generate bootstrap artifacts
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  datasurface/datasurface:latest \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Demo_PSP \
  --rte-name demo

# Copy DAG to dags folder and push (GitSync will pull automatically)
cp generated_output/Demo_PSP/*_infrastructure_dag.py dags/
git add dags/
git commit -m "Add infrastructure DAG"
git push

# Apply init jobs
kubectl apply -f generated_output/Demo_PSP/*_ring1_init_job.yaml
kubectl apply -f generated_output/Demo_PSP/*_model_merge_job.yaml
```

## Project Structure

```
.
├── dags/                  # Airflow DAGs (GitSync pulls from here)
├── docker/
│   └── postgres/          # PostgreSQL compose setup
├── helm/
│   └── airflow-values.yaml  # Airflow Helm values for Docker Desktop
├── eco.py                 # Ecosystem definition
├── rte_demo.py           # Runtime environment configuration
└── README.md
```

## DataSurface Artifacts

See [ARTIFACTS.md](ARTIFACTS.md) for accessing DataSurface Docker images and Python modules.
