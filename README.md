# DataSurface Bootstrap

A template for bootstrapping a DataSurface Yellow environment on Docker Desktop with Kubernetes.

## Prerequisites

- Docker Desktop with Kubernetes enabled
- `kubectl` and `helm` CLI tools
- GitHub Personal Access Token (for GitSync and model repository access)
- GitLab credentials for DataSurface Docker images (see [ARTIFACTS.md](ARTIFACTS.md))

## Environment Variables

Set these before starting:

```bash
export NAMESPACE="demo1"
export GITHUB_USERNAME="your-github-username"
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
export GITLAB_CUSTOMER_USER="gitlab+deploy-token-xxxxx"
export GITLAB_CUSTOMER_TOKEN="your-gitlab-deploy-token"
export DATASURFACE_VERSION="1.1.0"
```

## Quick Start

This guide walks you through:

1. Clone the model repository
2. Start PostgreSQL database container
3. Customize the model and push to your repository
4. Configure Kubernetes namespace, secrets, and image pull credentials
5. Install Airflow via Helm
6. Pull the DataSurface image
7. Generate bootstrap artifacts and deploy

### Step 1: Clone and Customize the Model

```bash
# Clone the template
git clone https://github.com/datasurface/demo1.git
cd demo1
```

### Step 1.5: Optional for AI assistants

There are several claude skills defined in the cloned repo. Use them to help with the steps below.

### Step 2: Start PostgreSQL

```bash
cd docker/postgres
docker compose up -d
cd ../..
```

This creates `airflow_db` and `merge_db` databases.

### Step 3: Customize the Model and push it to the demo1_actual repository

Customize the model for your environment:

**Edit `eco.py`:**

```python
GIT_REPO_OWNER: str = "yourorg"
GIT_REPO_NAME: str = "demo1_actual"
```

**Edit `rte_demo.py`:** Update the Docker image to use the GitLab registry:

```python
datasurfaceDockerImage="registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0",
```

**Edit `helm/airflow-values.yaml`:**

```yaml
dags:
  gitSync:
    repo: https://github.com/yourorg/demo1_airflow.git
```

Create empty repositories on GitHub:

- `yourorg/demo1_actual` - for the customized model
- `yourorg/demo1_airflow` - for Airflow DAGs (GitSync)

Push your customized model:

```bash
git remote set-url origin https://github.com/yourorg/demo1_actual.git
git add -A
git commit -m "Customize model for my environment"
git push -u origin main
```

### Step 4: Create Kubernetes Namespace and Secrets

```bash
# Create namespace
kubectl create namespace $NAMESPACE

# PostgreSQL credentials (for Airflow metadata)
kubectl create secret generic postgres \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# PostgreSQL credentials for merge database (used by DataSurface jobs)
kubectl create secret generic postgres-demo-merge \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# Git credentials for model repository (note: uppercase 'TOKEN')
kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_TOKEN \
  -n $NAMESPACE

# Git credentials for DAG sync (supports both v3 and v4 key formats)
kubectl create secret generic git-dags \
  --from-literal=GIT_SYNC_USERNAME=$GITHUB_USERNAME \
  --from-literal=GIT_SYNC_PASSWORD=$GITHUB_TOKEN \
  --from-literal=GITSYNC_USERNAME=$GITHUB_USERNAME \
  --from-literal=GITSYNC_PASSWORD=$GITHUB_TOKEN \
  -n $NAMESPACE

# GitLab registry credentials for DataSurface images
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE

# Attach image pull secret to default service account
kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{"imagePullSecrets": [{"name": "datasurface-registry"}]}'
```

### Step 5: Install Airflow

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  -f helm/airflow-values.yaml \
  -n $NAMESPACE \
  --timeout 10m
```

### Step 6: Pull DataSurface Image

```bash
# Login to GitLab registry
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"

# Pull the DataSurface image
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}
```

### Step 7: Generate and Deploy Bootstrap

```bash
# Generate bootstrap artifacts
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Demo_PSP \
  --rte-name demo

# Apply Kubernetes bootstrap (MCP server, network policies, etc.)
kubectl apply -f generated_output/Demo_PSP/kubernetes-bootstrap.yaml

# Push DAG to your GitSync repository
cd /path/to/demo1_airflow
mkdir -p dags
cp /path/to/demo1_actual/generated_output/Demo_PSP/*_infrastructure_dag.py dags/
git add dags/
git commit -m "Add infrastructure DAG"
git push

# Apply init and merge jobs
kubectl apply -f generated_output/Demo_PSP/demo_psp_ring1_init_job.yaml
kubectl apply -f generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
```

### Step 7: Verify Deployment

```bash
# Check all pods are running
kubectl get pods -n $NAMESPACE

# Check jobs completed successfully
kubectl get jobs -n $NAMESPACE

# Expected output:
# - airflow-* pods: Running
# - demo-psp-mcp-server-*: Running
# - demo-psp-ring1-init-*: Completed
# - demo-psp-model-merge-job-*: Completed
```

### Step 8: Access Airflow UI

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n $NAMESPACE
```

Open <http://localhost:8080> in your browser.

- Username: `admin`
- Password: `admin`

## Project Structure

```text
.
├── docker/
│   └── postgres/            # PostgreSQL compose setup
├── helm/
│   └── airflow-values.yaml  # Airflow Helm values for Docker Desktop
├── generated_output/        # Generated after running bootstrap (gitignored)
│   └── Demo_PSP/
│       ├── kubernetes-bootstrap.yaml
│       ├── demo_psp_infrastructure_dag.py
│       ├── demo_psp_ring1_init_job.yaml
│       ├── demo_psp_model_merge_job.yaml
│       └── demo_psp_reconcile_views_job.yaml
├── eco.py                   # Ecosystem definition
├── rte_demo.py              # Runtime environment configuration
└── README.md
```

## Secrets Reference

For detailed information on how Yellow converts model credential names to Kubernetes secrets and the expected environment variable format, see the [credential creation guide](.claude/skills/create-k8-credential/SKILLS.md).

| Secret Name | Keys | Purpose |
| ------------- | ------ | --------- |
| `postgres` | `USER`, `PASSWORD` | Airflow metadata database |
| `postgres-demo-merge` | `USER`, `PASSWORD` | DataSurface merge database |
| `git` | `TOKEN` | Model repository access |
| `git-dags` | `GITSYNC_USERNAME`, `GITSYNC_PASSWORD` | Airflow DAG sync |
| `datasurface-registry` | Docker registry auth | Pull DataSurface images |

## Troubleshooting

### ImagePullBackOff

If pods show `ImagePullBackOff`, verify:

1. `datasurface-registry` secret exists: `kubectl get secret datasurface-registry -n $NAMESPACE`
2. Default service account has imagePullSecrets: `kubectl get sa default -n $NAMESPACE -o yaml`
3. GitLab credentials are valid

### CreateContainerConfigError

Check for missing secrets:

```bash
kubectl describe pod <pod-name> -n $NAMESPACE | grep -A5 Events
```

Common missing secrets: `git`, `postgres-demo-merge`

### PostgreSQL Port Conflict

If Kubernetes jobs fail to connect to PostgreSQL with authentication errors or "database does not exist", you may have a local PostgreSQL instance running on port 5432 that conflicts with the Docker container.

Check for conflicting PostgreSQL:

```bash
# Check what's listening on 5432
lsof -i :5432

# If using Homebrew PostgreSQL, stop it
brew services stop postgresql@17  # or postgresql@16, postgresql, etc.
```

Kubernetes pods using `host.docker.internal:5432` will connect to whatever is listening on your host's port 5432, which may be a local PostgreSQL instead of the Docker container.

### Database Does Not Exist

If you see `database "merge_db" does not exist` or similar errors, the PostgreSQL init scripts may not have run. This happens when reusing an existing Docker volume.

Manually create the databases:

```bash
docker exec datasurface-postgres psql -U postgres -c "CREATE DATABASE airflow_db;" -c "CREATE DATABASE merge_db;"
```

Or remove the volume and restart:

```bash
cd docker/postgres
docker compose down -v
docker compose up -d
```

## DataSurface Artifacts

See [ARTIFACTS.md](ARTIFACTS.md) for accessing DataSurface Docker images and Python modules.
