# HOWTO: Setup YellowDataPlatform with Airflow 3.x and Helm

## Overview

This guide covers deploying YellowDataPlatform on Kubernetes using **Airflow 3.1.6** with **Helm**. It's designed for multi-node clusters with external PostgreSQL.

**Supported cluster types:**

- **Production clusters** (EKS, AKS, GKE, kubeadm) - standard Kubernetes, uses imagePullSecrets
- **K3s** (dev/edge) - lightweight alternative with simpler registry auth

This guide uses **kubeadm/containerd** patterns as the primary reference since they match production environments (EKS, AKS, GKE). K3s-specific shortcuts are noted where applicable.

For detailed architecture, fixes, and troubleshooting, see `../../tmp/airflow3x_status.md`.

## Prerequisites

- Kubernetes cluster (K3s or kubeadm with containerd)
- Helm 3.x installed
- External PostgreSQL database (or Tailscale-accessible postgres host)
- Docker for building/pulling images
- Docker Hub credentials (for private `datasurface/datasurface` image)
- GitHub repository with ecosystem model (`demo_bootstrap`)
- GitHub Personal Access Token
- NFS client installed on all nodes (required for Longhorn RWX volumes)

### Node Preparation

**Install NFS client on all Kubernetes nodes** (required for Longhorn ReadWriteMany volumes):

```bash
# On each node (Ubuntu/Debian)
sudo apt-get update && sudo apt-get install -y nfs-common

# Verify installation
dpkg -l | grep nfs-common
```

Without `nfs-common`, pods using shared volumes (like airflow-dags) will fail with mount errors.

## Phase 1: Cluster Preparation

### Step 1: Verify Cluster Access

```bash
# For k3s, use sudo
sudo kubectl get nodes

# Expected output:
# NAME        STATUS   ROLES                  AGE   VERSION
# kub-test    Ready    control-plane,master   30d   v1.31.2+k3s1
# kub-test2   Ready    <none>                 30d   v1.31.2+k3s1
```

### Step 2: Install Helm (if not already installed)

```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm version
```

### Step 3: Add Apache Airflow Helm Repository

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

## Phase 2: Create Namespace and Secrets

### Step 1: Create Namespace

```bash
export NAMESPACE="demo1"
sudo kubectl create namespace $NAMESPACE
```

### Step 2: Create Required Secrets

```bash
# Docker Hub credentials (for pulling private datasurface image)
sudo kubectl create secret docker-registry dockerhub-creds \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=datasurface \
  --docker-password=YOUR_DOCKER_HUB_PAT \
  --docker-email=your@email.com \
  -n $NAMESPACE

# PostgreSQL credentials (for merge database)
sudo kubectl create secret generic postgres \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# GitHub credentials (for model repository access)
sudo kubectl create secret generic git \
  --from-literal=TOKEN=your-github-personal-access-token \
  -n $NAMESPACE

# GitHub credentials (for DAG git-sync - both v3 and v4 key formats)
sudo kubectl create secret generic git-dags \
  --from-literal=GIT_SYNC_USERNAME=your-github-username \
  --from-literal=GIT_SYNC_PASSWORD=your-github-personal-access-token \
  --from-literal=GITSYNC_USERNAME=your-github-username \
  --from-literal=GITSYNC_PASSWORD=your-github-personal-access-token \
  -n $NAMESPACE

# DataTransformer credentials
sudo kubectl create secret generic mask-dt-cred \
  --from-literal=USER=mask_dt_cred \
  --from-literal=PASSWORD=Passw0rd123! \
  -n $NAMESPACE

# SQL Server credentials (if using SQL Server CRG)
sudo kubectl create secret generic sa \
  --from-literal=USER=sa \
  --from-literal=PASSWORD='pass@w0rd' \
  -n $NAMESPACE
```

**Secret Key Format:**

| Credential Type | Keys |
| ----------------- | ------ |
| Database | `USER`, `PASSWORD` |
| Token/PAT | `token` |
| API Key Pair | `api_key`, `api_secret` |

### Step 3: Create RBAC for Secret Access

Airflow 3.x SDK task runner needs direct Kubernetes API access to secrets:

```bash
cat <<EOF | sudo kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-secret-reader
  namespace: $NAMESPACE
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-secret-reader-binding
  namespace: $NAMESPACE
subjects:
- kind: ServiceAccount
  name: airflow-scheduler
  namespace: $NAMESPACE
- kind: ServiceAccount
  name: airflow-worker
  namespace: $NAMESPACE
- kind: ServiceAccount
  name: airflow-dag-processor
  namespace: $NAMESPACE
- kind: ServiceAccount
  name: airflow-triggerer
  namespace: $NAMESPACE
roleRef:
  kind: Role
  name: airflow-secret-reader
  apiGroup: rbac.authorization.k8s.io
EOF
```

## Phase 3: Prepare PostgreSQL Databases

### Step 1: Configure PostgreSQL Network Access

If PostgreSQL is on a separate host (e.g., accessible via Tailscale), configure `pg_hba.conf` to allow connections from the Kubernetes cluster network:

```bash
# On the PostgreSQL host, add to /etc/postgresql/*/main/pg_hba.conf:
# For Tailscale network (100.64.0.0/10)
host    all    all    100.64.0.0/10    scram-sha-256

# For pod network (check your cluster's pod CIDR)
host    all    all    10.244.0.0/16    scram-sha-256

# Reload PostgreSQL
sudo systemctl reload postgresql
```

### Step 2: Create Required Databases

```bash
# Connect to your PostgreSQL host (replace 'postgres' with your hostname)
export PG_HOST=postgres  # or postgres-co, etc.

PGPASSWORD=password psql -h $PG_HOST -U postgres -c "CREATE DATABASE airflow_db;"
PGPASSWORD=password psql -h $PG_HOST -U postgres -c "CREATE DATABASE merge_db_af3;"
PGPASSWORD=password psql -h $PG_HOST -U postgres -c "CREATE DATABASE customer_db;"

# CQRS database (for Consumer Resource Groups using CQRS pattern)
PGPASSWORD=password psql -h $PG_HOST -U postgres -c 'CREATE DATABASE "postgres-cqrs-af3";'

# Create DataTransformer user
PGPASSWORD=password psql -h $PG_HOST -U postgres -c "CREATE USER datasurfacedt WITH PASSWORD 'datasurface';"
PGPASSWORD=password psql -h $PG_HOST -U postgres -d merge_db_af3 -c "GRANT CREATE ON SCHEMA public TO datasurfacedt; GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datasurfacedt; GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datasurfacedt; GRANT USAGE ON SCHEMA public TO datasurfacedt; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO datasurfacedt; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO datasurfacedt;"

PGPASSWORD=password psql -h $PG_HOST -U postgres -c "CREATE USER mask_dt_cred WITH PASSWORD 'Passw0rd123!';"
 PGPASSWORD=password psql -h $PG_HOST -U postgres -d merge_db_af3 -c "GRANT CREATE ON SCHEMA public TO mask_dt_cred; GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mask_dt_cred; GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mask_dt_cred; GRANT USAGE ON SCHEMA public TO mask_dt_cred; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO mask_dt_cred; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO mask_dt_cred;"
```

### Step 3: Create SQL Server Database and User (if using SQL Server CRG)

If your model uses SQL Server for Consumer Resource Groups, create the database and user:

```bash
# Create CQRS database (if not already created)
sqlcmd -S sqlserver-co -U sa -P 'pass@w0rd' -C -Q "CREATE DATABASE cqrs;"

# Create DataTransformer login and user
sqlcmd -S sqlserver-co -U sa -P 'pass@w0rd' -C -Q "CREATE LOGIN mask_dt_cred WITH PASSWORD = 'MaskDT@pass123';"
sqlcmd -S sqlserver-co -U sa -P 'pass@w0rd' -C -Q "USE cqrs; CREATE USER mask_dt_cred FOR LOGIN mask_dt_cred; ALTER ROLE db_owner ADD MEMBER mask_dt_cred;"
```

**Note:** Replace `sqlserver-co` with your SQL Server hostname and use the password from your `mask-dt-cred` secret.

## Phase 4: Install Airflow with Helm

### Step 1: Create Helm Values File

```bash
cat > /tmp/airflow-values.yaml <<'EOF'
executor: CeleryExecutor

# Airflow version
images:
  airflow:
    repository: apache/airflow
    tag: "3.1.6"

# External PostgreSQL for Airflow metadata
postgresql:
  enabled: false

data:
  metadataConnection:
    user: postgres
    pass: password
    protocol: postgresql
    host: postgres  # Replace with your PostgreSQL hostname (e.g., postgres-co)
    port: 5432
    db: airflow_db

# Redis for Celery broker
redis:
  enabled: true
  persistence:
    enabled: true
    storageClassName: longhorn

# High availability schedulers
scheduler:
  replicas: 2

# Persistent Celery workers
workers:
  replicas: 2  # For better load distribution across N nodes, consider N workers
  persistence:
    enabled: true
    storageClassName: longhorn

# API server with NodePort for external access
apiServer:
  service:
    type: NodePort

# DAG processor
dagProcessor:
  enabled: true

# Triggerer for async sensors
triggerer:
  enabled: true

# GitSync for DAGs - pulls from GitHub repository
dags:
  persistence:
    enabled: false
  gitSync:
    enabled: true
    repo: https://github.com/your-org/your-dag-repo.git
    branch: main
    subPath: dags
    wait: 60
    credentialsSecret: git-dags  # Must have GIT_SYNC_USERNAME/PASSWORD and GITSYNC_USERNAME/PASSWORD keys

# Shared logs volume
logs:
  persistence:
    enabled: true
    storageClassName: longhorn
    size: 10Gi
    # Note: accessMode not configurable for logs in Helm chart

config:
  logging:
    # Delete local task logs after they're uploaded to remote storage (if configured)
    delete_local_logs: "False"
  scheduler:
    # Clean up old DAG runs and task instances
    dag_dir_list_interval: "60"

# Web UI credentials
webserver:
  defaultUser:
    enabled: true
    username: admin
    password: admin
EOF
```

### Step 2: Install Airflow

```bash
helm install airflow apache-airflow/airflow \
  -f /tmp/airflow-values.yaml \
  -n $NAMESPACE \
  --timeout 10m
```

**Note:** The values file specifies Airflow 3.1.6. To use a different version, update the `images.airflow.tag` value in the values file.

### Step 3: Wait for Pods to be Ready

```bash
sudo kubectl get pods -n $NAMESPACE -w

# Wait until all pods show Running or Completed
# Expected pods:
# - airflow-scheduler-* (2 replicas)
# - airflow-worker-* (2 replicas)
# - airflow-dag-processor-*
# - airflow-triggerer-*
# - airflow-api-server-*
# - airflow-redis-*
```

## Phase 5: Generate and Deploy Bootstrap Artifacts

### Step 1: Clone the Ecosystem Repository

```bash
cd /tmp
git clone https://github.com/datasurface/demo_bootstrap.git
cd demo_bootstrap
```

### Step 2: Pull DataSurface Docker Image

Pull to local Docker (for generating artifacts) and optionally pre-pull to cluster nodes.

```bash
# Pull locally for artifact generation
docker pull datasurface/datasurface:v1.1.0
```

**Pre-pull on cluster nodes (recommended for faster pod startup):**

```bash
# For production clusters (EKS, AKS, GKE, kubeadm) - requires credentials
for node in node1 node2 node3; do
  ssh user@$node "sudo ctr -n k8s.io images pull docker.io/datasurface/datasurface:v1.1.0 \
    --user datasurface:YOUR_DOCKER_HUB_PAT"
done

# For K3s clusters (with registries.yaml configured) - no credentials needed
sudo crictl pull datasurface/datasurface:latest
```

See [Private Docker Registry Authentication](#private-docker-registry-authentication) for complete setup details including imagePullSecrets.

### Step 3: Generate Bootstrap Artifacts

```bash
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  datasurface/datasurface:latest \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Test_DP \
  --rte-name prod

# Verify generated files
ls -la generated_output/Test_DP/
# Expected:
# - test_dp_infrastructure_dag.py
# - test_dp_ring1_init_job.yaml
# - test_dp_model_merge_job.yaml
# - test_dp_reconcile_views_job.yaml
# - kubernetes-bootstrap.yaml
```

### Step 4: Copy Job YAML Files to Remote Host

```bash
scp generated_output/Test_DP/*.yaml kub-test:/tmp/
```

### Step 5: Deploy Infrastructure DAG via Git

Push the generated DAG to your DAG repository. GitSync will automatically pull it.

```bash
# Create dags directory in your DAG repo
mkdir -p dags

# Copy generated DAG
cp generated_output/Test_DP/test_dp_infrastructure_dag.py dags/

# Commit and push
git add dags/
git commit -m "Add infrastructure DAG"
git push
```

GitSync polls every 60 seconds by default. DAGs will be available after the next sync.

### Step 6: Run Initialization Jobs

```bash
# Apply and wait for Ring1 Init (creates database schemas)
sudo kubectl apply -f /tmp/test_dp_ring1_init_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-ring1-init -n $NAMESPACE --timeout=300s

# Apply and wait for Model Merge (populates DAG configurations)
sudo kubectl apply -f /tmp/test_dp_model_merge_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-model-merge-job -n $NAMESPACE --timeout=300s

# Apply and wait for Reconcile Views
sudo kubectl apply -f /tmp/test_dp_reconcile_views_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-reconcile-views-job -n $NAMESPACE --timeout=300s
```

### Step 7: Wait for GitSync to Pull DAGs

GitSync automatically pulls DAGs every 60 seconds. Wait for the sync to complete:

```bash
# Check git-sync container logs
sudo kubectl logs deployment/airflow-scheduler -n $NAMESPACE -c git-sync --tail=20

# Or force a restart if needed
sudo kubectl rollout restart deployment airflow-scheduler airflow-dag-processor -n $NAMESPACE
```

## Phase 6: Verify Deployment

### Step 1: Check DAG Loading

```bash
# Should show 13 DAGs with 0 errors
sudo kubectl logs deployment/airflow-dag-processor -n $NAMESPACE --tail=50 | grep '# DAGs'
```

### Step 2: Check All DAGs are Registered

```bash
sudo kubectl logs deployment/airflow-dag-processor -n $NAMESPACE --tail=100 | grep 'Setting next_dagrun'
```

### Step 3: Access Web UI

```bash
# Get the NodePort
sudo kubectl get svc -n $NAMESPACE | grep api-server

# Access at http://kub-test:<nodeport>
# Credentials: admin / admin
```

### Step 4: Verify Database Tables

```bash
PGPASSWORD=password psql -h postgres -U postgres -d merge_db_af3 -c "\dt"

# Expected tables include:
# - test_dp_factory_dags
# - scd1_airflow_dsg
# - scd1_airflow_datatransformer
# - scd2_airflow_dsg
# - scd2_airflow_datatransformer
```

## Updating DAGs After Code Changes

### Step 1: Build and Push New Docker Image

```bash
cd /path/to/datasurface
PUSH_IMAGE=true ./build-docker-multiarch.sh
```

### Step 2: Pull Image on Cluster Nodes

**For production clusters** (EKS, AKS, GKE, kubeadm - requires explicit credentials):

```bash
for node in node1 node2 node3 node4; do
  ssh user@$node "sudo ctr -n k8s.io images pull docker.io/datasurface/datasurface:latest \
    --user datasurface:YOUR_DOCKER_HUB_PAT"
done
```

**For K3s clusters** (with registries.yaml configured - no credentials needed):

```bash
ssh kub-test "sudo crictl pull datasurface/datasurface:latest"
ssh kub-test2 "sudo crictl pull datasurface/datasurface:latest"
```

> **Note:** If using imagePullSecrets without pre-pulling, pods will pull on startup (slower but automatic).

### Step 3: Regenerate and Deploy DAG

```bash
# Pull locally first
docker pull datasurface/datasurface:latest

# Regenerate artifacts
cd /tmp/yellow_starter_af3
docker run --rm \
  -v "$(pwd)":/workspace/model \
  -w /workspace/model \
  datasurface/datasurface:latest \
  python -m datasurface.cmd.platform generatePlatformBootstrap \
  --ringLevel 0 \
  --model /workspace/model \
  --output /workspace/model/generated_output \
  --psp Test_DP \
  --rte-name prod

# Deploy via git (in your DAG repository)
cp generated_output/Test_DP/test_dp_infrastructure_dag.py dags/
git add dags/
git commit -m "Update infrastructure DAG"
git push
```

GitSync will automatically pull the updated DAG within 60 seconds. No restart required.

### Step 4: Re-run Model Merge (if table schema changed)

```bash
ssh kub-test "
  sudo kubectl delete job test-dp-model-merge-job -n $NAMESPACE
  sudo kubectl apply -f /tmp/test_dp_model_merge_job.yaml
  sudo kubectl wait --for=condition=complete job/test-dp-model-merge-job -n $NAMESPACE --timeout=120s
"
```

## Troubleshooting

### DAGs Not Loading

```bash
# Check dag-processor logs for errors
sudo kubectl logs deployment/airflow-dag-processor -n $NAMESPACE --tail=200 | grep -i error

# Verify DAG file exists
sudo kubectl exec deployment/airflow-dag-processor -n $NAMESPACE -c dag-processor -- ls -la /opt/airflow/dags/

# Check for Python syntax errors
sudo kubectl exec deployment/airflow-dag-processor -n $NAMESPACE -c dag-processor -- python /opt/airflow/dags/test_dp_infrastructure_dag.py
```

### Schema Changes (Breaking Table Updates)

`createOrUpdateTable` won't do breaking changes. Drop tables first:

```bash
PGPASSWORD=password psql -h postgres -U postgres -d merge_db_af3 -c "
  DROP TABLE IF EXISTS test_dp_factory_dags, scd1_airflow_dsg, scd1_airflow_datatransformer, scd2_airflow_dsg, scd2_airflow_datatransformer;
"

# Then re-run ring1-init and model-merge
sudo kubectl delete job test-dp-ring1-init -n $NAMESPACE
sudo kubectl apply -f /tmp/test_dp_ring1_init_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-ring1-init -n $NAMESPACE --timeout=120s

sudo kubectl delete job test-dp-model-merge-job -n $NAMESPACE
sudo kubectl apply -f /tmp/test_dp_model_merge_job.yaml
sudo kubectl wait --for=condition=complete job/test-dp-model-merge-job -n $NAMESPACE --timeout=120s
```

### Jobs Not Running (Already Completed)

```bash
# Delete old job before re-applying
sudo kubectl delete job test-dp-model-merge-job -n $NAMESPACE
sudo kubectl apply -f /tmp/test_dp_model_merge_job.yaml
```

### Private Docker Registry Authentication

The `datasurface/datasurface` image is private. This section covers authentication for production and development clusters.

#### Production Clusters (EKS, AKS, GKE, Kubeadm)

Production Kubernetes clusters use standard **imagePullSecrets** for private registry authentication. This is the recommended approach as it works across all Kubernetes distributions.

**Step 1: Create imagePullSecret in each namespace**

```bash
# Create secret in each namespace that needs it
kubectl create secret docker-registry dockerhub-creds \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=datasurface \
  --docker-password=YOUR_DOCKER_HUB_PAT \
  --docker-email=your@email.com \
  -n $NAMESPACE

# Patch default service account to use it automatically
kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{"imagePullSecrets": [{"name": "dockerhub-creds"}]}'
```

**Step 2: Reference secret in pod specs**

Generated job YAML files include imagePullSecrets. For custom pods:

```yaml
spec:
  imagePullSecrets:
    - name: dockerhub-creds
```

**Step 3 (Optional): Pre-pull images for faster startup**

Pre-pulling avoids pull latency and Docker Hub rate limits during pod creation:

```bash
# Pull using ctr with explicit credentials on each node
for node in cokub-master cokub-worker1 cokub-worker2 cokub-worker3; do
  ssh user@$node "sudo ctr -n k8s.io images pull docker.io/datasurface/datasurface:latest \
    --user datasurface:YOUR_DOCKER_HUB_PAT"
done
```

**Cloud-Native Alternatives:**

For managed Kubernetes, consider using cloud-native registries with IAM:

| Cloud | Registry | Auth Method |
| ------- | ---------- | ------------- |
| AWS EKS | ECR | IAM roles for service accounts |
| Azure AKS | ACR | Managed identity |
| Google GKE | Artifact Registry | Workload Identity |

These eliminate the need for imagePullSecrets by using cloud IAM.

#### K3s Clusters (Development/Edge)

K3s provides a simpler built-in registry configuration:

```bash
# Create/edit /etc/rancher/k3s/registries.yaml on each node
sudo tee /etc/rancher/k3s/registries.yaml <<'EOF'
mirrors:
  docker.io:
    endpoint:
      - "https://registry-1.docker.io"
configs:
  "registry-1.docker.io":
    auth:
      username: datasurface
      password: YOUR_DOCKER_HUB_PAT
EOF

# Restart k3s to apply
sudo systemctl restart k3s  # on master
sudo systemctl restart k3s-agent  # on workers
```

After configuration, `crictl pull` works without credentials:

```bash
sudo crictl pull datasurface/datasurface:latest
```

> **Note:** This K3s-specific approach doesn't work on production clusters (EKS, AKS, GKE, kubeadm). Use imagePullSecrets instead.

#### Comparison Table

| Feature | Production (EKS/AKS/GKE/Kubeadm) | K3s (Dev/Edge) |
| --------- | -------------------------------- | ---------------- |
| Config method | imagePullSecrets (standard K8s) | `registries.yaml` (K3s-specific) |
| Scope | Per-namespace | Cluster-wide |
| Works on all K8s | ✅ Yes | ❌ K3s only |
| Cloud IAM support | ✅ Yes | ❌ No |
| Recommended for | Production | Development |

### Image Pull Issues

**For production clusters (EKS, AKS, GKE, kubeadm):**

```bash
# Pull with explicit credentials
sudo ctr -n k8s.io images pull docker.io/datasurface/datasurface:latest \
  --user datasurface:YOUR_PAT

# Or ensure imagePullSecret exists in the namespace
kubectl get secret dockerhub-creds -n $NAMESPACE

# Check pod events for pull errors
kubectl describe pod <pod-name> -n $NAMESPACE | grep -A5 Events
```

**For K3s clusters:**

```bash
# Must pull to containerd, not just docker
sudo crictl pull datasurface/datasurface:latest

# If auth fails, check registries.yaml
sudo cat /etc/rancher/k3s/registries.yaml
```

**Verify image is available:**

```bash
sudo crictl images | grep datasurface
```

### Pods Stuck in Init or ContainerCreating

**Volume mount issues (Longhorn):**

```bash
# Check if nfs-common is installed on all nodes
for node in node1 node2 node3; do
  ssh user@$node "dpkg -l | grep nfs-common"
done

# If missing, install on each node:
sudo apt-get update && sudo apt-get install -y nfs-common
```

**PVC not bound:**

```bash
# Check PVC status
kubectl get pvc -n $NAMESPACE

# For Multi-Attach errors, ensure accessMode is ReadWriteMany for shared volumes
kubectl get pvc airflow-dags -n $NAMESPACE -o yaml | grep accessMode
```

### Git Cache Not Refreshing

If model changes aren't being picked up despite new git tags:

```bash
# Clear the git cache PVC
kubectl run cache-clear --rm -i --restart=Never \
  --image=busybox \
  -n $NAMESPACE \
  --overrides='{"spec":{"containers":[{"name":"cache-clear","image":"busybox","command":["sh","-c","rm -rf /cache/*"],"volumeMounts":[{"name":"git-cache","mountPath":"/cache"}]}],"volumes":[{"name":"git-cache","persistentVolumeClaim":{"claimName":"git-cache-pvc"}}]}}' \
  -- sh -c 'rm -rf /cache/* && echo Cache cleared'
```

The git cache is quantized to 5-minute intervals. After clearing, the next job run will fetch fresh from git.

### Logs Volume Full (Jobs Failing Immediately)

If all jobs suddenly start failing with `executor_state=failed` but `state=queued`, check for disk space issues:

```bash
# Check logs volume usage
kubectl exec deployment/airflow-worker -n $NAMESPACE -c worker -- df -h /opt/airflow/logs

# If usage is >90%, clean up old logs (delete logs older than 3 days)
kubectl exec airflow-worker-0 -n $NAMESPACE -c worker -- \
  find /opt/airflow/logs -type f -mtime +3 -delete

# Verify space recovered
kubectl exec deployment/airflow-worker -n $NAMESPACE -c worker -- df -h /opt/airflow/logs
```

**Prevention:** Deploy the log cleanup CronJob (see below). The Helm chart's `cleanup` CronJob is for orphaned pods, not log files. Without log cleanup configured, the logs PVC will eventually fill up.

**Symptoms:**

- Worker logs show: `OSError: [Errno 28] No space left on device`
- Scheduler logs show: `executor_state=failed` for tasks that never started
- All DAGs fail simultaneously

### Log Cleanup CronJob

The Airflow Helm chart's `cleanup` option cleans up orphaned Kubernetes pods, not log files. For log file cleanup, deploy this CronJob:

```bash
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: CronJob
metadata:
  name: airflow-log-cleanup
  namespace: $NAMESPACE
spec:
  schedule: "0 * * * *"  # Hourly
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: cleanup
            image: busybox:latest
            command:
            - /bin/sh
            - -c
            - |
              echo "Starting log cleanup at \$(date)"
              echo "Before cleanup:"
              df -h /opt/airflow/logs
              # Delete log files older than 48 hours (2 days)
              find /opt/airflow/logs -type f -mtime +2 -delete
              # Delete empty directories
              find /opt/airflow/logs -type d -empty -delete 2>/dev/null || true
              echo "After cleanup:"
              df -h /opt/airflow/logs
              echo "Cleanup complete at \$(date)"
            volumeMounts:
            - name: logs
              mountPath: /opt/airflow/logs
          volumes:
          - name: logs
            persistentVolumeClaim:
              claimName: airflow-logs
EOF
```

**Verify the CronJob is created:**

```bash
kubectl get cronjobs -n $NAMESPACE | grep log-cleanup

# Trigger a manual run to test
kubectl create job --from=cronjob/airflow-log-cleanup airflow-log-cleanup-now -n $NAMESPACE

# Check logs (may take a few minutes on slow storage like Longhorn)
kubectl logs job/airflow-log-cleanup-now -n $NAMESPACE
```

**Note:** On Longhorn or NFS-backed PVCs, the `find` command can be slow due to network filesystem overhead. The job may take several minutes to complete.

### Database Connection Issues

```bash
# Test connectivity from a pod
kubectl run db-test --rm -i --restart=Never \
  --image=postgres:16 \
  --env="PGPASSWORD=password" \
  -n $NAMESPACE \
  -- psql -h $PG_HOST -U postgres -c "SELECT 1;"

# Check pg_hba.conf allows cluster network
# On PostgreSQL host:
sudo grep -v '^#' /etc/postgresql/*/main/pg_hba.conf | grep -v '^$'
```

## Testing with Data Simulator

To test the ingestion pipeline, run the data simulator which generates continuous changes to the source database:

```bash
# Start the data simulator (runs continuously)
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: data-simulator
  namespace: $NAMESPACE
spec:
  imagePullSecrets:
    - name: dockerhub-creds
  containers:
  - name: data-simulator
    image: datasurface/datasurface:latest
    command:
    - python
    - src/tests/data_change_simulator.py
    - --host
    - $PG_HOST
    - --port
    - "5432"
    - --database
    - customer_db
    - --user
    - postgres
    - --password
    - password
    - --create-tables
    - --max-changes
    - "1000000"
    - --verbose
  restartPolicy: Never
EOF

# Monitor the simulator
kubectl logs data-simulator -n $NAMESPACE -f

# Check data is being ingested
PGPASSWORD=password psql -h $PG_HOST -U postgres -d merge_db_af3 -c "
SELECT 'scd1_customers' as tbl, COUNT(*) FROM scd1_store1_customers_m
UNION ALL SELECT 'scd2_customers', COUNT(*) FROM scd2_store1_customers_m;"

# Stop the simulator when done
kubectl delete pod data-simulator -n $NAMESPACE
```

The simulator creates `customers` and `addresses` tables in `customer_db` and continuously generates inserts, updates, and deletes.

## Architecture Reference

See `../../tmp/airflow3x_status.md` for:

- Detailed architecture diagrams
- Performance characteristics (CeleryExecutor vs KubernetesExecutor)
- Single-DAG optimization for scalability
- DAG ID as primary key schema
- All fixes applied for Airflow 3.x compatibility

## MCP Server (Model Context Protocol)

The bootstrap artifacts include an MCP server deployment that allows AI assistants (like Cursor) to query the DataSurface model.

### MCP Server Features

- Exposes the DataSurface model via Model Context Protocol
- Auto-refreshes from git repository every 5 minutes
- Provides tools for querying datastores, workspaces, lineage, etc.

### Accessing the MCP Server

```bash
# Get the MCP service NodePort
sudo kubectl get svc -n $NAMESPACE | grep mcp

# Example output:
# test-dp-mcp   NodePort   10.43.188.219   <none>   8000:31548/TCP
```

The MCP server is accessible at `http://<node>:<nodeport>/sse`

### Configuring Cursor IDE

Add to your Cursor MCP settings (`~/.cursor/mcp.json` or workspace settings):

```json
{
  "mcpServers": {
    "datasurface": {
      "url": "http://kub-test:31548/sse"
    }
  }
}
```

### Available MCP Tools

| Tool | Description |
| ------ | ------------- |
| `list_object_types` | List queryable object types |
| `list_objects` | List all objects of a type |
| `read_object` | Get full JSON for an object |
| `search_model` | Search across model objects |
| `get_lineage` | Get data lineage for a datastore |
| `list_all_repositories` | List all git repos used by model |

### Troubleshooting MCP

```bash
# Check MCP pod status
sudo kubectl get pods -n $NAMESPACE -l component=mcp

# Check MCP logs
sudo kubectl logs -n $NAMESPACE -l component=mcp --tail=50

# Restart MCP server
sudo kubectl rollout restart deployment/test-dp-mcp-server -n $NAMESPACE
```

## Scaling Perspectives

This section covers the scaling characteristics of the YellowDataPlatform architecture and how to size your deployment.

### Architecture Overview

The platform uses a **Database-Backed Dynamic DAG** pattern where:

- DAG definitions are stored in database tables, not Python files
- Celery workers act as "dispatchers" that launch Kubernetes pods
- Actual data processing happens in short-lived pods, not in workers
- The Airflow control plane only manages scheduling, not data movement

This separation means the system scales differently than traditional Airflow deployments.

### Scaling Layers

| Layer | Component | Scaling Factor | Bottleneck |
| ------- | ----------- | ---------------- | ------------ |
| **Control Plane** | Schedulers, Workers | DAG count, task throughput | Parsing latency, memory |
| **Execution** | Kubernetes pods | Concurrent jobs | Node CPU/RAM, pod limits |
| **Storage** | Merge database | Data volume, query load | Connection pool, I/O |

### Control Plane Sizing

**Recommended baseline (from Google Airflow 3 benchmarks):**

| Component | Replicas | Notes |
| ----------- | ---------- | ------- |
| Schedulers | 2 | HA, handles 10k+ DAGs with database-backed approach |
| Celery Workers | 6 | 16 concurrency each = 96 concurrent dispatches. For even distribution across N worker nodes, use N workers (e.g., 3 workers for 3 nodes) |
| DAG Processor | 1 | Reads factory tables, not files |
| Triggerer | 1 | For async sensors |

**Worker memory:** Each Celery worker uses ~5 GiB RAM due to:

- Airflow 3 SDK and provider packages
- Prefork pool (16 processes by default)
- SQLAlchemy connection pools

This is "infrastructure tax"—stable and predictable, not scaling with DAG count.

### Single-DAG Optimization

The infrastructure DAG uses Airflow 3's `get_parsing_context()` to detect task execution mode:

- **Discovery mode** (scheduler parsing): Creates all DAGs for the shard
- **Task execution mode**: Creates only the single DAG being run

This prevents memory explosion when scaling to thousands of DAGs. Workers never hold all DAG objects in memory simultaneously.

### DAG Count Limits

| DAG Count | Shards Needed | Notes |
| ----------- | --------------- | ------- |
| 1–1,000 | 1 | Default configuration |
| 1,000–5,000 | 1–2 | May see slight parsing lag |
| 5,000–20,000 | 5–10 | Sharding recommended |
| 20,000–100,000 | 20–50 | Requires metadata DB tuning |

Enable sharding by setting `num_shards` in your platform configuration. Each shard generates a separate DAG file that loads `1/N` of the DAGs.

### Kubernetes Capacity Planning

**Pod resource defaults (from infrastructure_dag template):**

| Job Type | Memory Request | CPU Request | Memory Limit | CPU Limit |
| ---------- | ---------------- | ------------- | -------------- | ----------- |
| Ingestion | 256Mi | 100m | 256Mi | 100m |
| DataTransformer | 512Mi | 200m | 2Gi | 1000m |
| CQRS Sync | 512Mi | 200m | 1Gi | 500m |
| DC Reconcile | 512Mi | 200m | 2Gi | 1000m |

**Capacity estimation:**

```text
Available headroom = Total cluster resources - Control plane overhead
Concurrent pods = min(RAM headroom / avg pod RAM, CPU headroom / avg pod CPU)
```

**Example (4-node cluster, 24 cores, 64 GB RAM):**

- Control plane uses ~14% CPU, ~34% RAM
- Available: ~20 cores, ~40 GB RAM
- At 500Mi/200m per pod: ~80 concurrent pods possible
- At 50% utilization target: ~40 concurrent pods

### Throughput Estimation

**Jobs per hour:**

```test
Throughput = Concurrent pods × (3600 / avg job duration in seconds)
```

**Example calculations:**

| Job Duration | Concurrent Pods | Jobs/Hour | Jobs/Day |
| -------------- | ----------------- | ----------- | ---------- |
| 30 seconds | 40 | 4,800 | 115,000 |
| 60 seconds | 40 | 2,400 | 57,600 |
| 120 seconds | 40 | 1,200 | 28,800 |

### Supported Stream Counts

| Schedule | Concurrent Pods | Supported Streams |
| ---------- | ----------------- | ------------------- |
| Hourly | 40 | ~4,800 |
| Every 15 min | 40 | ~1,200 |
| Every 5 min | 40 | ~400 |

### Scaling Horizontally

**To increase capacity:**

1. **Add Kubernetes worker nodes** (primary scaling lever)
   - More nodes = more concurrent pods
   - Linear scaling for execution capacity

2. **Increase DAG shards** (for DAG count > 5,000)
   - Set `num_shards` in platform configuration
   - Each shard is a separate DAG file

3. **Add database read replicas** (for data volume > 100 GB/hour)
   - Use PgBouncer for connection pooling
   - Consider read replicas for CQRS queries

4. **Increase Celery workers** (rarely needed)
   - Only if dispatch latency is the bottleneck
   - 6 workers handles most workloads

### Monitoring Cluster Load

```bash
# Check node resource usage
sudo kubectl top nodes

# Check pod resource usage (sorted by CPU)
sudo kubectl top pods -n $NAMESPACE --sort-by='cpu' | head -n 20

# Check Airflow worker memory
sudo kubectl top pods -n $NAMESPACE -l component=worker

# Count active DAGs
PGPASSWORD=password psql -h $PG_HOST -U postgres -d merge_db_af3 -c "
  SELECT 'ingestion' as type, COUNT(*) FROM scd1_airflow_dsg WHERE status = 'active'
  UNION ALL
  SELECT 'datatransformer', COUNT(*) FROM scd1_airflow_datatransformer WHERE status = 'active';"
```

### Cost Optimization

1. **Right-size pod resources:** Override defaults in DataStore/DataTransformer declarations using `job_limits`

2. **Use spot/preemptible nodes:** Ingestion pods are stateless and can tolerate interruption

3. **Schedule non-critical jobs off-peak:** Use cron schedules to spread load

4. **Monitor and tune:** Track actual resource usage vs requests and adjust

## Quick Reference

| Component | Command |
| ----------- | --------- |
| Check pods | `sudo kubectl get pods -n yp-airflow3` |
| Check DAG loading | `sudo kubectl logs deployment/airflow-dag-processor -n yp-airflow3 --tail=50 \| grep '# DAGs'` |
| Restart Airflow | `sudo kubectl rollout restart deployment airflow-scheduler airflow-dag-processor -n yp-airflow3` |
| Deploy DAG | `git add dags/ && git commit -m "Update DAG" && git push` (GitSync pulls automatically) |
| Check GitSync | `sudo kubectl logs deployment/airflow-scheduler -n yp-airflow3 -c git-sync --tail=20` |
| Database access | `PGPASSWORD=password psql -h $PG_HOST -U postgres -d merge_db_af3` |
| View job logs | `sudo kubectl logs job/test-dp-model-merge-job -n yp-airflow3` |
| Check ingestion data | `PGPASSWORD=password psql -h $PG_HOST -U postgres -d merge_db_af3 -c "SELECT COUNT(*) FROM scd1_store1_customers_m;"` |
| Clear git cache | See [Git Cache Not Refreshing](#git-cache-not-refreshing) |
| MCP Server URL | `http://<node>:<nodeport>/sse` (get nodeport from `kubectl get svc -n yp-airflow3 \| grep mcp`) |
| Restart MCP | `sudo kubectl rollout restart deployment/test-dp-mcp-server -n yp-airflow3` |
