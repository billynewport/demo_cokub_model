---
name: DataSurface Setup Walkthrough
description: Interactive walkthrough for setting up a DataSurface Yellow environment on Docker Desktop. Use this skill to guide users through the complete installation process step-by-step.
---

# DataSurface Setup Walkthrough

This skill guides you through setting up a DataSurface Yellow environment on Docker Desktop with Kubernetes. Follow each step in order and verify completion before proceeding.

## IMPORTANT: Execution Rules

1. **Execute steps sequentially** - Do not skip ahead or combine steps
2. **Verify each step** - Confirm success before proceeding to the next step
3. **Ask for missing information** - If environment variables or credentials are not provided, ask the user
4. **Report failures immediately** - If any step fails, stop and troubleshoot before continuing

## Pre-Flight Checklist

Before starting, verify the user has:

- [ ] Docker Desktop running with Kubernetes enabled
- [ ] `kubectl` CLI installed and configured
- [ ] `helm` CLI installed
- [ ] GitHub Personal Access Token (needs repo access)
- [ ] GitLab credentials for DataSurface images

Ask the user for these environment variables if not already set:

```bash
NAMESPACE          # Kubernetes namespace (default: demo1)
GITHUB_USERNAME    # GitHub username
GITHUB_TOKEN       # GitHub Personal Access Token
GITLAB_CUSTOMER_USER   # GitLab deploy token username
GITLAB_CUSTOMER_TOKEN  # GitLab deploy token
DATASURFACE_VERSION    # DataSurface version (default: 1.1.0)
```

Also ask for the target repository names:

- **Model repository**: Where the customized model will be pushed (e.g., `yourorg/demo1_actual`)
- **Airflow DAG repository**: Where DAGs will be synced from (e.g., `yourorg/demo1_airflow`)

**Detect the Kubernetes storage class** (varies between Docker Desktop installations):

```bash
kubectl get storageclass
```

Common values:

- `standard` (some Docker Desktop versions)
- `hostpath` (other Docker Desktop versions)

Save this value - you'll need it in Step 2.

---

## Step 0: Clean Up Previous Installation (if needed)

**Skip this step if this is a fresh installation.**

If there's an existing installation, clean it up first:

```bash
# Check for existing namespace
kubectl get namespace $NAMESPACE
```

If the namespace exists:

```bash
# Uninstall Airflow
helm uninstall airflow -n $NAMESPACE

# Delete namespace
kubectl delete namespace $NAMESPACE

# If namespace is stuck in Terminating state (wait 30 seconds, then check):
kubectl get namespace $NAMESPACE -o json | jq '.spec.finalizers = []' | \
  kubectl replace --raw "/api/v1/namespaces/$NAMESPACE/finalize" -f -
```

Reset PostgreSQL databases (choose one approach):

**Option A: If PostgreSQL container is already running** (preserves container, clears data):

```bash
# Drop and recreate databases to clear old Airflow DAG history and merge data
docker exec datasurface-postgres psql -U postgres -c "DROP DATABASE IF EXISTS airflow_db;" 
docker exec datasurface-postgres psql -U postgres -c "CREATE DATABASE airflow_db;"
docker exec datasurface-postgres psql -U postgres -c "DROP DATABASE IF EXISTS merge_db;"
docker exec datasurface-postgres psql -U postgres -c "CREATE DATABASE merge_db;"
docker exec datasurface-postgres psql -U postgres -c "DROP DATABASE IF EXISTS customer_db;"
docker exec datasurface-postgres psql -U postgres -c "CREATE DATABASE customer_db;"
```

**Option B: Full reset** (destroys and recreates container):

```bash
cd docker/postgres
docker compose down -v
```

**Checkpoint:** `kubectl get namespace $NAMESPACE` should return "not found"

---

## Step 1: Start PostgreSQL

```bash
cd docker/postgres
docker compose up -d
```

**Checkpoint:** Run `docker ps | grep datasurface-postgres` - container should be running

---

## Step 2: Customize the Model

Edit three files to configure for the user's environment:

### 2a. Edit `eco.py`

Update the repository owner and name:

```python
GIT_REPO_OWNER: str = "<user's github org or username>"
GIT_REPO_NAME: str = "<model repo name, e.g., demo1_actual>"
```

### 2b. Edit `helm/airflow-values.yaml`

Update the DAG sync repository URL:

```yaml
dags:
  gitSync:
    repo: https://github.com/<org>/<airflow-repo>.git
```

**Also update the storage class** if not `standard` (check all `storageClassName` entries):

```yaml
redis:
  persistence:
    storageClassName: <storage-class>  # e.g., hostpath

workers:
  persistence:
    storageClassName: <storage-class>
```

### 2c. Edit `rte_demo.py`

Confirm the Docker image uses the correct version:

```python
datasurfaceDockerImage="registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0",
```

**Also update the storage class** if not `standard`:

```python
pv_storage_class="<storage-class>",  # e.g., "hostpath"
```

And in `git_config`:

```python
git_config: GitCacheConfig = GitCacheConfig(
    enabled=True,
    access_mode="ReadWriteOnce",
    storageClass="<storage-class>"  # e.g., "hostpath"
)
```

**Checkpoint:** All files have been edited with the user's values, including correct storage class

---

## Step 3: Push Customized Model to Repository

**CRITICAL: Do not skip this step. The model MUST be pushed AND tagged for DataSurface to pick it up.**

```bash
# Change remote to target repository
git remote set-url origin https://github.com/<org>/<model-repo>.git

# Stage and commit changes
git add eco.py helm/airflow-values.yaml rte_demo.py
git commit -m "Customize model for environment"

# Push to the model repository (force if replacing existing)
git push -u origin main --force

# Tag the commit for DataSurface to recognize it
git tag v1.0.0-demo
git push origin v1.0.0-demo
```

**Checkpoint:**

- Run `git remote -v` - should show the target model repository
- Run `git log -1` - should show the customize commit
- Run `git tag` - should show `v1.0.0-demo`
- Verify on GitHub that the repository has the updated files AND the tag exists

---

## Step 4: Create Kubernetes Namespace and Secrets

```bash
# Create namespace
kubectl create namespace $NAMESPACE

# PostgreSQL credentials (for Airflow metadata)
kubectl create secret generic postgres \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# PostgreSQL credentials for merge database
kubectl create secret generic postgres-demo-merge \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# Git credentials for model repository
kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_TOKEN \
  -n $NAMESPACE

# Git credentials for DAG sync
kubectl create secret generic git-dags \
  --from-literal=GIT_SYNC_USERNAME=$GITHUB_USERNAME \
  --from-literal=GIT_SYNC_PASSWORD=$GITHUB_TOKEN \
  --from-literal=GITSYNC_USERNAME=$GITHUB_USERNAME \
  --from-literal=GITSYNC_PASSWORD=$GITHUB_TOKEN \
  -n $NAMESPACE

# GitLab registry credentials
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE

# Attach image pull secret to default service account
kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{"imagePullSecrets": [{"name": "datasurface-registry"}]}'
```

**Checkpoint:** Run `kubectl get secrets -n $NAMESPACE` - should show all 6 secrets:

- postgres
- postgres-demo-merge
- git
- git-dags
- datasurface-registry
- (plus default service account token)

---

## Step 5: Install Airflow via Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  -f helm/airflow-values.yaml \
  -n $NAMESPACE \
  --timeout 10m
```

**Checkpoint:** Run `kubectl get pods -n $NAMESPACE` - Airflow pods should be starting/running:

- airflow-api-server
- airflow-scheduler
- airflow-dag-processor
- airflow-triggerer
- airflow-worker
- airflow-redis
- airflow-statsd

---

## Step 5a: Create RBAC for Airflow Secret Access

**CRITICAL: The infrastructure DAG needs to read Kubernetes secrets. Without this, DAGs will fail to import.**

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-secret-reader
  namespace: $NAMESPACE
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: airflow-secret-reader-binding
  namespace: $NAMESPACE
subjects:
- kind: ServiceAccount
  name: airflow-dag-processor
  namespace: $NAMESPACE
- kind: ServiceAccount
  name: airflow-worker
  namespace: $NAMESPACE
- kind: ServiceAccount
  name: airflow-scheduler
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

**Checkpoint:** Run `kubectl get role,rolebinding -n $NAMESPACE` - should show:

- role.rbac.authorization.k8s.io/airflow-secret-reader
- rolebinding.rbac.authorization.k8s.io/airflow-secret-reader-binding

---

## Step 6: Pull DataSurface Image and Generate Bootstrap

```bash
# Login to GitLab registry
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"

# Pull the image
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}

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
```

**Checkpoint:** Run `ls generated_output/Demo_PSP/` - should contain:

- kubernetes-bootstrap.yaml
- demo_psp_infrastructure_dag.py
- demo_psp_ring1_init_job.yaml
- demo_psp_model_merge_job.yaml
- demo_psp_reconcile_views_job.yaml

---

## Step 7: Deploy Bootstrap and Jobs

**IMPORTANT:** Jobs must be run sequentially. The ring1-init job creates database tables that model-merge depends on. Running them simultaneously causes a race condition where model-merge fails trying to access non-existent tables.

```bash
# Apply Kubernetes bootstrap
kubectl apply -f generated_output/Demo_PSP/kubernetes-bootstrap.yaml

# Delete existing jobs if redeploying
kubectl delete job demo-psp-ring1-init demo-psp-model-merge-job -n $NAMESPACE --ignore-not-found

# Step 1: Apply and wait for ring1-init to complete (creates tables)
kubectl apply -f generated_output/Demo_PSP/demo_psp_ring1_init_job.yaml
kubectl wait --for=condition=complete --timeout=120s job/demo-psp-ring1-init -n $NAMESPACE

# Step 2: Apply model-merge job (depends on tables created by ring1-init)
kubectl apply -f generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
kubectl wait --for=condition=complete --timeout=120s job/demo-psp-model-merge-job -n $NAMESPACE
```

**Checkpoint:** Run `kubectl get jobs -n $NAMESPACE` - both jobs should complete:

- demo-psp-ring1-init: Complete
- demo-psp-model-merge-job: Complete

If jobs fail, check logs:

```bash
kubectl logs job/demo-psp-ring1-init -n $NAMESPACE
kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE
```

---

## Step 8: Push DAG to Airflow Repository

```bash
# Clone the airflow DAG repository (or navigate to existing clone)
cd /tmp
git clone https://github.com/<org>/<airflow-repo>.git
cd <airflow-repo>

# Copy the infrastructure DAG
mkdir -p dags
cp <path-to-model>/generated_output/Demo_PSP/demo_psp_infrastructure_dag.py dags/

# Commit and push
git add dags/
git commit -m "Add infrastructure DAG"
git push
```

**Checkpoint:** Verify on GitHub that the DAG file exists in the airflow repository under `dags/`

---

## Step 9: Verify Deployment

### 9a. Check Pods and Jobs Status

```bash
# Check all pods are running
kubectl get pods -n $NAMESPACE

# Check jobs completed
kubectl get jobs -n $NAMESPACE
```

Expected state:

- All airflow-* pods: Running
- demo-psp-ring1-init: Complete (1/1)
- demo-psp-model-merge-job: Complete (1/1)

### 9b. Verify Job Logs (No Errors)

```bash
# Check ring1-init logs - should end with "Ring 1 initialization complete"
kubectl logs job/demo-psp-ring1-init -n $NAMESPACE | tail -5

# Check model-merge logs - should end with "Model merge handler complete"
# and show "Populated factory DAG configurations" with config_count > 0
kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE | tail -10
```

**Key success indicators in model-merge logs:**
- `"Cleared existing factory DAG configurations"` - tables exist
- `"Populated factory DAG configurations"` with `config_count: 2` (or more)
- `"Populated CQRS DAG configurations"` with `config_count: 1` (or more)
- No ERROR level messages (WARNING about event publishing is normal)

### 9c. Verify Database Tables Created

```bash
# Connect to PostgreSQL and check tables exist in merge_db
docker exec -it datasurface-postgres psql -U postgres -d merge_db -c "\dt"
```

Expected tables created by ring1-init:
- `demo_psp_factory_dags`
- `demo_psp_cqrs_dags`
- `demo_psp_dc_reconcile_dags`
- `scd2_airflow_dsg`
- `scd2_airflow_datatransformer`

**Checkpoint:** All pods running, jobs completed (1/1), tables exist in merge_db

### 9d. Verify DAGs are Registered in Airflow

Wait 60-90 seconds for git-sync to pull the DAG file, then verify DAGs are loaded:

```bash
kubectl exec -n $NAMESPACE deployment/airflow-dag-processor -c dag-processor -- airflow dags list 2>&1 | grep -v "DeprecationWarning\|RemovedInAirflow\|permissions.py"
```

Expected DAGs (5 total):

| DAG ID | Status | Description |
|--------|--------|-------------|
| `scd2_factory_dag` | Active | Factory DAG for SCD2 pipelines |
| `Demo_PSP_K8sMergeDB_reconcile` | Active | DataContainer reconciliation |
| `Demo_PSP_default_K8sMergeDB_cqrs` | Active | CQRS DAG |
| `demo-psp_infrastructure` | Paused | Infrastructure management |
| `scd2_datatransformer_factory` | Paused | DataTransformer factory |

**Checkpoint:** All 5 DAGs appear in the list. If DAGs are missing, check for import errors:

```bash
kubectl exec -n $NAMESPACE deployment/airflow-dag-processor -c dag-processor -- airflow dags list-import-errors
```

---

## Step 10: Access Airflow UI

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n $NAMESPACE
```

Open <http://localhost:8080> in browser:

- Username: `admin`
- Password: `admin`

---

## Troubleshooting Quick Reference

### Namespace stuck in Terminating

```bash
kubectl get namespace $NAMESPACE -o json | jq '.spec.finalizers = []' | \
  kubectl replace --raw "/api/v1/namespaces/$NAMESPACE/finalize" -f -
```

### ImagePullBackOff

```bash
kubectl get secret datasurface-registry -n $NAMESPACE
kubectl get sa default -n $NAMESPACE -o yaml | grep imagePullSecrets
```

### Job failures

```bash
kubectl logs job/demo-psp-ring1-init -n $NAMESPACE
kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE
```

### PostgreSQL connection issues

```bash
lsof -i :5432  # Check for port conflicts
docker logs datasurface-postgres  # Check container logs
```

### PVC pending / storage class not found

If PersistentVolumeClaims are stuck in Pending:

```bash
# Check available storage classes
kubectl get storageclass

# Check PVC status
kubectl get pvc -n $NAMESPACE

# Describe PVC for error details
kubectl describe pvc <pvc-name> -n $NAMESPACE
```

Common fix: Update `storageClassName` in `helm/airflow-values.yaml` and `rte_demo.py` to match your cluster's storage class (e.g., `hostpath` instead of `standard`), then redeploy.

### DAG import errors (Forbidden / secrets access)

If DAGs fail to import with "Forbidden" errors reading secrets:

```bash
# Check for import errors
kubectl exec -n $NAMESPACE deployment/airflow-dag-processor -c dag-processor -- airflow dags list-import-errors

# Verify RBAC exists
kubectl get role,rolebinding -n $NAMESPACE | grep airflow-secret

# If missing, create RBAC (see Step 5a) then restart dag-processor:

kubectl rollout restart deployment/airflow-dag-processor -n $NAMESPACE
```
