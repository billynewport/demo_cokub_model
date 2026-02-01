---
name: Troubleshoot DataSurface Yellow Kubernetes Job Failures
description: Diagnose and fix common failures in Yellow Kubernetes jobs (init, merge, reconcile).
---
# Troubleshooting Yellow Kubernetes Job Failures

This guide helps diagnose and resolve failures in DataSurface Yellow Kubernetes jobs.

## Quick Diagnosis

### Check Job Status

```bash
# List all jobs and their status
kubectl get jobs -n $NAMESPACE

# Expected successful output:
# NAME                        COMPLETIONS   DURATION   AGE
# demo-psp-ring1-init         1/1           45s        10m
# demo-psp-model-merge-job    1/1           30s        5m
```

### View Job Logs

```bash
# Get logs from a job's pod
kubectl logs job/demo-psp-ring1-init -n $NAMESPACE
kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE

# If the job has multiple attempts, get logs from specific pod
kubectl get pods -n $NAMESPACE | grep demo-psp
kubectl logs <pod-name> -n $NAMESPACE
```

### Describe Job for Events

```bash
kubectl describe job demo-psp-model-merge-job -n $NAMESPACE
```

## Common Failures and Solutions

### 1. Credential Not Found

**Error Pattern:**
```
Credential not found: user or password is None
ValueError: Credential 'postgres-demo-merge' not found or incomplete
```

**Cause:** The Kubernetes secret doesn't exist, has wrong key names, or the secret name doesn't match Yellow's naming convention.

**Diagnosis:**
```bash
# Check if secret exists
kubectl get secret postgres-demo-merge -n $NAMESPACE

# View secret keys (not values)
kubectl describe secret postgres-demo-merge -n $NAMESPACE

# Check environment variables in pod
kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE | grep -i credential
```

**Solution:**

Yellow converts credential names using these rules:
- Lowercase
- Underscores (`_`) become hyphens (`-`)
- Spaces become hyphens

Create the secret with correct keys:

```bash
# For USER_PASSWORD credentials
kubectl create secret generic postgres-demo-merge \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE

# For API_TOKEN credentials (e.g., git)
kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_TOKEN \
  -n $NAMESPACE
```

**Key names are case-sensitive:** Use `USER`, `PASSWORD`, `TOKEN` (uppercase).

See [credential creation guide](../create-k8-credential/SKILLS.md) for complete details.

---

### 2. Database Does Not Exist

**Error Pattern:**
```
FATAL: database "merge_db" does not exist
psycopg2.OperationalError: connection to server failed
```

**Cause:** PostgreSQL init scripts didn't run (existing Docker volume) or wrong PostgreSQL instance is being accessed.

**Diagnosis:**
```bash
# Connect to PostgreSQL and list databases
docker exec datasurface-postgres psql -U postgres -c "\l"

# Or use local psql if available
psql -h localhost -U postgres -c "\l"
```

**Solution A - Create databases manually:**
```bash
docker exec datasurface-postgres psql -U postgres \
  -c "CREATE DATABASE airflow_db;" \
  -c "CREATE DATABASE merge_db;"
```

**Solution B - Reset Docker volume:**
```bash
cd docker/postgres
docker compose down -v
docker compose up -d
```

---

### 3. PostgreSQL Port Conflict

**Error Pattern:**
```
FATAL: password authentication failed for user "postgres"
FATAL: database "merge_db" does not exist
```
(But you're sure the credentials and database are correct)

**Cause:** A local PostgreSQL (e.g., Homebrew) is running on port 5432, and Kubernetes pods connect to it instead of the Docker container via `host.docker.internal:5432`.

**Diagnosis:**
```bash
# Check what's listening on 5432
lsof -i :5432

# Connect and check PostgreSQL version
psql -h localhost -U postgres -c "SELECT version();"
# If it shows "PostgreSQL 17.x (Homebrew)" instead of "16-alpine", wrong instance!
```

**Solution:**
```bash
# Stop Homebrew PostgreSQL
brew services stop postgresql@17
# or
brew services stop postgresql@16
# or
brew services stop postgresql

# Verify Docker PostgreSQL is now accessible
psql -h localhost -U postgres -c "SELECT version();"
# Should show: PostgreSQL 16.x (Debian/Alpine)
```

---

### 4. ImagePullBackOff

**Error Pattern:**
```
Status: ImagePullBackOff
Failed to pull image "registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0"
```

**Diagnosis:**
```bash
kubectl describe pod <pod-name> -n $NAMESPACE | grep -A10 Events
```

**Solution:**

1. Verify registry secret exists:
```bash
kubectl get secret datasurface-registry -n $NAMESPACE
```

2. Create registry secret if missing:
```bash
kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username="$GITLAB_CUSTOMER_USER" \
  --docker-password="$GITLAB_CUSTOMER_TOKEN" \
  -n $NAMESPACE
```

3. Attach to default service account:
```bash
kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{"imagePullSecrets": [{"name": "datasurface-registry"}]}'
```

4. Verify credentials work locally:
```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0
```

---

### 5. CreateContainerConfigError

**Error Pattern:**
```
Status: CreateContainerConfigError
secret "git" not found
```

**Diagnosis:**
```bash
kubectl describe pod <pod-name> -n $NAMESPACE | grep -A5 Events
```

**Solution:**

Create the missing secret. Common missing secrets:

```bash
# Git token for model repository
kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_TOKEN \
  -n $NAMESPACE

# Merge database credentials
kubectl create secret generic postgres-demo-merge \
  --from-literal=USER=postgres \
  --from-literal=PASSWORD=password \
  -n $NAMESPACE
```

---

### 6. Job Using Stale Docker Image

**Symptom:** You pulled a new image but the job still fails with the same error.

**Cause:** Kubernetes caches images by tag. If the tag (e.g., `v1.1.0`) hasn't changed, K8s uses the cached image.

**Solution:**

1. Ensure job YAML has `imagePullPolicy: Always`:
```yaml
containers:
- name: model-merge-handler
  image: registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0
  imagePullPolicy: Always
```

2. Delete completed job and reapply:
```bash
kubectl delete job demo-psp-model-merge-job -n $NAMESPACE
kubectl apply -f generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
```

3. Pull image locally to ensure Docker Desktop has latest:
```bash
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v1.1.0
```

---

### 7. Git Repository Access Denied

**Error Pattern:**
```
fatal: Authentication failed for 'https://github.com/yourorg/demo1_actual.git'
remote: Repository not found
```

**Diagnosis:**
```bash
# Check git secret exists and has TOKEN key
kubectl describe secret git -n $NAMESPACE
```

**Solution:**

1. Verify token has repo access permissions on GitHub

2. Recreate secret with valid token:
```bash
kubectl delete secret git -n $NAMESPACE
kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_TOKEN \
  -n $NAMESPACE
```

3. Test token locally:
```bash
git ls-remote https://${GITHUB_TOKEN}@github.com/yourorg/demo1_actual.git
```

---

## Rerunning Failed Jobs

Jobs are immutable once created. To rerun:

```bash
# Delete the failed job
kubectl delete job demo-psp-model-merge-job -n $NAMESPACE

# Reapply
kubectl apply -f generated_output/Demo_PSP/demo_psp_model_merge_job.yaml

# Watch logs
kubectl logs -f job/demo-psp-model-merge-job -n $NAMESPACE
```

## Verifying Successful Completion

```bash
# All jobs should show COMPLETIONS as 1/1
kubectl get jobs -n $NAMESPACE

# Check pod status
kubectl get pods -n $NAMESPACE

# Expected:
# - demo-psp-ring1-init-xxxxx: Completed
# - demo-psp-model-merge-job-xxxxx: Completed
# - airflow-* pods: Running
# - demo-psp-mcp-server-*: Running
```

## Getting Help

If issues persist:

1. Collect full logs:
```bash
kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE > merge-job.log
kubectl describe job demo-psp-model-merge-job -n $NAMESPACE > merge-job-describe.log
```

2. Check generated YAML for issues:
```bash
cat generated_output/Demo_PSP/demo_psp_model_merge_job.yaml
```

3. Verify all secrets exist:
```bash
kubectl get secrets -n $NAMESPACE
```
