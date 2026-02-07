# DataSurface Remote Cluster Setup Walkthrough

This skill guides you through setting up a DataSurface Yellow environment on a remote Kubernetes cluster accessed via SSH. It uses external PostgreSQL databases (separate hosts for Airflow metadata and merge data) and Longhorn storage.

## IMPORTANT: Execution Rules

1. **Execute steps sequentially** - Do not skip ahead or combine steps
2. **Verify each step** - Confirm success before proceeding to the next step
3. **Ask for missing information** - If environment variables or credentials are not provided, ask the user
4. **Report failures immediately** - If any step fails, stop and troubleshoot before continuing

## Pre-Flight Checklist

Before starting, verify the user has:

- [ ] A remote Kubernetes cluster accessible via SSH
- [ ] SSH key that authenticates to the cluster master node
- [ ] `kubectl` and `helm` installed on the remote master node
- [ ] Docker installed locally (for image pull and bootstrap generation)
- [ ] `psql` installed locally (for database setup)
- [ ] Two external PostgreSQL databases reachable from both local machine and K8s pods
- [ ] GitHub Personal Access Token (needs repo access)
- [ ] GitLab credentials for DataSurface images
- [ ] `nfs-common` installed on all cluster nodes (required for Longhorn RWX volumes)
- [ ] PostgreSQL `pg_hba.conf` configured to allow connections from pod CIDR and Tailscale network

Ask the user for these values if not already provided:

```bash
# SSH connection
SSH_KEY               # Path to SSH private key (e.g., ~/.ssh/id_rsa_batch)
SSH_USER              # SSH username (e.g., billy)
SSH_HOST              # Remote master node hostname (e.g., cokub-master)

# Kubernetes
NAMESPACE             # Kubernetes namespace (must use hyphens, not underscores)
STORAGE_CLASS         # Storage class available on remote cluster

# PostgreSQL
AIRFLOW_DB_HOST       # Hostname of Airflow metadata database
MERGE_DB_HOST         # Hostname of merge database
PG_USER               # PostgreSQL username (default: postgres)
PG_PASSWORD           # PostgreSQL password

# GitHub
GITHUB_USERNAME       # GitHub username
GITHUB_MODEL_PULL_TOKEN   # GitHub PAT for model repository access
GH_AIRFLOW_USER       # GitHub username for DAG sync
GH_AIRFLOW_PAT        # GitHub PAT for DAG sync

# GitLab
GITLAB_CUSTOMER_USER  # GitLab deploy token username
GITLAB_CUSTOMER_TOKEN # GitLab deploy token

# Repositories
MODEL_REPO            # Model repository (e.g., yourorg/demo_model)
AIRFLOW_REPO          # Airflow DAG repository (e.g., yourorg/demo_airflow)

# DataSurface
DATASURFACE_VERSION   # DataSurface version (default: 1.1.0)
```

**Define an SSH helper** to use throughout (avoids repeating SSH options):

```bash
SSH_CMD="ssh -i $SSH_KEY $SSH_USER@$SSH_HOST"
SCP_CMD="scp -i $SSH_KEY"
```

### Verify NFS client on all nodes

**Required for Longhorn ReadWriteMany volumes.** Without `nfs-common`, pods using shared volumes will fail with mount errors.

```bash
# Check each node
for node in $($SSH_CMD "kubectl get nodes -o jsonpath='{.items[*].metadata.name}'"); do
  $SSH_CMD "ssh $node 'dpkg -l | grep nfs-common'"
done

# If missing on any node, install:
# ssh <node> "sudo apt-get update && sudo apt-get install -y nfs-common"
```

### Verify PostgreSQL network access

Ensure `pg_hba.conf` on each PostgreSQL host allows connections from the Kubernetes pod network and Tailscale network:

```bash
# On each PostgreSQL host, add to /etc/postgresql/*/main/pg_hba.conf:
# For Tailscale network (100.64.0.0/10)
host    all    all    100.64.0.0/10    scram-sha-256

# For pod network (check your cluster's pod CIDR)
host    all    all    10.244.0.0/16    scram-sha-256

# Reload PostgreSQL
sudo systemctl reload postgresql
```

### Verify SSH connectivity and cluster access

```bash
$SSH_CMD "hostname && kubectl get nodes"
```

All nodes should be in `Ready` status.

### Detect the Kubernetes storage class

```bash
$SSH_CMD "kubectl get storageclass"
```

Save the default storage class name (e.g., `longhorn`, `standard`). **Important:** If using Longhorn, the git-cache PVC must use `ReadWriteMany` access mode since it is shared between the MCP server deployment and batch jobs.

### Verify database connectivity from local machine

```bash
PGPASSWORD=$PG_PASSWORD psql -h $AIRFLOW_DB_HOST -U $PG_USER -c "SELECT version();"
PGPASSWORD=$PG_PASSWORD psql -h $MERGE_DB_HOST -U $PG_USER -c "SELECT version();"
```

---

## Step 0: Configure CoreDNS for Database Hostname Resolution

**CRITICAL: K8s pods cannot resolve Tailscale or other non-cluster hostnames by default.** The CoreDNS `hosts` plugin needs entries with FQDN aliases matching the pod search domains.

### 0a. Check current CoreDNS configuration

```bash
$SSH_CMD "kubectl get configmap coredns -n kube-system -o yaml"
```

### 0b. Check pod search domains

```bash
$SSH_CMD 'kubectl run dns-check --rm -it --restart=Never --image=busybox -- cat /etc/resolv.conf'
```

Note the search domains (e.g., `default.svc.cluster.local svc.cluster.local cluster.local leopard-mizar.ts.net`). The last entry is typically the Tailscale domain if nodes use Tailscale.

### 0c. Add static host entries with FQDN aliases

**IMPORTANT:** Due to `ndots:5` in K8s pods, bare hostnames are tried with search domain suffixes first. You must add aliases for each search domain suffix that might match.

For Tailscale hosts, add both the bare name and the `.ts-domain.ts.net` FQDN:

```bash
$SSH_CMD 'cat <<'"'"'EOF'"'"' | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns
  namespace: kube-system
data:
  Corefile: |
    .:53 {
        errors
        health {
           lameduck 5s
        }
        ready
        hosts {
           <AIRFLOW_DB_IP> <AIRFLOW_DB_HOST> <AIRFLOW_DB_HOST>.<TS_DOMAIN>
           <MERGE_DB_IP> <MERGE_DB_HOST> <MERGE_DB_HOST>.<TS_DOMAIN>
           fallthrough
        }
        kubernetes cluster.local in-addr.arpa ip6.arpa {
           pods insecure
           fallthrough in-addr.arpa ip6.arpa
           ttl 30
        }
        prometheus :9153
        forward . /etc/resolv.conf {
           max_concurrent 1000
        }
        cache 30 {
           disable success cluster.local
           disable denial cluster.local
        }
        loop
        reload
        loadbalance
    }
EOF'
```

### 0d. Restart CoreDNS to pick up changes

The `reload` plugin auto-detects changes, but a restart is more reliable:

```bash
$SSH_CMD "kubectl rollout restart deployment coredns -n kube-system && \
  kubectl rollout status deployment coredns -n kube-system --timeout=60s"
```

### 0e. Verify resolution from inside a pod

```bash
$SSH_CMD "kubectl run dns-test --rm -it --restart=Never --image=busybox -- nslookup $AIRFLOW_DB_HOST"
```

**Expected:** The name resolves (via the Tailscale search domain suffix). Ignore NXDOMAIN errors for other search domain suffixes — as long as one resolves, it works. The key line is:

```
Name:    <hostname>.<ts-domain>
Address: <IP>
```

### 0f. Verify database connectivity from inside a pod

```bash
$SSH_CMD "kubectl run db-test --rm -it --restart=Never --image=postgres:16 -- \
  sh -c \"PGPASSWORD=$PG_PASSWORD psql -h $AIRFLOW_DB_HOST -U $PG_USER -c 'SELECT 1'\""
```

**Checkpoint:**
- Both database hostnames resolve from inside K8s pods
- Pods can connect to both databases

---

## Step 1: Clean and Prepare PostgreSQL Databases

Reset both databases to ensure clean state. Run from local machine using `psql`:

```bash
PGPASSWORD=$PG_PASSWORD psql -h $AIRFLOW_DB_HOST -U $PG_USER \
  -c "DROP DATABASE IF EXISTS airflow_db;" \
  -c "CREATE DATABASE airflow_db;"

PGPASSWORD=$PG_PASSWORD psql -h $MERGE_DB_HOST -U $PG_USER \
  -c "DROP DATABASE IF EXISTS merge_db;" \
  -c "CREATE DATABASE merge_db;"
```

**Checkpoint:**
- Both databases exist and are empty:

```bash
PGPASSWORD=$PG_PASSWORD psql -h $AIRFLOW_DB_HOST -U $PG_USER -c "\l airflow_db"
PGPASSWORD=$PG_PASSWORD psql -h $MERGE_DB_HOST -U $PG_USER -c "\l merge_db"
```

---

## Step 2: Customize the Model

Edit three files to configure for the remote environment:

### 2a. Edit `eco.py`

```python
GIT_REPO_OWNER: str = "<github-username>"
GIT_REPO_NAME: str = "<model-repo-name>"
```

### 2b. Edit `rte_demo.py`

Update namespace, merge host, storage class, and git cache access mode:

```python
KUB_NAME_SPACE: str = "<namespace>"          # Must use hyphens, not underscores
MERGE_HOST: str = "<merge-db-host>"          # External PostgreSQL hostname
```

**Storage class and git cache (CRITICAL for remote clusters with Longhorn):**

```python
git_config: GitCacheConfig = GitCacheConfig(
    enabled=True,
    access_mode="ReadWriteMany",             # MUST be ReadWriteMany for shared PVC
    storageClass="<storage-class>"           # e.g., "longhorn"
)
```

```python
pv_storage_class="<storage-class>",          # e.g., "longhorn"
```

### 2c. Edit `helm/airflow-values.yaml`

Update the Airflow metadata DB host, DAG repo, and storage class:

```yaml
data:
  metadataConnection:
    host: <airflow-db-host>    # External PostgreSQL hostname (NOT host.docker.internal)

dags:
  gitSync:
    repo: https://github.com/<org>/<airflow-repo>.git

redis:
  persistence:
    storageClassName: <storage-class>

workers:
  persistence:
    storageClassName: <storage-class>

logs:
  persistence:
    storageClassName: <storage-class>
```

**Checkpoint:** All three files edited with correct values

---

## Step 3: Initialize the Airflow DAG Repository

**CRITICAL: Do this BEFORE installing Airflow.** The Airflow Helm chart uses git-sync init containers that will fail if the DAG repository is empty or missing the `main` branch. This causes pods to enter `Init:Error` state.

### 3a. Generate bootstrap artifacts first (needed for the DAG file)

```bash
docker login registry.gitlab.com -u "$GITLAB_CUSTOMER_USER" -p "$GITLAB_CUSTOMER_TOKEN"

docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}

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

**Checkpoint:** `ls generated_output/Demo_PSP/` shows:
- kubernetes-bootstrap.yaml
- demo_psp_infrastructure_dag.py
- demo_psp_ring1_init_job.yaml
- demo_psp_model_merge_job.yaml
- demo_psp_reconcile_views_job.yaml

### 3b. Initialize and push the DAG repository

```bash
cd /tmp
rm -rf <airflow-repo-name>
git clone https://github.com/<org>/<airflow-repo>.git
cd <airflow-repo-name>
git checkout -b main
mkdir -p dags
cp <path-to-model>/generated_output/Demo_PSP/demo_psp_infrastructure_dag.py dags/
git add dags/
git commit -m "Add infrastructure DAG"
git push -u origin main
```

**Checkpoint:** DAG file exists on GitHub under `dags/` on the `main` branch

---

## Step 4: Push Customized Model to Repository

```bash
# Change remote to target model repository
git remote set-url origin https://github.com/<org>/<model-repo>.git

# Stage and commit
git add eco.py helm/airflow-values.yaml rte_demo.py
git commit -m "Customize model for remote cluster"

# Push and tag
git push -u origin main --force
git tag v1.0.0-demo
git push origin v1.0.0-demo
```

**Checkpoint:**
- `git remote -v` shows the target model repository
- `git tag` shows `v1.0.0-demo`
- GitHub shows updated files and tag

---

## Step 5: Create Kubernetes Namespace and Secrets

All commands run via SSH on the remote master node.

**IMPORTANT:** Kubernetes namespace names must be valid DNS labels — lowercase alphanumeric and hyphens only. **Underscores are not allowed.**

```bash
$SSH_CMD "kubectl create namespace $NAMESPACE"

# PostgreSQL credentials for Airflow metadata DB
$SSH_CMD "kubectl create secret generic postgres \
  --from-literal=USER=$PG_USER \
  --from-literal=PASSWORD=$PG_PASSWORD \
  -n $NAMESPACE"

# PostgreSQL credentials for merge database
$SSH_CMD "kubectl create secret generic postgres-demo-merge \
  --from-literal=USER=$PG_USER \
  --from-literal=PASSWORD=$PG_PASSWORD \
  -n $NAMESPACE"

# Git credentials for model repository
$SSH_CMD "kubectl create secret generic git \
  --from-literal=TOKEN=$GITHUB_MODEL_PULL_TOKEN \
  -n $NAMESPACE"

# Git credentials for DAG sync
$SSH_CMD "kubectl create secret generic git-dags \
  --from-literal=GIT_SYNC_USERNAME=$GH_AIRFLOW_USER \
  --from-literal=GIT_SYNC_PASSWORD=$GH_AIRFLOW_PAT \
  --from-literal=GITSYNC_USERNAME=$GH_AIRFLOW_USER \
  --from-literal=GITSYNC_PASSWORD=$GH_AIRFLOW_PAT \
  -n $NAMESPACE"

# GitLab registry credentials
$SSH_CMD "kubectl create secret docker-registry datasurface-registry \
  --docker-server=registry.gitlab.com \
  --docker-username=$GITLAB_CUSTOMER_USER \
  --docker-password=$GITLAB_CUSTOMER_TOKEN \
  -n $NAMESPACE"

# Attach image pull secret to default service account
$SSH_CMD "kubectl patch serviceaccount default -n $NAMESPACE \
  -p '{\"imagePullSecrets\": [{\"name\": \"datasurface-registry\"}]}'"
```

**Checkpoint:** `$SSH_CMD "kubectl get secrets -n $NAMESPACE"` shows all 5 secrets:
- postgres
- postgres-demo-merge
- git
- git-dags
- datasurface-registry

---

## Step 6: Install Airflow via Helm

Copy the values file to the remote host, then install:

```bash
$SCP_CMD helm/airflow-values.yaml $SSH_USER@$SSH_HOST:/tmp/airflow-values.yaml

$SSH_CMD "helm repo add apache-airflow https://airflow.apache.org && helm repo update"

$SSH_CMD "helm install airflow apache-airflow/airflow \
  -f /tmp/airflow-values.yaml \
  -n $NAMESPACE \
  --timeout 10m"
```

**Checkpoint:** `$SSH_CMD "kubectl get pods -n $NAMESPACE"` — all Airflow pods should reach Running state:
- airflow-api-server
- airflow-scheduler
- airflow-dag-processor
- airflow-triggerer
- airflow-worker
- airflow-redis
- airflow-statsd

**Note:** If the DAG repository was not initialized before this step (Step 3), the git-sync init containers will fail with `couldn't find remote ref main` and pods will be stuck in `Init:Error`.

---

## Step 6a: Create RBAC for Airflow Secret Access

**CRITICAL: The infrastructure DAG needs to read Kubernetes secrets. Without this, DAGs will fail to import.**

```bash
$SSH_CMD 'cat <<EOF | kubectl apply -f -
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
EOF'
```

**Checkpoint:** `$SSH_CMD "kubectl get role,rolebinding -n $NAMESPACE"` shows:
- role.rbac.authorization.k8s.io/airflow-secret-reader
- rolebinding.rbac.authorization.k8s.io/airflow-secret-reader-binding

---

## Step 7: Deploy Bootstrap and Run Jobs

Copy the generated YAML files to the remote host:

```bash
$SCP_CMD generated_output/Demo_PSP/*.yaml $SSH_USER@$SSH_HOST:/tmp/
```

### 7a. Apply Kubernetes bootstrap

```bash
$SSH_CMD "kubectl apply -f /tmp/kubernetes-bootstrap.yaml"
```

This creates:
- git-cache-pvc (PersistentVolumeClaim)
- logging ConfigMap
- NetworkPolicy
- MCP server Deployment and Service

### 7b. Run jobs sequentially

**IMPORTANT:** Jobs must run sequentially. The ring1-init job creates database tables that model-merge depends on.

```bash
# Delete any existing jobs
$SSH_CMD "kubectl delete job demo-psp-ring1-init demo-psp-model-merge-job \
  -n $NAMESPACE --ignore-not-found"

# Run ring1-init (creates tables)
$SSH_CMD "kubectl apply -f /tmp/demo_psp_ring1_init_job.yaml && \
  kubectl wait --for=condition=complete --timeout=180s \
  job/demo-psp-ring1-init -n $NAMESPACE"

# Run model-merge (populates DAG configurations)
$SSH_CMD "kubectl apply -f /tmp/demo_psp_model_merge_job.yaml && \
  kubectl wait --for=condition=complete --timeout=180s \
  job/demo-psp-model-merge-job -n $NAMESPACE"
```

**Checkpoint:**

```bash
$SSH_CMD "kubectl get jobs -n $NAMESPACE"
```

Both jobs should show `Complete` with `1/1` completions.

Check logs for success messages:

```bash
# Should end with "Ring 1 initialization complete"
$SSH_CMD "kubectl logs job/demo-psp-ring1-init -n $NAMESPACE | tail -3"

# Should show config_count > 0 and "Model merge handler complete"
$SSH_CMD "kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE | tail -10"
```

**Key success indicators in model-merge logs:**
- `"Populated factory DAG configurations"` with `config_count: 2` (or more)
- `"Populated CQRS DAG configurations"` with `config_count: 1` (or more)
- `"Populated DC reconcile DAG configurations"` with `config_count: 1` (or more)
- No ERROR level messages (WARNING about event publishing is normal)

---

## Step 8: Verify Deployment

### 8a. Check pods and jobs

```bash
$SSH_CMD "kubectl get pods -n $NAMESPACE"
$SSH_CMD "kubectl get jobs -n $NAMESPACE"
```

Expected: All airflow pods Running, MCP server Running, both jobs Complete.

### 8b. Verify database tables

```bash
PGPASSWORD=$PG_PASSWORD psql -h $MERGE_DB_HOST -U $PG_USER -d merge_db -c "\dt"
```

Expected tables:
- `demo_psp_factory_dags`
- `demo_psp_cqrs_dags`
- `demo_psp_dc_reconcile_dags`
- `scd2_airflow_dsg`
- `scd2_airflow_datatransformer`

### 8c. Verify DAGs are registered

Wait 60-90 seconds for git-sync, then:

```bash
$SSH_CMD 'kubectl exec -n $NAMESPACE deployment/airflow-dag-processor \
  -c dag-processor -- airflow dags list 2>&1 | \
  grep -v "DeprecationWarning\|RemovedInAirflow\|permissions.py"'
```

Expected DAGs (5 total):

| DAG ID | Description |
|--------|-------------|
| `scd2_factory_dag` | Factory DAG for SCD2 pipelines |
| `Demo_PSP_K8sMergeDB_reconcile` | DataContainer reconciliation |
| `Demo_PSP_default_K8sMergeDB_cqrs` | CQRS DAG |
| `demo-psp_infrastructure` | Infrastructure management |
| `scd2_datatransformer_factory` | DataTransformer factory |

### 8d. Check for import errors

```bash
$SSH_CMD 'kubectl exec -n $NAMESPACE deployment/airflow-dag-processor \
  -c dag-processor -- airflow dags list-import-errors'
```

Should return no errors.

---

## Step 9: Access Airflow UI

Use SSH port forwarding to access the Airflow UI from your local browser:

```bash
ssh -i $SSH_KEY -L 8080:localhost:8080 $SSH_USER@$SSH_HOST \
  "kubectl port-forward svc/airflow-api-server 8080:8080 -n $NAMESPACE"
```

Open http://localhost:8080:
- Username: `admin`
- Password: `admin`

---

## Troubleshooting Quick Reference

### CoreDNS: Pods can't resolve database hostnames

Symptoms: Jobs or pods fail with "could not translate host name" errors.

```bash
# Check CoreDNS config has hosts entries with FQDN aliases
$SSH_CMD "kubectl get configmap coredns -n kube-system -o yaml"

# Verify from a test pod (ignore NXDOMAIN for non-matching search domains)
$SSH_CMD "kubectl run dns-test --rm -it --restart=Never --image=busybox -- nslookup <hostname>"

# Restart CoreDNS if config was recently changed
$SSH_CMD "kubectl rollout restart deployment coredns -n kube-system"
```

### Git-sync init containers fail (Init:Error)

Symptoms: Airflow pods stuck in `Init:Error` or `Init:CrashLoopBackOff`.

```bash
$SSH_CMD "kubectl logs -n $NAMESPACE <pod-name> -c git-sync-init"
```

Common cause: The DAG repository is empty or missing the `main` branch. **Solution:** Initialize the DAG repository (Step 3) before installing Airflow.

### PVC Multi-Attach error

Symptoms: Jobs stuck in `ContainerCreating` with `Multi-Attach error for volume`.

```bash
$SSH_CMD "kubectl describe pod <pod-name> -n $NAMESPACE | tail -10"
```

**Root cause:** The git-cache PVC was created with `ReadWriteOnce` but is shared between the MCP server and batch jobs.

**Fix:** Recreate the PVC with `ReadWriteMany`:

```bash
# Scale down MCP server and delete jobs using the PVC
$SSH_CMD "kubectl scale deployment demo-psp-mcp-server -n $NAMESPACE --replicas=0"
$SSH_CMD "kubectl delete jobs --all -n $NAMESPACE"

# Wait for pods to terminate, then delete and recreate PVC
$SSH_CMD "kubectl delete pvc git-cache-pvc -n $NAMESPACE"

# If PVC stuck in Terminating:
$SSH_CMD "kubectl patch pvc git-cache-pvc -n $NAMESPACE \
  -p '{\"metadata\":{\"finalizers\":null}}'"

# Recreate with ReadWriteMany
$SSH_CMD 'cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: git-cache-pvc
  namespace: $NAMESPACE
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 5Gi
  storageClassName: <storage-class>
EOF'

# Scale MCP server back up
$SSH_CMD "kubectl scale deployment demo-psp-mcp-server -n $NAMESPACE --replicas=1"
```

**Prevention:** Set `access_mode="ReadWriteMany"` in `rte_demo.py` GitCacheConfig before generating bootstrap.

### Namespace stuck in Terminating

```bash
$SSH_CMD "kubectl get namespace $NAMESPACE -o json | \
  jq '.spec.finalizers = []' | \
  kubectl replace --raw \"/api/v1/namespaces/$NAMESPACE/finalize\" -f -"
```

### ImagePullBackOff

```bash
$SSH_CMD "kubectl get secret datasurface-registry -n $NAMESPACE"
$SSH_CMD "kubectl get sa default -n $NAMESPACE -o yaml | grep imagePullSecrets"
```

### Job failures

```bash
$SSH_CMD "kubectl logs job/demo-psp-ring1-init -n $NAMESPACE"
$SSH_CMD "kubectl logs job/demo-psp-model-merge-job -n $NAMESPACE"
```

### DAG import errors (Forbidden / secrets access)

```bash
$SSH_CMD 'kubectl exec -n $NAMESPACE deployment/airflow-dag-processor \
  -c dag-processor -- airflow dags list-import-errors'

# Check RBAC exists
$SSH_CMD "kubectl get role,rolebinding -n $NAMESPACE | grep airflow-secret"

# If missing, create RBAC (Step 6a) then restart dag-processor:
$SSH_CMD "kubectl rollout restart deployment/airflow-dag-processor -n $NAMESPACE"
```

---

## Key Differences from Local (Docker Desktop) Setup

| Aspect | Local Setup | Remote Setup |
|--------|------------|--------------|
| Kubernetes access | Direct `kubectl` | Via SSH to master node |
| Database hosts | `host.docker.internal` | External hostnames (e.g., Tailscale) |
| Database DNS | Automatic | Requires CoreDNS `hosts` entries with FQDN aliases |
| Storage class | `standard` or `hostpath` | `longhorn` (or cluster-specific) |
| Git cache PVC | `ReadWriteOnce` (single-node) | `ReadWriteMany` (multi-node, shared access) |
| File transfer | Direct paths | SCP files to remote host |
| PostgreSQL reset | `docker compose down -v` | `DROP DATABASE` / `CREATE DATABASE` via psql |
| Airflow UI access | `kubectl port-forward` | SSH tunnel + `kubectl port-forward` |
| DAG repo timing | Can initialize after Airflow install | **Must initialize before Airflow install** (git-sync init fails on empty repo) |

---

## Post-Setup: Pre-Pull Images on Cluster Nodes (Recommended)

Pre-pulling the DataSurface image on worker nodes avoids slow image pulls during job execution and prevents registry rate limits:

```bash
# Pull using ctr with explicit credentials on each node
for node in $($SSH_CMD "kubectl get nodes -o jsonpath='{.items[*].metadata.name}'"); do
  echo "Pulling on $node..."
  $SSH_CMD "ssh $node 'sudo ctr -n k8s.io images pull \
    registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION} \
    --user ${GITLAB_CUSTOMER_USER}:${GITLAB_CUSTOMER_TOKEN}'"
done

# Verify image is available on each node
for node in $($SSH_CMD "kubectl get nodes -o jsonpath='{.items[*].metadata.name}'"); do
  echo "=== $node ==="
  $SSH_CMD "ssh $node 'sudo crictl images | grep datasurface'"
done
```

---

## Post-Setup: Deploy Log Cleanup CronJob

The Airflow logs PVC will eventually fill up, causing all jobs to fail with `OSError: No space left on device`. Deploy this CronJob to prevent it:

```bash
$SSH_CMD 'cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: CronJob
metadata:
  name: airflow-log-cleanup
  namespace: $NAMESPACE
spec:
  schedule: "0 * * * *"
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
              echo "Starting log cleanup at $(date)"
              echo "Before cleanup:"
              df -h /opt/airflow/logs
              find /opt/airflow/logs -type f -mtime +2 -delete
              find /opt/airflow/logs -type d -empty -delete 2>/dev/null || true
              echo "After cleanup:"
              df -h /opt/airflow/logs
              echo "Cleanup complete at $(date)"
            volumeMounts:
            - name: logs
              mountPath: /opt/airflow/logs
          volumes:
          - name: logs
            persistentVolumeClaim:
              claimName: airflow-logs
EOF'
```

Verify and test:

```bash
# Check CronJob exists
$SSH_CMD "kubectl get cronjobs -n $NAMESPACE | grep log-cleanup"

# Trigger a manual run
$SSH_CMD "kubectl create job --from=cronjob/airflow-log-cleanup airflow-log-cleanup-now -n $NAMESPACE"

# Check logs (may take several minutes on Longhorn/NFS)
$SSH_CMD "kubectl logs job/airflow-log-cleanup-now -n $NAMESPACE"
```

**Symptoms of full logs volume:**
- Worker logs show: `OSError: [Errno 28] No space left on device`
- Scheduler logs show: `executor_state=failed` for tasks that never started
- All DAGs fail simultaneously

---

## Post-Setup: MCP Server Access

The bootstrap artifacts include an MCP server deployment that allows AI assistants to query the DataSurface model.

### Check MCP server status

```bash
$SSH_CMD "kubectl get pods -n $NAMESPACE -l app=demo-psp-mcp-server"
$SSH_CMD "kubectl get svc -n $NAMESPACE | grep mcp"
```

### Access MCP server via SSH tunnel

```bash
# Get the MCP service NodePort
$SSH_CMD "kubectl get svc demo-psp-mcp -n $NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}'"

# Or port-forward
ssh -i $SSH_KEY -L 8000:localhost:8000 $SSH_USER@$SSH_HOST \
  "kubectl port-forward svc/demo-psp-mcp 8000:8000 -n $NAMESPACE"
```

The MCP server is accessible at `http://localhost:8000/sse`

### Configure Claude Code or Cursor IDE

Add to `.mcp.json` or Cursor MCP settings:

```json
{
  "mcpServers": {
    "datasurface": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

### Available MCP Tools

| Tool | Description |
|------|-------------|
| `list_object_types` | List queryable object types |
| `list_objects` | List all objects of a type |
| `read_object` | Get full JSON for an object |
| `search_model` | Search across model objects |
| `get_lineage` | Get data lineage for a datastore |
| `list_all_repositories` | List all git repos used by model |

---

## Updating After Code Changes

### Regenerate and redeploy DAGs

```bash
# Pull latest DataSurface image locally
docker pull registry.gitlab.com/datasurface-inc/datasurface/datasurface:v${DATASURFACE_VERSION}

# Regenerate artifacts
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

# Update DAG in airflow repo
cp generated_output/Demo_PSP/demo_psp_infrastructure_dag.py /tmp/<airflow-repo>/dags/
cd /tmp/<airflow-repo>
git add dags/ && git commit -m "Update infrastructure DAG" && git push
```

Git-sync will pick up the change within 60 seconds.

### Re-run jobs after schema changes

`createOrUpdateTable` will not perform breaking schema changes. If table schemas have changed, drop tables first:

```bash
# Drop tables on the merge DB
PGPASSWORD=$PG_PASSWORD psql -h $MERGE_DB_HOST -U $PG_USER -d merge_db -c "
  DROP TABLE IF EXISTS demo_psp_factory_dags, demo_psp_cqrs_dags,
    demo_psp_dc_reconcile_dags, scd2_airflow_dsg, scd2_airflow_datatransformer;
"

# Copy updated job YAMLs to remote
$SCP_CMD generated_output/Demo_PSP/*.yaml $SSH_USER@$SSH_HOST:/tmp/

# Delete old jobs and re-run
$SSH_CMD "kubectl delete job demo-psp-ring1-init demo-psp-model-merge-job \
  -n $NAMESPACE --ignore-not-found"

$SSH_CMD "kubectl apply -f /tmp/demo_psp_ring1_init_job.yaml && \
  kubectl wait --for=condition=complete --timeout=180s \
  job/demo-psp-ring1-init -n $NAMESPACE"

$SSH_CMD "kubectl apply -f /tmp/demo_psp_model_merge_job.yaml && \
  kubectl wait --for=condition=complete --timeout=180s \
  job/demo-psp-model-merge-job -n $NAMESPACE"
```

### Clear git cache

If model changes aren't being picked up despite new git tags (the git cache is quantized to 5-minute intervals):

```bash
$SSH_CMD "kubectl run cache-clear --rm -i --restart=Never \
  --image=busybox \
  -n $NAMESPACE \
  --overrides='{\"spec\":{\"containers\":[{\"name\":\"cache-clear\",\"image\":\"busybox\",\"command\":[\"sh\",\"-c\",\"rm -rf /cache/*\"],\"volumeMounts\":[{\"name\":\"git-cache\",\"mountPath\":\"/cache\"}]}],\"volumes\":[{\"name\":\"git-cache\",\"persistentVolumeClaim\":{\"claimName\":\"git-cache-pvc\"}}]}}' \
  -- sh -c 'rm -rf /cache/* && echo Cache cleared'"
```

---

## Additional Troubleshooting

### Logs volume full (all jobs suddenly failing)

```bash
# Check logs volume usage
$SSH_CMD "kubectl exec deployment/airflow-worker -n $NAMESPACE -c worker -- df -h /opt/airflow/logs"

# Emergency cleanup: delete logs older than 3 days
$SSH_CMD "kubectl exec airflow-worker-0 -n $NAMESPACE -c worker -- \
  find /opt/airflow/logs -type f -mtime +3 -delete"

# Deploy the log cleanup CronJob (see Post-Setup section) to prevent recurrence
```

### Schema changes not taking effect

`createOrUpdateTable` is non-destructive — it won't drop columns or change types. You must drop the affected tables manually and re-run ring1-init + model-merge. See "Re-run jobs after schema changes" above.

### Pods stuck in ContainerCreating (volume mount errors)

Check if `nfs-common` is installed on the node where the pod is scheduled:

```bash
# Find which node the pod is on
$SSH_CMD "kubectl get pod <pod-name> -n $NAMESPACE -o jsonpath='{.spec.nodeName}'"

# Check nfs-common on that node
$SSH_CMD "ssh <node-name> 'dpkg -l | grep nfs-common'"

# If missing:
$SSH_CMD "ssh <node-name> 'sudo apt-get update && sudo apt-get install -y nfs-common'"
```

### Monitoring cluster resource usage

```bash
# Node resource usage
$SSH_CMD "kubectl top nodes"

# Pod resource usage (sorted by CPU)
$SSH_CMD "kubectl top pods -n $NAMESPACE --sort-by=cpu"

# Check Airflow worker memory
$SSH_CMD "kubectl top pods -n $NAMESPACE -l component=worker"
```
